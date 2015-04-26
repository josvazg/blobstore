/*
Package blobserver a Content Addressed basic service definition an implementation

The normal BlobStore user interface allows just to:
- Read a blob or stream of bytes given its content based hash key (for instance SHA-1 of all the bytes)
- Writes a blob and get its content based hash key back (used for later retrieval)
- Enumerate the available blobs (identified by key)

For administration an extended interface is provided at also allows to:
- Remove a blob by hash key

*/
package blobserver

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	CorruptedBlobErrorPrefix = "Corrupted Blob:"
	FilesAtOnce              = 10
)

// A blob key
type Key []byte

// String returns the hexadecimal string representation of the key
func (k Key) String() string {
	return hex.EncodeToString(k)
}

// Equals returns true if this and key are the same sequence of bytes
func (k Key) Equals(key Key) bool {
	return bytes.Compare(k, key) == 0
}

// A Key or Error type
type KeyOrError struct {
	key Key
	err error
}

// list position
type Pos []*os.File

// BlobStore saves and retrieves blobs identified by the hash of its content
type BlobStore interface {
	// Read returns a reader for the given blob content hash key, or an error (like 'key nor found')
	Read(key Key) (io.Reader, error)
	// Write a new blob, passed by as a reader and returns the readed blob content hash key, or an error
	Write(blob io.Reader) (Key, error)
	// List returns list of stored keys via a channel,
	// each entry could have an error and then the channel will be closed
	List() (<-chan KeyOrError, error)
}

// BlobStoreAdmin is a BlobStore that can also remove blobs
// usually an elevated piviledges operation that only a Garbage Collector needs to do based on certain policies
type BlobStoreAdmin interface {
	BlobStore
	// Remove the given key, returns an error is something goes wrong (if the key is not present it does NOT complain)
	Remove(key Key) error
}

// File blob server implements a BlobServer on the filesystem
type FileBlobServer struct {
	dir  string
	hash crypto.Hash
}

// Read retrieves a reader for the given blob from the file system
func (fbs *FileBlobServer) Read(key Key) (io.Reader, error) {
	if len(key) < fbs.hash.Size() {
		return nil, fmt.Errorf("Expected a %d bytes long hash key, but got just %dbytes in %v",
			fbs.hash.Size(), len(key), key)
	}
	file, err := os.Open(buildFilename(fbs.dir, key))
	if err != nil {
		return nil, err
	}
	return &checkedReader{file, key, fbs.hash.New()}, nil
}

// Write stores the bytes from the given reader to the file system and returns the matching hash key
func (fbs *FileBlobServer) Write(blob io.Reader) (Key, error) {
	tmpFilename := buildTmpFilename(fbs.dir)
	newblob, err := os.OpenFile(tmpFilename, os.O_CREATE|os.O_WRONLY, 0700)
	if err == nil {
		defer newblob.Close()
		hasher := fbs.hash.New()
		_, err := io.Copy(io.MultiWriter(newblob, hasher), blob)
		if err == nil {
			key := Key(hasher.Sum(nil))
			filename := buildFilename(fbs.dir, key)
			if fileExists(filename) {
				// no need to keep to copies of the same bytes
				err = os.Remove(tmpFilename)
			} else {
				err = os.MkdirAll(filepath.Dir(filename), 0750)
				if err == nil {
					err = os.Rename(tmpFilename, filename)
				}
			}
			return key, err
		}
	}
	return nil, err
}

// List returns list of stored keys via a channel
// It is a recursive directory/file search depth-first
func (fbs *FileBlobServer) List() (<-chan KeyOrError, error) {
	root, err := os.Open(fbs.dir)
	if err != nil {
		return nil, err
	}
	keys := make(chan KeyOrError)
	go func() {
		if fbs.listTo(keys, root) {
			// if the return is true, keys channel is still open and we must close it here
			close(keys)
		}
	}()
	return keys, nil
}

// listTo lists stored keys, from root, depth first and sends each one through the given channel.
// It returns true if the caller should continue or false if an error stopped the search and the caller has to give up
func (fbs *FileBlobServer) listTo(keys chan<- KeyOrError, root *os.File) bool {
	for {
		fileInfos, err := root.Readdir(FilesAtOnce)
		if err == io.EOF { // on EOF we are done
			return true
		} else if err != nil {
			failKeyOrError(keys, err)
		}
		for _, fileInfo := range fileInfos {
			if fileInfo.IsDir() { // If it is a dir...
				// Open the dir file
				newroot, err := os.Open(filepath.Join(root.Name(), fileInfo.Name()))
				// if the open failed, send the open error through the channel & fail the pipeline
				if err != nil {
					failKeyOrError(keys, err)
				}
				// Otherwise list the subdir as a newroot, but fail the pipeline if that returns false (=failure)
				if !fbs.listTo(keys, newroot) {
					return false // give up if the subtree failed
				}
			} else { // If it is Not a directory but a file...
				// get the filename
				filename := fileInfo.Name()
				// strip the extension, if any
				if strings.Contains(filename, ".") {
					filename = strings.Split(filename, ".")[0]
				}
				// try to decode to binary from hex string
				bytes, err := hex.DecodeString(filename)
				// if the filename was a proper hex string (decode suceeded) and of the right size, send it through keys
				if err == nil && len(bytes) == fbs.hash.Size() {
					keys <- KeyOrError{Key(bytes), nil}
				}
			}
		}
	}
}

// failKeyOrError will send a error via keys and then immediatelly close the keys channel, to fail the keys stream
func failKeyOrError(keys chan<- KeyOrError, err error) bool {
	keys <- KeyOrError{nil, err}
	close(keys)
	return false
}

// Remove the given key, returns an error is something goes wrong (if the key is not present it does NOT complain)
func (fbs *FileBlobServer) Remove(key Key) (err error) {
	err = nil
	filename := buildFilename(fbs.dir, key)
	if fileExists(filename) {
		err = os.Remove(filename)
	}
	return err
}

// checkedReader is a reader wrapper that fails the last read if the readed contents don't match the
// expected hash key as computed by the given hash algorithm
type checkedReader struct {
	io.Reader
	key  Key
	hash hash.Hash
}

// Read will return an error prefixed by 'CorruptedBlobErrorPrefix' if the readed blob did not match the hash key
func (cr *checkedReader) Read(buf []byte) (n int, err error) {
	n, err = cr.Reader.Read(buf)
	if n > 0 {
		cr.hash.Write(buf[:n])
	}
	if err != nil && err == io.EOF {
		actualKey := Key(cr.hash.Sum(nil))
		if !cr.key.Equals(actualKey) {
			return n, fmt.Errorf("%s expected hash was %v but got %v!!", CorruptedBlobErrorPrefix, cr.key, actualKey)
		}
	}
	return n, err
}

// builds a filename full path of where the key blob should be placed
func buildFilename(dir string, key Key) string {
	hexKey := key.String()
	return filepath.Join(dir, hexKey[0:2], hexKey[2:4], hexKey[4:6], hexKey[6:8], fmt.Sprintf("%s.blob", hexKey))
}

// builds a temporary filename
func buildTmpFilename(dir string) string {
	key := make([]byte, sha1.Size)
	rand.Reader.Read(key)
	return filepath.Join(dir, fmt.Sprintf("%s.new", Key(key).String()))
}

// fileExists return whether ot not filename exists
func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}
