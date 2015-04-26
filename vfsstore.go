package blobstore

import (
	"crypto"
	"encoding/hex"
	"fmt"
	"io"
)

const (
	VFS_ROOT = ""
)

// VFS blob server implements a BlobServer on a Virtual Filesystem (VirtualFS)
type VFSBlobServer struct {
	VirtualFS
	hash crypto.Hash
}

// VirtualFS contains the minimum methods required from any FileSystem to support a BlobServer
type VirtualFS interface {
	// Open a key contents for reading by keyname (not needs to be equal to hashkey hex exactly, can contain more)
	Open(keyname string) (io.ReadCloser, error)
	// Create a key to write a key's contents for the first time
	Create(keyname string) (io.WriteCloser, error)
	// Delete a key & contents from the FS
	Delete(keyname string) error
	// Does the given key exists?
	Exists(keyname string) bool
	// Rename a key, usually only used once, when the contents are done writting and the correspoding hash key is known
	Rename(oldkeyname, newkeyname string) error
	// List all present keys in sort order to the keys channel as filtered by acceptor
	ListTo(keys chan<- KeyOrError, acceptor func(string) Key, dir string) bool
	// Keyname returns a keyname full path of where the key blob should be placed
	Keyname(key Key) string
	// Tmpkeyname returns a temporary filename
	TmpKeyname(size int) string
}

// Read retrieves a reader for the given blob from the file system
func (vbs *VFSBlobServer) Read(key Key) (io.Reader, error) {
	if len(key) < vbs.hash.Size() {
		return nil, fmt.Errorf("Expected a %d bytes long hash key, but got just %dbytes in %v",
			vbs.hash.Size(), len(key), key)
	}
	file, err := vbs.Open(vbs.Keyname(key))
	if err != nil {
		return nil, err
	}
	return &checkedReader{file, key, vbs.hash.New()}, nil
}

// Write stores the bytes from the given reader to the file system and returns the matching hash key
func (vbs *VFSBlobServer) Write(blob io.Reader) (Key, error) {
	tmpKeyname := vbs.TmpKeyname(vbs.hash.Size())
	newblob, err := vbs.Create(tmpKeyname)
	if err == nil {
		defer newblob.Close()
		hasher := vbs.hash.New()
		_, err := io.Copy(io.MultiWriter(newblob, hasher), blob)
		if err == nil {
			key := Key(hasher.Sum(nil))
			keyname := vbs.Keyname(key)
			if vbs.Exists(keyname) {
				// no need to keep to copies of the same bytes
				err = vbs.Delete(tmpKeyname)
			} else {
				err = vbs.Rename(tmpKeyname, keyname)
			}
			return key, err
		}
	}
	return nil, err
}

// List returns list of stored keys via a channel
// It is a recursive directory/file search depth-first
func (vbs *VFSBlobServer) List() <-chan KeyOrError {
	keys := make(chan KeyOrError)
	go func() {
		if vbs.ListTo(keys, vbs.acceptor, VFS_ROOT) {
			// if the return is true, keys channel is still open and we must close it here
			close(keys)
		}
	}()
	return keys
}

// Remove the given key, returns an error is something goes wrong (if the key is not present it does NOT complain)
func (vbs *VFSBlobServer) Remove(key Key) (err error) {
	keyname := vbs.Keyname(key)
	if vbs.Exists(keyname) {
		err = vbs.Delete(keyname)
	}
	return err
}

// acceptor knows how to accept and transform valid key names to keys
func (vbs *VFSBlobServer) acceptor(name string) Key {
	// try to decode to binary from hex string
	bytes, err := hex.DecodeString(name)
	// if the filename was a proper hex string (decode suceeded) and of the right size, send it through keys
	if err == nil && len(bytes) == vbs.hash.Size() {
		return Key(bytes)
	}
	return nil
}

// failKeyOrError will send a error via keys and then immediatelly close the keys channel, to fail the keys stream
func failKeyOrError(keys chan<- KeyOrError, err error) bool {
	keys <- KeyOrError{nil, err}
	close(keys)
	return false
}
