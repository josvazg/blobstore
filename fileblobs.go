package blobstore

import (
	"crypto"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	defaultPerms = 0750
	vfsRoot      = ""
	filesAtOnce  = 10
)

// NewFileBlobServer returns a VFSBlobServer using a fileBlobs, that is on top of the os files
func NewFileBlobServer(dir string, hash crypto.Hash) *VFSBlobServer {
	return &VFSBlobServer{fileBlobs{dir}, hash}
}

// VirtualFS on OS implementation
type fileBlobs struct {
	dir string
}

// Open a file contents for reading
func (vfs fileBlobs) Open(key string) (io.ReadCloser, error) {
	return os.Open(key)
}

// Create a file to write a key's contents for the first time
func (vfs fileBlobs) Create(key string) (io.WriteCloser, error) {
	return os.OpenFile(key, os.O_CREATE|os.O_WRONLY, defaultPerms)
}

// Delete a key & contents from the FS
func (vfs fileBlobs) Delete(key string) error {
	return os.Remove(key)
}

// Does the given key exists in disk?
func (vfs fileBlobs) Exists(key string) bool {
	_, err := os.Stat(key)
	return !os.IsNotExist(err)
}

// Rename a key, usually only used once, when the contents are done writting and the correspoding hash key is known
func (vfs fileBlobs) Rename(oldkey, newkey string) error {
	err := os.MkdirAll(filepath.Dir(newkey), defaultPerms)
	if err == nil {
		err = os.Rename(oldkey, newkey)
	}
	return err
}

// ListTo lists all present keys in sort order to the keys channel
func (vfs fileBlobs) ListTo(keys chan<- KeyOrError, acceptor func(string) Key) bool {
	return vfs.listTo(keys, acceptor, vfsRoot)
}

// listTo is the internal recursive implementation of ListTo list key names from recursive directories
func (vfs fileBlobs) listTo(keys chan<- KeyOrError, acceptor func(string) Key, dir string) bool {
	if dir == vfsRoot { // start at the root dir
		dir = vfs.dir
	}
	root, err := os.Open(dir)
	if err != nil {
		return failKeyOrError(keys, err)
	}
	for {
		fileInfos, err := root.Readdir(filesAtOnce)
		if err == io.EOF { // on EOF we are done
			return true
		} else if err != nil {
			return failKeyOrError(keys, err)
		}
		for _, fileInfo := range fileInfos {
			if fileInfo.IsDir() { // If it is a dir...
				// List tha branch, but fail the pipeline if that returns false (=failure)
				if !vfs.listTo(keys, acceptor, filepath.Join(dir, fileInfo.Name())) {
					return false // give up if the subtree failed
				}
			} else { // If it is Not a directory but a file...
				// get the filename
				filename := fileInfo.Name()
				// strip the extension, if any
				if strings.Contains(filename, ".") {
					filename = strings.Split(filename, ".")[0]
				}
				// if filename is accepted by acceptor it will produce a non nil key, then send it through keys
				key := acceptor(filename)
				if key != nil {
					keys <- KeyOrError{key, nil}
				}
			}
		}
	}
}

// keyname returns a filename full path of where the key blob should be placed
func (vfs fileBlobs) Keyname(key Key) string {
	hexKey := key.String()
	return filepath.Join(vfs.dir, hexKey[0:2], hexKey[2:4], hexKey[4:6], hexKey[6:8], fmt.Sprintf("%s.blob", hexKey))
}

// tmpkeyname returns a temporary filename
func (vfs fileBlobs) TmpKeyname(size int) string {
	key := make([]byte, size)
	rand.Reader.Read(key)
	return filepath.Join(vfs.dir, fmt.Sprintf("%s.new", Key(key).String()))
}
