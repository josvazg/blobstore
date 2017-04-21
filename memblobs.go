package blobstore

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
)

const (
	initialMemBuffer = 10
)

// NewMemBlobServer returns a VFSBlobServer using a fileBlobs, that is on top of the os files
func NewMemBlobServer(hash crypto.Hash) *VFSBlobServer {
	return &VFSBlobServer{newMemBlobs(), hash}
}

// newMemBlobs returns a new memBlobs
func newMemBlobs() *memBlobs {
	return &memBlobs{make(map[string]*bytes.Buffer), make(sort.StringSlice, 0)}
}

// VirtualFS blob support on memory. Useful for testing abut also for in memory cache
// Content Addressed Blobs have perfect caching, as they are immutable
type memBlobs struct {
	blobs    map[string]*bytes.Buffer
	keynames sort.StringSlice
}

// Open a key contents for reading
func (mem *memBlobs) Open(key string) (io.ReadCloser, error) {
	buf, ok := mem.blobs[key]
	if !ok {
		return nil, fmt.Errorf("Key not found: %s", key)
	}
	return ioutil.NopCloser(buf), nil
}

// Create a key to set its contents
func (mem *memBlobs) Create(keyname string) (io.WriteCloser, error) {
	buf := make([]byte, 0, initialMemBuffer)
	mem.blobs[keyname] = bytes.NewBuffer(buf)
	mem.insert(keyname)
	return nopWriterCloser(mem.blobs[keyname]), nil
}

// Delete a key & contents from memory (and never fails)
func (mem *memBlobs) Delete(keyname string) error {
	delete(mem.blobs, keyname)
	return nil
}

// Does the given key exists in memory
func (mem *memBlobs) Exists(keyname string) bool {
	_, ok := mem.blobs[keyname]
	return ok
}

// Rename a key, usually only used once, when the contents are done writting and the correspoding hash key is known
func (mem *memBlobs) Rename(oldkey, newkey string) error {
	mem.blobs[newkey] = mem.blobs[oldkey]
	mem.insert(newkey)
	delete(mem.blobs, oldkey)
	mem.extract(oldkey)
	return nil
}

// List all present keys in sort order to the keys channel
func (mem *memBlobs) ListTo(keys chan<- KeyOrError, acceptor func(string) Key) bool {
	for _, keyname := range mem.keynames {
		key := acceptor(keyname)
		if key != nil {
			keys <- KeyOrError{key, nil}
		}
	}
	return true
}

// keyname returns a key name, in memory the hash key is used directly as key name
func (mem *memBlobs) Keyname(key Key) string {
	return key.String()
}

// tmpkeyname returns a temporary keyname
func (mem *memBlobs) TmpKeyname(size int) string {
	key := make([]byte, size)
	rand.Reader.Read(key)
	return fmt.Sprintf("%s.new", Key(key).String())
}

// insert places a keyname ordered within the keynames list
func (mem *memBlobs) insert(keyname string) {
	index := mem.keynames.Search(keyname)
	mem.keynames = append(mem.keynames, keyname)       // optimistically we place it in the end and hope for the best
	for i := (len(mem.keynames) - 1); i > index; i-- { // shift everything after index one position to the 'right'
		mem.keynames[i] = mem.keynames[i-1]
	}
	if index < (len(mem.keynames) - 1) { // if we were too optimistic, replace the key in its right place
		mem.keynames[index] = keyname
	}
}

// extract removes a keyname from the ordered keynames list
func (mem *memBlobs) extract(keyname string) bool {
	index := mem.keynames.Search(keyname)
	if mem.keynames[index] != keyname {
		return false
	}
	for i := index; i < (len(mem.keynames) - 1); i++ { // shift everything after index one position to the 'left'
		mem.keynames[i] = mem.keynames[i+1]
	}
	mem.keynames = mem.keynames[:len(mem.keynames)-1] // finally shrink the list by one
	return true
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

// nopWriterCloser returns a WriterCloser with a no-op Close method wrapping the provided writer r
func nopWriterCloser(w io.Writer) io.WriteCloser {
	return nopCloser{w}
}
