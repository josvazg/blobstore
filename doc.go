/*
Package blobserver a Content Addressed basic service definition an implementation

The normal BlobStore user interface allows just to:
- Read a blob or stream of bytes given its content based hash key (for instance SHA-1 of all the bytes)
- Writes a blob and get its content based hash key back (used for later retrieval)
- Enumerate the available blobs (identified by key)

For administration an extended interface is provided at also allows to:
- Remove a blob by hash key

*/
package blobstore

import (
	"bytes"
	"crypto"
	"encoding/hex"
	"io"
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

// A Key or Error type for key listing
type KeyOrError struct {
	key Key
	err error
}

// BlobStore saves and retrieves blobs identified by the hash of its content
type BlobStore interface {
	// Read returns a reader for the given blob content hash key, or an error (like 'key nor found')
	Read(key Key) (io.Reader, error)
	// Write a new blob, passed by as a reader and returns the readed blob content hash key, or an error
	Write(blob io.Reader) (Key, error)
	// List returns list of stored keys via a channel,
	// each entry could have an error and then the channel will be closed
	List() <-chan KeyOrError
}

// BlobStoreAdmin is a BlobStore that can also remove blobs
// usually an elevated piviledges operation that only a Garbage Collector needs to do based on certain policies
type BlobStoreAdmin interface {
	BlobStore
	// Remove the given key, returns an error is something goes wrong (if the key is not present it does NOT complain)
	Remove(key Key) error
}

// NewFileBlobStore returns a files BlobStore
func NewFileBlobStore(dir string, hash crypto.Hash) BlobStore {
	return NewFileBlobServer(dir, hash)
}

// NewFileBlobStore returns a files BlobStoreAdmin
func NewFileBlobStoreAdmin(dir string, hash crypto.Hash) BlobStoreAdmin {
	return NewFileBlobServer(dir, hash)
}

// NewMemBlobStore returns a files BlobStore
func NewMemBlobStore(hash crypto.Hash) BlobStore {
	return NewMemBlobServer(hash)
}

// NewMemBlobStore returns a files BlobStoreAdmin
func NewMemBlobStoreAdmin(hash crypto.Hash) BlobStoreAdmin {
	return NewMemBlobServer(hash)
}
