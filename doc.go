/*
Package blobstore a Content Addressed basic service definition an implementation

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
	corruptedBlobErrorPrefix = "Corrupted Blob:"
)

// Key is the blob key type
type Key []byte

// String returns the hexadecimal string representation of the key
func (k Key) String() string {
	return hex.EncodeToString(k)
}

// Equals returns true if this and key are the same sequence of bytes
func (k Key) Equals(key Key) bool {
	return bytes.Compare(k, key) == 0
}

// KeyOrError type for key listing
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

// BlobAdmin is a BlobStore that can also remove blobs
// usually an elevated piviledges operation that only a Garbage Collector needs to do based on certain policies
type BlobAdmin interface {
	BlobStore
	// Remove the given key, returns an error is something goes wrong (if the key is not present it does NOT complain)
	Remove(key Key) error
}

// NewFileBlobStore returns a files BlobStore
func NewFileBlobStore(dir string, hash crypto.Hash) BlobStore {
	return NewFileBlobServer(dir, hash)
}

// NewFileBlobAdmin returns a files BlobAdmin
func NewFileBlobAdmin(dir string, hash crypto.Hash) BlobAdmin {
	return NewFileBlobServer(dir, hash)
}

// NewMemBlobStore returns a files BlobStore
func NewMemBlobStore(hash crypto.Hash) BlobStore {
	return NewMemBlobServer(hash)
}

// NewMemBlobAdmin returns a files BlobAdmin
func NewMemBlobAdmin(hash crypto.Hash) BlobAdmin {
	return NewMemBlobServer(hash)
}
