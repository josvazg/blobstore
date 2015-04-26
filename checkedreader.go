package blobstore

import (
	"fmt"
	"hash"
	"io"
)

// checkedReader is a reader wrapper that fails the last read if the readed contents don't match the
// expected hash key as computed by the given hasher
type checkedReader struct {
	io.Reader
	key    Key
	hasher hash.Hash
}

// Read will return an error prefixed by 'CorruptedBlobErrorPrefix' if the readed blob did not match the hash key
func (cr *checkedReader) Read(buf []byte) (n int, err error) {
	n, err = cr.Reader.Read(buf)
	if n > 0 {
		cr.hasher.Write(buf[:n])
	}
	if err != nil && err == io.EOF {
		actualKey := Key(cr.hasher.Sum(nil))
		if !cr.key.Equals(actualKey) {
			return n, fmt.Errorf("%s expected hash was %v but got %v!!", CorruptedBlobErrorPrefix, cr.key, actualKey)
		}
	}
	return n, err
}
