package blobstore

import (
	"bytes"
	"crypto"
	"crypto/sha1"
	"encoding/hex"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"testing"
)

var testData = []struct {
	input, expectedHash, expectedPath string
}{
	{"Hola!",
		"f648cdc2cee763f6cb9087a0580729712d93250e",
		"f6/48/cd/c2/f648cdc2cee763f6cb9087a0580729712d93250e.blob"},
	{"hi there!",
		"a903cda4b5b93d3204af0fd6b7b92d24af1923a5",
		"a9/03/cd/a4/a903cda4b5b93d3204af0fd6b7b92d24af1923a5.blob"},
}

// TestFilePaths checks that the hash calculus and the hash file path as correct as expected
func TestFilePaths(t *testing.T) {
	// setup
	fb := fileBlobs{""}
	// exercise
	for _, testCase := range testData {
		key := blobKey(sha1.New(), ([]byte)(testCase.input))
		assert(key.Equals(toKeyOrDie(t, testCase.expectedHash)), t,
			"Input's '%s' expected hash was %s but got %s!", testCase.input, testCase.expectedHash, key)
		path := fb.Keyname(key)
		assert(path == testCase.expectedPath, t,
			"Input's '%s' with hash:\n%s\nexpected path was:\n%s\nBut got:\n%s",
			testCase.input, key, testCase.expectedPath, path)
	}
}

// TestCheckedReader makes sure CheckedReader is doing its job as expected
func TestCheckedReader(t *testing.T) {
	for _, testCase := range testData {
		// goodReader must succeed
		goodReader := &checkedReader{
			strings.NewReader(testCase.input),
			toKeyOrDie(t, testCase.expectedHash),
			sha1.New()}
		err := readAll(goodReader)
		assert(err == io.EOF, t, "Unexpected error reading from goodReader: %s", err)
		// badreader must fail, as we skipped the first byte
		badReader := &checkedReader{
			strings.NewReader(testCase.input[1:]), // corrupted input, lacks first byte
			toKeyOrDie(t, testCase.expectedHash),
			sha1.New()}
		err = readAll(badReader)
		assert(strings.Contains(err.Error(), CorruptedBlobErrorPrefix), t,
			"An %s error was expected, but we got %s instead", CorruptedBlobErrorPrefix, err)
	}
}

// TestReadsNWrites test that the persistent blobserver does its reads and writes as expected
func TestReadsNWrites(t *testing.T) {
	// setup
	// prepare a root for the blob store filesystem with a random name and a file blobserver on it
	dir := fileBlobs{""}.TmpKeyname(10)
	os.Mkdir(dir, 0700)
	fileBlobs := NewFileBlobStoreAdmin(dir, crypto.SHA1)
	// exercise
	for _, testCase := range testData {
		expectedKey := toKeyOrDie(t, testCase.expectedHash)
		// 1 read must fail
		_, err := fileBlobs.Read(expectedKey)
		assert(err != nil, t, "Reading %s should had failed!", testCase.expectedHash)
		assert(!strings.Contains(err.Error(), "bytes long hash key"), t,
			"Error type when reading %s:%v", testCase.expectedHash, err)
		// 2 write must succeed and key must match
		key, err := fileBlobs.Write(strings.NewReader(testCase.input))
		assert(err == nil, t, "Error writing blob %s:%s", testCase.expectedHash, err)
		assert(key.Equals(expectedKey), t, "Expected blob key to be %s but got %s", testCase.expectedHash, key)
		// 3 read must now succeed
		reader, err := fileBlobs.Read(key)
		assert(err == nil, t, "Error fetching %s: %v", key, err)
		blobBytes, err := ioutil.ReadAll(reader)
		assert(err == nil, t, "Error reading %s: %v", key, err)
		assert(bytes.Compare(blobBytes, []byte(testCase.input)) == 0, t,
			"Expected to read '%s' but got '%s'", testCase.input, blobBytes)
		// 4 writing again must succeed and key must match all over again
		key, err = fileBlobs.Write(strings.NewReader(testCase.input))
		assert(err == nil, t, "Error writing blob %s:%s", testCase.expectedHash, err)
		assert(key.Equals(expectedKey), t, "Expected blob key to be %s but got %s", testCase.expectedHash, key)
		// 5 remove must succeed
		err = fileBlobs.Remove(key)
		assert(err == nil, t, "Error removing %s: %v", key, err)
		// 6 read must now fail
		err = fileBlobs.Remove(key)
		assert(err == nil, t, "Error removing %s: %v", key, err)
	}
	// cleanup
	// Remove the root for the blob store filesystem
	err := os.RemoveAll(dir)
	assert(err == nil, t, "Error in cleanup removing %s: %v", dir, err)
}

// TestList test that the list call returns all stored keys as expected
func TestList(t *testing.T) {
	// setup
	// prepare a root for the blob store filesystem with a random name and a file blobserver on it
	dir := fileBlobs{""}.TmpKeyname(10)
	os.Mkdir(dir, 0700)
	fileBlobs := NewFileBlobStore(dir, crypto.SHA1)
	expectedKeys := buildExpectedKeysList()
	// exercise
	for _, testCase := range testData {
		expectedKey := toKeyOrDie(t, testCase.expectedHash)
		// 1 write must succeed and key must match
		key, err := fileBlobs.Write(strings.NewReader(testCase.input))
		assert(err == nil, t, "Error writing blob %s:%s", testCase.expectedHash, err)
		assert(key.Equals(expectedKey), t, "Expected blob key to be %s but got %s", testCase.expectedHash, key)
	}
	blobKeys := fileBlobs.List()
	assert(blobKeys != nil, t, "Error calling List: nil blobKeys returned")
	i := 0
	for blobKey := range blobKeys {
		assert(blobKey.err == nil, t, "Error in List stream: %s", blobKey.err)
		key := strings.ToLower(blobKey.key.String())
		assert(key == expectedKeys[i], t, "Next expected key in list was %s, but got %s", expectedKeys[i], key)
		i++
	}
	// cleanup
	// Remove the root for the blob store filesystem
	err := os.RemoveAll(dir)
	assert(err == nil, t, "Error in cleanup removing %s: %v", dir, err)
}

// buildExpectedKeysList builds a ordered expected list of keys from testData
func buildExpectedKeysList() []string {
	expectedKeys := make([]string, len(testData))
	for i, testCase := range testData {
		expectedKeys[i] = testCase.expectedHash
	}
	sort.StringSlice(expectedKeys).Sort()
	return expectedKeys
}

// readAll reads all of r till EOF or another error
func readAll(r io.Reader) (err error) {
	buf := make([]byte, 1) // small buffer to ensure we really use the loop on very small input data
	for err = nil; err == nil; {
		_, err = r.Read(buf)
	}
	return err
}

// toKeyOrDie is a helper function to convert a string to a Hexadecimal Hash key
// if the string can´t be converted, the program dies
func toKeyOrDie(t *testing.T, hexstr string) Key {
	bytes, err := hex.DecodeString(hexstr)
	if err != nil {
		t.Fatalf("Error decoding hex '%s' back to binary: %s", hexstr, err.Error())
	}
	return Key(bytes)
}

// blobKey is a helper function that returns the key of a blob on the given hasher
func blobKey(h hash.Hash, blob []byte) Key {
	h.Write(blob)
	return Key(h.Sum(nil))
}

// assert is a helper function for test assertions
func assert(assertion bool, t *testing.T, format string, args ...interface{}) {
	if !assertion {
		t.Fatalf(format, args)
	}
}
