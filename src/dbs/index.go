package dbs

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/ZeppelinCode/go-kv-store/src/binutil"
)

// ASCII codes
const (
	colon = 58
	comma = 44
)

type Index interface {
	ListAllDocuments() []string
	GetPositionOf(document string) (int64, bool)
	IndexDocumentPosition(document string, loc int64) error
	RemoveDocument(document string)
}

// An IndexImpl that contains a document name -> byte position map in the
// document binary blob. Indexed locations are stored in this format:
// ${document_name}:${location_in_blob},${document_name2}:${location_in_blob},....
type IndexImpl struct {
	filePositions map[string]int64
}

// NewIndex creates a new index
func NewIndex() *IndexImpl {
	indexMap := make(map[string]int64)
	return &IndexImpl{indexMap}
}

// LoadIndex loads an existing index from disk in memory
func LoadIndex(r io.Reader) *IndexImpl {
	index := NewIndex()
	br := bufio.NewReader(r)
	nextGroup, _ := br.ReadBytes(comma)

	for len(nextGroup) > 0 {
		docName, docLocation := extractNameAndLocationFromBinaryBlob(nextGroup)
		index.IndexDocumentPosition(docName, docLocation)
		nextGroup, _ = br.ReadBytes(comma)
	}

	return index
}

// MarshalIndex returns a binary representation of an index
// that LoadIndex can interpret
func (idx *IndexImpl) MarshalIndex() []byte {
	marshalled := make([]byte, getIndexByteSize(idx))
	pos := 0
	for k, v := range idx.filePositions {
		binutil.CopyBytesAtLocation(marshalled, pos, []byte(k))
		pos += len([]byte(k))
		marshalled[pos] = colon
		pos++
		binutil.WriteIntToSliceAtLocation(marshalled, pos, v)
		pos += 8
		marshalled[pos] = comma
		pos++
	}
	return marshalled
}

// GetPositionOf returns the byte position of a document in the blob
func (idx *IndexImpl) GetPositionOf(document string) (int64, bool) {
	loc, ok := idx.filePositions[document]
	return loc, ok
}

// IndexDocumentPosition adds a new document to the index
// TODO: document should not contain : or ,
func (idx *IndexImpl) IndexDocumentPosition(document string, loc int64) error {
	_, ok := idx.filePositions[document]
	if ok == true {
		return errors.New("Document " + document + " already exists")
	}

	idx.filePositions[document] = loc
	return nil
}

// RemoveDocument removes a document from the index
func (idx *IndexImpl) RemoveDocument(document string) {
	delete(idx.filePositions, document)
}

func (idx *IndexImpl) ListAllDocuments() []string {
	// All zeros hammer this
	// This test would fail if this is not set to 0
	docs := make([]string, 0, len(idx.filePositions))
	for doc := range idx.filePositions {
		docs = append(docs, doc)
	}
	return docs
}

func extractNameAndLocationFromBinaryBlob(blob []byte) (string, int64) {
	// Don't want the comma
	docName := make([]byte, 0, 10)
	br := bytes.NewReader(blob)
	currentByte, _ := br.ReadByte()
	for ; currentByte != colon; currentByte, _ = br.ReadByte() {
		docName = append(docName, currentByte)
	}

	locationBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		locationBytes[i], _ = br.ReadByte()
	}
	location := int64(binary.LittleEndian.Uint64(locationBytes))

	return string(docName), location
}

func getIndexByteSize(idx *IndexImpl) int {
	indexSize := 0
	for documentName := range idx.filePositions {
		indexSize += len([]byte(documentName))
		indexSize += 10 // 8 for the location, 1 for : and 1 for ,
	}
	return indexSize
}
