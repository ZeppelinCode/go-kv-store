package dbs

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ZeppelinCode/go-kv-store/src/binutil"
)

func TestNewIndex(t *testing.T) {
	index := NewIndex()
	assert.NotNil(t, index)
}

func TestListAllDocuments(t *testing.T) {
	index := NewIndex()
	index.IndexDocumentPosition("doc1", 50)
	allDocs := index.ListAllDocuments()
	assert.Equal(t, allDocs, []string{"doc1"})
}

func TestIndexDocumentPosition(t *testing.T) {
	index := NewIndex()
	err := index.IndexDocumentPosition("mydoc", 50)
	assert.Nil(t, err)
}

func TestIndexDocumentPositionDuplicate(t *testing.T) {
	index := NewIndex()
	err := index.IndexDocumentPosition("mydoc", 50)
	assert.Nil(t, err)
	err = index.IndexDocumentPosition("mydoc", 100)
	assert.NotNil(t, err)
}

func TestGetPositionOfWhenNotPresent(t *testing.T) {
	index := NewIndex()
	_, ok := index.GetPositionOf("mydoc")
	assert.False(t, ok)
}

func TestGetPositionOfWhenPresent(t *testing.T) {
	index := NewIndex()
	index.IndexDocumentPosition("mydoc", 50)
	loc, ok := index.GetPositionOf("mydoc")
	assert.True(t, ok)
	assert.Equal(t, loc, int64(50))
}

func TestLoadIndex(t *testing.T) {
	marshalledIndex := generateMarshalledIndex()
	r := bytes.NewReader(marshalledIndex)

	index := LoadIndex(r)
	loc, _ := index.GetPositionOf("mydoc")
	assert.Equal(t, int64(50), loc)

	loc, _ = index.GetPositionOf("mydoc2")
	assert.Equal(t, int64(60), loc)

	loc, _ = index.GetPositionOf("mydoc3")
	assert.Equal(t, int64(70), loc)
}

// http://zacg.github.io/blog/2014/10/05/go-asserts-and-multiple-return-values/
func TestMarshalIndex(t *testing.T) {
	idx := NewIndex()
	idx.IndexDocumentPosition("d1", 0)
	idx.IndexDocumentPosition("d2", 10)
	idx.IndexDocumentPosition("d3", 20)

	marshalled := idx.MarshalIndex()
	unmarshalled := LoadIndex(bytes.NewReader(marshalled))

	assert.Equal(t, len(unmarshalled.filePositions), 3)
	assert.Equal(t, G(unmarshalled.GetPositionOf("d1")), G(idx.GetPositionOf("d1")))
	assert.Equal(t, G(unmarshalled.GetPositionOf("d2")), G(idx.GetPositionOf("d2")))
	assert.Equal(t, G(unmarshalled.GetPositionOf("d3")), G(idx.GetPositionOf("d3")))
}

func generateMarshalledIndex() []byte {
	// mydoc:50,mydoc2:60,mydoc3:70,
	marshalledIndex := make([]byte, 0, 256)
	marshalledBuffer := bytes.NewBuffer(marshalledIndex)
	doc1loc := binutil.Int64ToBytes(int64(50))
	doc2loc := binutil.Int64ToBytes(int64(60))
	doc3loc := binutil.Int64ToBytes(int64(70))

	marshalledBuffer.WriteString("mydoc:")
	marshalledBuffer.Write(doc1loc)
	marshalledBuffer.WriteString(",")

	marshalledBuffer.WriteString("mydoc2:")
	marshalledBuffer.Write(doc2loc)
	marshalledBuffer.WriteString(",")

	marshalledBuffer.WriteString("mydoc3:")
	marshalledBuffer.Write(doc3loc)
	marshalledBuffer.WriteString(",")

	return marshalledBuffer.Bytes()
}

func G(a, b interface{}) []interface{} {
	return []interface{}{a, b}
}
