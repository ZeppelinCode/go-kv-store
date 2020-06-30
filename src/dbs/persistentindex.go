package dbs

import (
	"io/ioutil"
	"os"
)

// PersistentIndex represents an index backed by a file system file
type PersistentIndex struct {
	*IndexImpl        // In-memory index
	indexLoc   string // file system location of index file
}

// NewPersistentIndex creates an instance of a PersistedIndex
// indexLoc is the file system location of the index file
func NewPersistentIndex(indexLoc string) *PersistentIndex {
	f, err := os.OpenFile(indexLoc, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic("Could not create database index")
	}
	index := LoadIndex(f)
	return &PersistentIndex{index, indexLoc}
}

// GetPositionOf returns the location of a document inside the main blob
func (p *PersistentIndex) GetPositionOf(document string) (int64, bool) {
	return p.IndexImpl.GetPositionOf(document)
}

func (p *PersistentIndex) IndexDocumentPosition(document string, loc int64) error {
	err := p.IndexImpl.IndexDocumentPosition(document, loc)
	if err != nil {
		return err
	}
	return rewriteIndex(p)
}

func (p *PersistentIndex) RemoveDocument(document string) {
	lenBefore := len(p.filePositions)
	p.IndexImpl.RemoveDocument(document)
	// Only do disk IO if we deleted something...
	if lenBefore > len(p.filePositions) {
		// We're not handling the edge case here
		// maybe a better interface but this is irrelevant
		rewriteIndex(p)
	}
}

func (idx *PersistentIndex) ListAllDocuments() []string {
	return idx.IndexImpl.ListAllDocuments()
}

// TODO it's not very efficient to rewrite
// TODO the index every time it's mutated
func rewriteIndex(p *PersistentIndex) error {
	marshalled := p.IndexImpl.MarshalIndex()
	return ioutil.WriteFile(p.indexLoc, marshalled, 0644)
}
