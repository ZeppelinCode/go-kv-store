package fsint

import "github.com/ZeppelinCode/go-kv-store/src/dbs/dbstructs"

// A DocumentDeleter is responsible for deleting documents
// Documents are not really "deleted" their space is simply marked as available
// to the document positioner
type DocumentDeleter struct {
	documentPositioner DocumentPositioner
}

// NewDocumentDeleter returns a new document deleter
func NewDocumentDeleter(dp DocumentPositioner) *DocumentDeleter {
	return &DocumentDeleter{dp}
}

// DeleteDocument marks a the space of a document as free
func (deleter *DocumentDeleter) DeleteDocument(doc *dbstructs.Document) {
	currentDataBlock := doc.Head

	for currentDataBlock != nil {
		freedSpace := dbstructs.FreeSpace{
			Pos: currentDataBlock.Location,
			Len: docSizePlusHeaders(currentDataBlock.Length),
		}
		deleter.documentPositioner.addFreeLocation(freedSpace)
		currentDataBlock = currentDataBlock.Next.Link
	}
}
