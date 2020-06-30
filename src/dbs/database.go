package dbs

import (
	"errors"

	"github.com/ZeppelinCode/go-kv-store/src/dbs/dbstructs"
	"github.com/ZeppelinCode/go-kv-store/src/dbs/fsint"
)

// TODO: consider concurrent reads and writes and reservations of data sectors
// TODO: Free lists when you implement delete and lesser updates

// A Database contains both an index and a data store
type Database struct {
	*fsint.DbFileHandler
	index   Index
	creator *fsint.DocumentCreator
	updater *fsint.DocumentUpdater
	deleter *fsint.DocumentDeleter
}

// NewDatabase returns a new database with a file handle of `d`
func NewDatabase(d string, documentPositioner fsint.DocumentPositioner) (*Database, error) {
	index := NewPersistentIndex(d + "_i")
	dbfh, err := fsint.NewDbFileHandler(d)
	if err != nil {
		return nil, err
	}
	database := &Database{
		dbfh,
		index,
		fsint.NewDocumentCreator(documentPositioner),
		fsint.NewDocumentUpdater(documentPositioner),
		fsint.NewDocumentDeleter(documentPositioner),
	}
	return database, nil
}

// StoreNewDocument creates and indexes a new document in the data store
// TODO: look for duplicates
func (d *Database) StoreNewDocument(name string, payload string) error {
	// Can make this faster in the future using goroutines
	n, writtenAt, err := d.creator.StoreNewDocument(d.DbFileHandler, payload)
	if err == nil {
		if writtenAt != -1 {
			d.index.IndexDocumentPosition(name, writtenAt)
		} else {
			d.index.IndexDocumentPosition(name, d.EofLocation)
			d.EofLocation += int64(n)
		}
	}
	return err
}

// UpdateDocument updates an existing document or returns an error
// if the document does not exist or there is an io issue
func (d *Database) UpdateDocument(name string, payload string) error {
	pos, exists := d.index.GetPositionOf(name)
	if exists == false {
		return errors.New("Document does not exist")
	}
	return d.updater.UpdateDocument(d.DbFileHandler, payload, pos)
}

// RetrieveDocument returns a stored document in the data store
func (d *Database) RetrieveDocument(name string) (*dbstructs.Document, error) {
	return retrieveDocument(d, name)
}

// DeleteDocument removes the document from the index
// and sets its locations to free
// It does not clear contents of the document in the storage, it simply
// allows for them to be reused
func (d *Database) DeleteDocument(name string) error {
	doc, err := retrieveDocument(d, name)
	if err != nil {
		return err
	}

	d.deleter.DeleteDocument(doc)
	d.index.RemoveDocument(name)
	return nil
}

func (d *Database) StoredDocuments() []string {
	return d.index.ListAllDocuments()
}

func retrieveDocument(d *Database, name string) (*dbstructs.Document, error) {
	pos, exists := d.index.GetPositionOf(name)
	if exists == false {
		return nil, errors.New("Document does not exist")
	}
	return fsint.RetrieveDocument(d.DbFileHandler, pos)
}
