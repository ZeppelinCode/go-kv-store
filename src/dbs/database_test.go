package dbs

import (
	"os"
	"testing"

	"github.com/ZeppelinCode/go-kv-store/src/dbs/fsint"

	"github.com/stretchr/testify/assert"
)

const dbLoc = "./test.store"

func TestDocumentStorageAndRetrieval(t *testing.T) {
	defer deleteFile()

	payload := "mypayload"
	positioner := fsint.DocumentPositionerImpl{}
	db, _ := NewDatabase(dbLoc, &positioner)

	db.StoreNewDocument("doc", payload)
	doc, _ := db.RetrieveDocument("doc")
	assert.Equal(t, doc.Head.Data, payload)
	assert.Equal(t, doc.Head.Next.Location, int64(-1))
	assert.Equal(t, doc.Head.Prev.Location, int64(-1))
	assert.Nil(t, doc.Head.Next.Link)
	assert.Nil(t, doc.Head.Prev.Link)
	assert.Equal(t, doc.Head.Length, int64(len([]byte(payload))))
}

func TestDocumentStoreUpdateAndRetrieve(t *testing.T) {
	defer deleteFile()

	payload := "oldpayload"
	payload2 := "newpayloadextended"
	payload2Block1 := "newpayload"
	payload2Block2 := "extended"
	payload3 := "oldpayloadextendedhello"
	payload3Block3 := "hello"

	positioner := fsint.DocumentPositionerImpl{}
	db, _ := NewDatabase(dbLoc, &positioner)

	db.StoreNewDocument("doc", payload)
	db.UpdateDocument("doc", payload2)

	doc, _ := db.RetrieveDocument("doc")

	// Current block needs to be replaced
	assert.Equal(t, doc.Head.Data, payload2Block1)
	assert.Equal(t, doc.Head.Next.Link.Data, payload2Block2)

	db.UpdateDocument("doc", payload3)
	doc, _ = db.RetrieveDocument("doc")
	assert.Equal(t, doc.Head.Data, payload)
	assert.Equal(t, doc.Head.Next.Link.Data, payload2Block2)
	assert.Equal(t, doc.Head.Next.Link.Next.Link.Data, payload3Block3)
}

// Hexdump these during tutorials
func TestDocumentStoreInterleave(t *testing.T) {
	defer deleteFile()

	payload := "oldpayload"
	payload2 := "newpayloadextended"
	payload3 := "oldpayloadextendedhello"

	positioner := fsint.DocumentPositionerImpl{}
	db, _ := NewDatabase(dbLoc, &positioner)

	db.StoreNewDocument("doc", payload)
	db.StoreNewDocument("doc2", payload2)

	doc, _ := db.RetrieveDocument("doc")
	doc2, _ := db.RetrieveDocument("doc2")

	db.UpdateDocument("doc", payload3)
	doc, _ = db.RetrieveDocument("doc")
	assert.Equal(t, doc.Head.Data, payload)
	assert.Equal(t, doc.Head.Next.Link.Data, "extendedhello")
	assert.Equal(t, doc2.Head.Data, "newpayloadextended")
}

// https://blog.codecentric.de/en/2017/08/gomock-tutorial/
// mockgen -destination=./mocks/dbs/mock_index.go -package=mocks github.com/ZeppelinCode/go-kv-store/src/dbs Index

// func TestDocumentDeletion(t *testing.T) {
// 	mockCtrl := gomock.NewController(t)
// 	defer mockCtrl.Finish()

// 	deleter := gomock.NewMockDocumentDeleter()
// 	index := gomock.NewMockIndex()

// 	mockDoer := mocks.NewMockDoer(mockCtrl)
// }

func deleteFile() {
	os.Remove(dbLoc)
}
