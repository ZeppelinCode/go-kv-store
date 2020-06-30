package fsint

import "github.com/ZeppelinCode/go-kv-store/src/dbs/dbstructs"

type DocumentCreator struct {
	dp DocumentPositioner
}

func NewDocumentCreator(dp DocumentPositioner) *DocumentCreator {
	return &DocumentCreator{dp}
}

// StoreNewDocument stores a new data block at the end of the file
func (dc *DocumentCreator) StoreNewDocument(dbfh *DbFileHandler, payload string) (int, int64, error) {
	dataBlock := dbstructs.NewDataBlock(payload)
	return dc.dp.appendDataBlockToStore(dbfh, dataBlock)
}
