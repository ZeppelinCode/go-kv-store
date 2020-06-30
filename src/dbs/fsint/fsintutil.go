package fsint

import "github.com/ZeppelinCode/go-kv-store/src/dbs/dbstructs"

// Appends a data block to the end of the file
func appendDataBlockToStore(dbfh *DbFileHandler, b *dbstructs.DataBlock) (int, error) {
	marshalled := b.Marshal()
	// Go to the end when appending
	dbfh.file.Seek(0, 2)
	n, err := dbfh.file.Write(marshalled)
	return n, err
}

// Overrides the contents of an existing data block
func overrideDataBlock(dbfh *DbFileHandler, db *dbstructs.DataBlock) (int, error) {
	marshalled := db.Marshal()
	n, err := dbfh.file.WriteAt(marshalled, db.Location)
	return n, err
}
