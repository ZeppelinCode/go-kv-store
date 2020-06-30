package fsint

import (
	"github.com/ZeppelinCode/go-kv-store/src/dbs/dbstructs"
)

// RetrieveDocument retrieves a document which starts at a certain position
// All data block links of the document are traversed
func RetrieveDocument(dbfs *DbFileHandler, pos int64) (*dbstructs.Document, error) {
	firstDataBlock, err := getDataBlock(dbfs, pos)
	if err != nil {
		return nil, err
	}

	err = linkAllDataBlocksForDoc(dbfs, firstDataBlock)
	if err != nil {
		return nil, err
	}

	return &dbstructs.Document{firstDataBlock}, nil
}

// Retrieves a single data block at a particular position
func getDataBlock(dbfs *DbFileHandler, pos int64) (*dbstructs.DataBlock, error) {
	headerBlob := make([]byte, dbstructs.HeaderSize)
	_, err := dbfs.file.ReadAt(headerBlob, pos)
	if err != nil {
		return nil, err
	}

	blockHeader := dbstructs.UnmarshalDataBlockHeader(headerBlob, pos)
	dataBlob := make([]byte, blockHeader.Length)

	_, err = dbfs.file.ReadAt(dataBlob, pos+dbstructs.HeaderSize)
	if err != nil {
		return nil, err
	}

	return &dbstructs.DataBlock{
		blockHeader,
		string(dataBlob),
	}, nil
}

// Reads all data block links of `first` from first and links them together
func linkAllDataBlocksForDoc(dbfs *DbFileHandler, first *dbstructs.DataBlock) error {
	currentDataBlock := first

	nextDataBlockLocation := currentDataBlock.Next.Location
	for nextDataBlockLocation != int64(-1) {
		nextDataBlock, err := getDataBlock(dbfs, nextDataBlockLocation)
		if err != nil {
			return err
		}

		nextDataBlock.Prev.Link = currentDataBlock
		nextDataBlock.Prev.Location = currentDataBlock.Location
		currentDataBlock.Next.Link = nextDataBlock

		currentDataBlock = nextDataBlock
		nextDataBlockLocation = currentDataBlock.Next.Location
	}

	return nil
}
