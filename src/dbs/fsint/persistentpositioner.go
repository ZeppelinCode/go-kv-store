package fsint

import (
	"io/ioutil"
	"os"

	"github.com/ZeppelinCode/go-kv-store/src/dbs/dbstructs"
)

// A PersistentDocumentPositioner is an implementation of the
// DocumentPositioner interface which uses a DocumentPositionerImpl
type PersistentDocumentPositioner struct {
	dpi           *DocumentPositionerImpl
	positionerLoc string
}

// NewPersistentDocumentPositioner creates/loads a file for the free document locations
func NewPersistentDocumentPositioner(dbloc string) *PersistentDocumentPositioner {
	positionerLoc := dbloc + "_f"
	f, err := os.OpenFile(positionerLoc, os.O_RDONLY, 0644)
	var dpi *DocumentPositionerImpl
	if err != nil {
		dp := DocumentPositionerImpl{}
		dpi = &dp
	} else {
		dpi = loadPositioner(f)
	}
	return &PersistentDocumentPositioner{dpi, positionerLoc}
}

func (dp *PersistentDocumentPositioner) getFreeLocations() []dbstructs.FreeSpace {
	return dp.dpi.getFreeLocations()
}

func (dp *PersistentDocumentPositioner) addFreeLocation(freedSpace dbstructs.FreeSpace) {
	dp.dpi.addFreeLocation(freedSpace)

	rewriteDocumentPositioner(dp)
}

func (dp *PersistentDocumentPositioner) appendDataBlockToStore(
	dbfh *DbFileHandler,
	b *dbstructs.DataBlock) (int, int64, error) {

	n, writtenAt, err := dp.dpi.appendDataBlockToStore(dbfh, b)
	if err == nil {
		rewriteDocumentPositioner(dp)
	}
	return n, writtenAt, err
}

func rewriteDocumentPositioner(dp *PersistentDocumentPositioner) error {
	marshalledDocumentIndex := dp.dpi.marshalPositioner()
	return ioutil.WriteFile(dp.positionerLoc, marshalledDocumentIndex, 0644)
}
