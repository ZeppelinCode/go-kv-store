package fsint

import (
	"bufio"
	"encoding/binary"
	"io"
	"math"

	"github.com/ZeppelinCode/go-kv-store/src/binutil"

	"github.com/ZeppelinCode/go-kv-store/src/dbs/dbstructs"
)

// A DocumentPositioner is responsible for keeping track of where a
// particular document should go in the file.
type DocumentPositioner interface {
	// Appends a data block either to the end of a file or to a best fit
	// free location, if available
	appendDataBlockToStore(*DbFileHandler, *dbstructs.DataBlock) (int, int64, error)
	// Returns all free locations
	getFreeLocations() []dbstructs.FreeSpace
	// Indexes a new free location in the file
	addFreeLocation(dbstructs.FreeSpace)
}

// DocumentPositionerImpl is an in-memory implementation of the DocumentPositioner interface
type DocumentPositionerImpl struct {
	freeLocations []dbstructs.FreeSpace
}

func (dp *DocumentPositionerImpl) marshalPositioner() []byte {
	// Size of freespace
	payload := make([]byte, len(dp.freeLocations)*16)
	for i, loc := range dp.freeLocations {
		marshalledLoc := loc.Marshal()
		binutil.CopyBytesAtLocation(payload, i*16, marshalledLoc)
	}
	return payload
}

func loadPositioner(r io.Reader) *DocumentPositionerImpl {
	freeSpaces := make([]dbstructs.FreeSpace, 0)
	currentFreeSpaceBytes := make([]byte, 16)

	br := bufio.NewReader(r)
	byteCount := 0

	for {
		// EOF check
		b, err := br.ReadByte()
		if err != nil {
			break
		}
		currentFreeSpaceBytes[byteCount] = b
		byteCount++

		// Read a full FreeSpace(Pos, Len) block => load it into memory
		if byteCount == 16 {
			byteCount = 0

			pos := int64(binary.LittleEndian.Uint64(currentFreeSpaceBytes[:8]))
			len := int64(binary.LittleEndian.Uint64(currentFreeSpaceBytes[8:]))
			fs := dbstructs.FreeSpace{Pos: pos, Len: len}
			freeSpaces = append(freeSpaces, fs)
		}

	}
	return &DocumentPositionerImpl{freeSpaces}
}

func (dp *DocumentPositionerImpl) getFreeLocations() []dbstructs.FreeSpace {
	return dp.freeLocations
}

func (dp *DocumentPositionerImpl) addFreeLocation(freedSpace dbstructs.FreeSpace) {
	dp.freeLocations = append(dp.freeLocations, freedSpace)
}

func (dp *DocumentPositionerImpl) appendDataBlockToStore(
	dbFileHandler *DbFileHandler,
	b *dbstructs.DataBlock) (written int, writtenAtPosition int64, err error) {
	marshalled := b.Marshal()

	bestFitIdx := findBestFitLocationForDataBlock(dp, b.Length)
	// The document can be fit into "hole" in the document blob
	if bestFitIdx >= 0 {
		freeSpace := dp.freeLocations[bestFitIdx]
		// dbfh.file.Seek(freeSpace.Pos, 1)
		n, err := dbFileHandler.file.WriteAt(marshalled, freeSpace.Pos)
		if err != nil {
			return -1, -1, err
		}

		// Need to update the free locations
		updateFreeSpace(dp, n, bestFitIdx)
		return n, freeSpace.Pos, nil
	}

	// Go to the end and append the new block
	dbFileHandler.file.Seek(0, 2) // 0 offset relative to the end of the file (2)
	n, err := dbFileHandler.file.Write(marshalled)
	return n, -1, err
}

func updateFreeSpace(
	dp *DocumentPositionerImpl,
	bytesWritten int,
	bestFitIndex int) {

	// Need to take the address to perform the udpate
	freeSpace := &dp.freeLocations[bestFitIndex]
	// If the free space has been completely exhausted -> remove it
	if freeSpace.Len == int64(bytesWritten) {
		dp.freeLocations = append(dp.freeLocations[0:bestFitIndex], dp.freeLocations[bestFitIndex+1:]...)
		return
	}

	// Reduce length
	freeSpace.Len = freeSpace.Len - int64(bytesWritten)
	freeSpace.Pos = freeSpace.Pos + int64(bytesWritten)
}

func findBestFitLocationForDataBlock(dp *DocumentPositionerImpl, dataBlockLen int64) int {
	// No free locations
	if len(dp.freeLocations) < 1 {
		return -1
	}

	bestFitIdx := -1
	var loss int64 = math.MaxInt64
	for i := 0; i < len(dp.freeLocations); i++ {
		newLoss := dp.freeLocations[i].Len - docSizePlusHeaders(dataBlockLen)

		// Perfect space match
		if newLoss == 0 {
			return i
		}

		if newLoss < loss && newLoss >= 0 {
			loss = newLoss
			bestFitIdx = i
		}
	}

	return bestFitIdx
}

func docSizePlusHeaders(size int64) int64 {
	return size + dbstructs.HeaderSize
}
