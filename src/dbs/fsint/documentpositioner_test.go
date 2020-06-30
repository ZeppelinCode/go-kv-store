package fsint

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ZeppelinCode/go-kv-store/src/dbs/dbstructs"
)

func TestDocumentPositionerSpaceWhenEmpty(t *testing.T) {
	dp := &DocumentPositionerImpl{}
	idx := findBestFitLocationForDataBlock(dp, 1)
	assert.Equal(t, -1, idx)
}

func TestDocumentPositionerPicksBestFit(t *testing.T) {
	dp := testGenerateDocumentPositioner()
	idx := findBestFitLocationForDataBlock(dp, 6) // 30 - header
	assert.Equal(t, 1, idx)
}

func TestDocumentPositionerEdge(t *testing.T) {
	dp := testGenerateDocumentPositioner()
	idx := findBestFitLocationForDataBlock(dp, 26) // 50 - header
	assert.Equal(t, 0, idx)
}

func TestDocumentPositionerReturnsNothingWhenNoFit(t *testing.T) {
	dp := testGenerateDocumentPositioner()
	idx := findBestFitLocationForDataBlock(dp, 61)
	assert.Equal(t, -1, idx)
}

func TestUpdateFreeSpaceRemovesEntryIfWholeSpaceGotFilled(t *testing.T) {
	dp := testGenerateDocumentPositioner()
	updateFreeSpace(dp, 30, 1)
	assert.Equal(t, 2, len(dp.freeLocations))
	assert.Equal(t, int64(50), dp.freeLocations[0].Len)
	assert.Equal(t, int64(60), dp.freeLocations[1].Len)
}

func TestUpdateFreeSpaceUpdatesFreeEntryWhenLocationNotExhausted(t *testing.T) {
	dp := testGenerateDocumentPositioner()
	updateFreeSpace(dp, 25, 1)
	assert.Equal(t, 3, len(dp.freeLocations))
	assert.Equal(t, int64(50), dp.freeLocations[0].Len)
	assert.Equal(t, int64(0), dp.freeLocations[0].Pos)
	assert.Equal(t, int64(5), dp.freeLocations[1].Len)
	assert.Equal(t, int64(125), dp.freeLocations[1].Pos)
	assert.Equal(t, int64(60), dp.freeLocations[2].Len)
	assert.Equal(t, int64(200), dp.freeLocations[2].Pos)
}

func TestLoadPositioner(t *testing.T) {
	inp := []byte{
		0, 0, 0, 0, 0, 0, 0, 0,
		50, 0, 0, 0, 0, 0, 0, 0,
		100, 0, 0, 0, 0, 0, 0, 0,
		30, 0, 0, 0, 0, 0, 0, 0,
		200, 0, 0, 0, 0, 0, 0, 0,
		60, 0, 0, 0, 0, 0, 0, 0,
	}
	bb := bytes.NewBuffer(inp)
	dpi := loadPositioner(bb)
	assert.Equal(t, testGenerateDocumentPositioner(), dpi)
}

func TestMarshalPositioner(t *testing.T) {
	dp := testGenerateDocumentPositioner()
	marshalled := dp.marshalPositioner()
	expected := []byte{
		0, 0, 0, 0, 0, 0, 0, 0,
		50, 0, 0, 0, 0, 0, 0, 0,
		100, 0, 0, 0, 0, 0, 0, 0,
		30, 0, 0, 0, 0, 0, 0, 0,
		200, 0, 0, 0, 0, 0, 0, 0,
		60, 0, 0, 0, 0, 0, 0, 0,
	}
	assert.Equal(t, expected, marshalled)
}

func testGenerateDocumentPositioner() *DocumentPositionerImpl {
	return &DocumentPositionerImpl{
		[]dbstructs.FreeSpace{
			dbstructs.FreeSpace{Pos: 0, Len: 50},
			dbstructs.FreeSpace{Pos: 100, Len: 30},
			dbstructs.FreeSpace{Pos: 200, Len: 60},
		},
	}
}
