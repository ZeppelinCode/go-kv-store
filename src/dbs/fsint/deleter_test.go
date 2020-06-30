package fsint

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ZeppelinCode/go-kv-store/src/dbs/dbstructs"
)

func TestDeleteDocument(t *testing.T) {
	block1payload := "hello there"
	block2payload := "general Kenobi"

	block1 := dbstructs.NewDataBlock(block1payload)
	block1.Location = 10
	block2 := dbstructs.NewDataBlock(block2payload)
	block2.Location = 30
	block1.Next.Link = block2

	doc := &dbstructs.Document{block1}
	docPositioner := DocumentPositionerImpl{}
	deleter := &DocumentDeleter{&docPositioner}
	deleter.DeleteDocument(doc)

	freedSpaceOfBlock1 := docPositioner.freeLocations[0]
	freedSpaceOfBlock2 := docPositioner.freeLocations[1]

	assert.Equal(t, int64(24)+int64(len([]byte(block1payload))), freedSpaceOfBlock1.Len)
	assert.Equal(t, int64(10), freedSpaceOfBlock1.Pos)
	assert.Equal(t, int64(24)+int64(len([]byte(block2payload))), freedSpaceOfBlock2.Len)
	assert.Equal(t, int64(30), freedSpaceOfBlock2.Pos)
}
