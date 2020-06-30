package dbstructs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataDocumentString(t *testing.T) {
	block1 := NewDataBlock("block1 ")
	block2 := NewDataBlock("block2")
	block1.Next.Link = block2

	doc := &Document{block1}

	assert.Equal(t, "block1 block2", doc.String())
}
