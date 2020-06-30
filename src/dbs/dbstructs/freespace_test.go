package dbstructs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshal(t *testing.T) {
	freeSpace := FreeSpace{55, 577}
	marshalled := freeSpace.Marshal()

	// 55 0 0 0 0 0 0 0 65 2 0 0 0 0 0 0
	assert.Equal(t, uint8(55), marshalled[0])
	assert.Equal(t, uint8(65), marshalled[8])
	assert.Equal(t, uint8(2), marshalled[9])
}
