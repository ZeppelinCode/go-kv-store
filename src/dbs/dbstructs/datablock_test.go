package dbstructs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshalUnmarshal(t *testing.T) {
	source := DataBlock{
		DataBlockHeader{
			Length: 50,
			Prev:   &DataBlockLink{nil, 100},
			Next:   &DataBlockLink{nil, 150},
		},
		"potatoes",
	}

	marshalled := source.Marshal()
	unmarshalled := UnmarshalToDataBlock(marshalled, 1)

	assert.Equal(t, source.Length, unmarshalled.Length)
	assert.Equal(t, source.Prev.Location, unmarshalled.Prev.Location)
	assert.Equal(t, source.Next.Location, unmarshalled.Next.Location)
	assert.Equal(t, source.Data, unmarshalled.Data)
}

// 0000 0000 1111 1111 -> taking a byte off of this takes the lowest 8 bits -> 255
// v := int16(255)
// b := make([]byte, 2)
// b[0] = byte(v)
// b[1] = byte(v >> 8)
// fmt.Println(b) -> [255, 0]

// 0000 0001 0000 0000-> taking a byte off of this takes the lowest 8 bits -> 256
// v := int16(256)
// b := make([]byte, 2)
// b[0] = byte(v)
// b[1] = byte(v >> 8) // Shift the first 8 bits to the right and take their byte -> equals 1..
// fmt.Println(b) -> [0, 1]
