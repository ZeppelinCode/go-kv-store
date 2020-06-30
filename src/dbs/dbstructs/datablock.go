package dbstructs

import (
	"encoding/binary"

	"github.com/ZeppelinCode/go-kv-store/src/binutil"
)

// HeaderSize is the size of a DataBlockHeader in bytes
const HeaderSize int64 = 24

// A DataBlock represents a linked list of data
type DataBlock struct {
	DataBlockHeader
	Data string
}

// DataBlockHeader contains information about the current data block
// This is represented as | Length | PrevLocation | NextLocation | binary blob
// in the file system
type DataBlockHeader struct {
	Length   int64
	Location int64
	Prev     *DataBlockLink
	Next     *DataBlockLink
}

// DataBlockLink in memory and disk representation of a data block link
type DataBlockLink struct {
	Link     *DataBlock
	Location int64
}

// NewDataBlock creates a new data block with the provided data and no links
func NewDataBlock(data string) *DataBlock {
	numberOfBytes := len([]byte(data))
	return &DataBlock{
		DataBlockHeader{
			int64(numberOfBytes),
			-1,
			&DataBlockLink{nil, -1},
			&DataBlockLink{nil, -1},
		},
		data,
	}
}

// Marshal encodes a DataBlock into a binary format
func (db *DataBlock) Marshal() []byte {
	dataSize := len([]byte(db.Data))

	// Two Data Block Pointers and the size of the Length -> 3*8 = 24 bytes
	payload := make([]byte, 24+dataSize)
	binutil.WriteIntToSliceAtLocation(payload, 0, db.Length)

	binutil.WriteIntToSliceAtLocation(payload, 8, prevOrNextLocation(db.Prev))
	binutil.WriteIntToSliceAtLocation(payload, 16, prevOrNextLocation(db.Next))
	binutil.CopyBytesAtLocation(payload, 24, []byte(db.Data))
	return payload
}

// UnmarshalToDataBlock decodes a binary blob to a data block
func UnmarshalToDataBlock(b []byte, loc int64) *DataBlock {
	data := string(b[24:])
	return &DataBlock{
		UnmarshalDataBlockHeader(b[:24], loc),
		data,
	}
}

// UnmarshalDataBlockHeader unmarshals only the header portion of a data block
// at location loc
func UnmarshalDataBlockHeader(b []byte, loc int64) DataBlockHeader {
	length := int64(binary.LittleEndian.Uint64(b[:8]))
	prevLoc := int64(binary.LittleEndian.Uint64(b[8:16]))
	nextLoc := int64(binary.LittleEndian.Uint64(b[16:24]))
	return DataBlockHeader{
		length,
		loc,
		&DataBlockLink{nil, prevLoc},
		&DataBlockLink{nil, nextLoc},
	}
}

// Copy returns a copy of a data block
func (db *DataBlock) Copy() *DataBlock {
	if db == nil {
		return nil
	}

	return &DataBlock{
		copyHeader(&db.DataBlockHeader),
		db.Data,
	}
}

// Copy of a data block header
func copyHeader(dh *DataBlockHeader) DataBlockHeader {
	return DataBlockHeader{
		dh.Length,
		dh.Location,
		copyDataBlockLink(dh.Prev),
		copyDataBlockLink(dh.Next),
	}
}

// Copy of a data block link
func copyDataBlockLink(dl *DataBlockLink) *DataBlockLink {
	if dl == nil {
		return nil
	}

	return &DataBlockLink{
		dl.Link,
		dl.Location,
	}
}

func prevOrNextLocation(dl *DataBlockLink) int64 {
	if dl == nil {
		return -1
	}
	return dl.Location
}
