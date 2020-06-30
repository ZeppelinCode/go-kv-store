package dbstructs

import (
	"github.com/ZeppelinCode/go-kv-store/src/binutil"
)

// FreeSpace represents a free space in the db file
// Stored as Pos | Len binary blob on the file system
type FreeSpace struct {
	Pos int64
	Len int64
}

// Marshal converts a FreeSpace struct into a Pos | Len byte array
// ready to be written to disk
func (f FreeSpace) Marshal() []byte {
	payload := make([]byte, 16)
	binutil.WriteIntToSliceAtLocation(payload, 0, f.Pos)
	binutil.WriteIntToSliceAtLocation(payload, 8, f.Len)
	return payload
}
