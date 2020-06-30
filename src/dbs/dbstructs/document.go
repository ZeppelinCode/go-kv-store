package dbstructs

import (
	"bytes"
)

// A Document is a single document in the binary blob
type Document struct {
	Head *DataBlock
}

func (d *Document) String() string {
	var buffer bytes.Buffer

	currentBlock := d.Head
	for currentBlock != nil {
		buffer.WriteString(currentBlock.Data)
		currentBlock = currentBlock.Next.Link
	}
	return buffer.String()
}
