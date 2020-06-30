package fsint

import "github.com/ZeppelinCode/go-kv-store/src/dbs/dbstructs"

type DocumentUpdater struct {
	dp DocumentPositioner
}

func NewDocumentUpdater(dp DocumentPositioner) *DocumentUpdater {
	return &DocumentUpdater{dp}
}

// UpdateDocument updates a document which starts at position `pos` with a new `payload`
func (du *DocumentUpdater) UpdateDocument(dbfh *DbFileHandler, payload string, pos int64) error {
	doc, err := RetrieveDocument(dbfh, pos)
	if err != nil {
		return err
	}

	firstDataBlock := buildNewPayloadDataBlocks(dbfh, doc, payload)
	return writeAllDataBlocksToDisk(dbfh, firstDataBlock, du.dp)
}

// Contains an update context which keeps track of which
// data block we are currently updating and how much of the payload we've
// prepared
type uctx struct {
	payloadIndex             int                  // How much of the payload we've copied
	bPayload                 []byte               // Byte array of the whole payload
	headOfUpdatedDataBlock   *dbstructs.DataBlock // Pointer to the first payload data block
	currentDataBlock         *dbstructs.DataBlock // Current data block we are updating
	currentDataBlockLocation int64                // Index of our position in the current data block
	currentDataBlockBuffer   []byte               // Current data block contents
}

// Builds new payload data blocks by copying the old ones and updating the contents
// of the copies with the new payload. If there are not enough data blocks to house
// the new payload, a new data block is created in order to store the remaining data
func buildNewPayloadDataBlocks(
	dbfh *DbFileHandler,
	doc *dbstructs.Document,
	payload string) *dbstructs.DataBlock {

	headOfUpdatedDataBlock := doc.Head.Copy()
	c := &uctx{
		payloadIndex:             0,
		bPayload:                 []byte(payload),
		headOfUpdatedDataBlock:   headOfUpdatedDataBlock,
		currentDataBlock:         headOfUpdatedDataBlock,
		currentDataBlockLocation: int64(0),
		currentDataBlockBuffer:   []byte(headOfUpdatedDataBlock.Data),
	}

	// While we've still not copied all of the payload's contents
	for c.payloadIndex < len(c.bPayload) {
		// If the current data block has been filled out, move onto the next one
		if c.currentDataBlockLocation >= c.currentDataBlock.Length {
			c.currentDataBlock.Data = string(c.currentDataBlockBuffer)
			prepareNextDataBlock(dbfh, c)
		}

		// Copy payload data into the current data block and update indices
		c.currentDataBlockBuffer[c.currentDataBlockLocation] = c.bPayload[c.payloadIndex]
		c.currentDataBlockLocation++
		c.payloadIndex++
	}
	// Set the data for the last data block
	c.currentDataBlock.Data = string(c.currentDataBlockBuffer)
	return headOfUpdatedDataBlock
}

func prepareNextDataBlock(dbfh *DbFileHandler, c *uctx) {
	// Is there a data block after the current one
	nextDataBlock := c.currentDataBlock.Next.Link.Copy()

	// If yes: create a new and final data block
	if nextDataBlock == nil {
		nextDataBlock = dbstructs.NewDataBlock("")
		newDataBlockLength := len(c.bPayload) - c.payloadIndex
		nextDataBlock.Length = int64(newDataBlockLength)
		c.currentDataBlockBuffer = make([]byte, newDataBlockLength)
		c.currentDataBlock.Next.Location = dbfh.EofLocation
		c.currentDataBlockLocation = 0
	} else { // Else: use an existing data block
		c.currentDataBlockBuffer = []byte(nextDataBlock.Data)
		c.currentDataBlockLocation = 0
	}

	nextDataBlock.Prev.Link = c.currentDataBlock
	c.currentDataBlock.Next.Link = nextDataBlock
	c.currentDataBlock = nextDataBlock
	c.currentDataBlockLocation = int64(0)
}

// Writes all new data blocks in the place of the old data blocks
// Appends a potential new data block at the end of the file
func writeAllDataBlocksToDisk(dbfh *DbFileHandler, b *dbstructs.DataBlock, dp DocumentPositioner) error {
	currentDataBlock := b
	for currentDataBlock != nil {
		if currentDataBlock.Location == -1 {
			n, writtenAt, err := dp.appendDataBlockToStore(dbfh, currentDataBlock)
			if err == nil && writtenAt != -1 {
				dbfh.EofLocation += int64(n)
			}
			break
		}

		_, err := overrideDataBlock(dbfh, currentDataBlock)
		if err != nil {
			return err
		}
		currentDataBlock = currentDataBlock.Next.Link
	}
	return nil
}
