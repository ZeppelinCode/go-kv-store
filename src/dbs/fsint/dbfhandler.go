package fsint

import "os"

// DbFileHandler is a wrapper around the file in which all data is stored
type DbFileHandler struct {
	file        *os.File // File Handler
	EofLocation int64    // end location of file; used to create data block links
}

// NewDbFileHandler creates/opens an existing file and returns an object
func NewDbFileHandler(file string) (*DbFileHandler, error) {
	// In go files are not truncated by default it seems...
	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	fileStat, _ := f.Stat()
	return &DbFileHandler{f, fileStat.Size()}, nil
}
