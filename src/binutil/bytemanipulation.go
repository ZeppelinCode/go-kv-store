package binutil

// Little Endian
func WriteIntToSliceAtLocation(b []byte, loc int, v int64) {
	b[loc] = byte(v)
	b[loc+1] = byte(v >> 8)
	b[loc+2] = byte(v >> 16)
	b[loc+3] = byte(v >> 24)
	b[loc+4] = byte(v >> 32)
	b[loc+5] = byte(v >> 40)
	b[loc+6] = byte(v >> 48)
	b[loc+7] = byte(v >> 56)
}

func CopyBytesAtLocation(dest []byte, loc int, src []byte) {
	for i, b := range src {
		dest[loc+i] = b
	}
}

// Little Endian
func Int64ToBytes(v int64) []byte {
	var b [8]byte
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)
	return b[:]
}
