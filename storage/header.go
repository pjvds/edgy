package storage

type ScanResult struct {
	LastMessage Header
}

type Header struct {
	Magic         byte
	MessageId     MessageId
	ContentLength int32
	ContentHash   uint64
}

func ReadHeader(buffer []byte) Header {
	if len(buffer) < HEADER_LENGTH {
		panic("buffer size too small")
	}

	return Header{
		Magic:         buffer[INDEX_START],
		MessageId:     MessageId(byteOrder.Uint64(buffer[INDEX_ID:])),
		ContentLength: int32(byteOrder.Uint32(buffer[INDEX_LENGTH:])),
		ContentHash:   byteOrder.Uint64(buffer[INDEX_HASH:]),
	}
}

func (this Header) Write(buffer []byte) {
	if len(buffer) < HEADER_LENGTH {
		panic("buffer size too small")
	}

	buffer[INDEX_START] = this.Magic
	byteOrder.PutUint64(buffer[INDEX_ID:], uint64(this.MessageId))
	byteOrder.PutUint32(buffer[INDEX_LENGTH:], uint32(this.ContentLength))
	byteOrder.PutUint64(buffer[INDEX_HASH:], this.ContentHash)
}
