package storage

import "unsafe"

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

func ReadHeaderUnsafe(buffer []byte) Header {
	if len(buffer) < HEADER_LENGTH {
		panic("buffer size too small")
	}

	p := unsafe.Pointer(&buffer[0])

	return Header{
		Magic:         *(*byte)(unsafe.Pointer(uintptr(p))),
		MessageId:     *(*MessageId)(unsafe.Pointer(uintptr(p) + uintptr(INDEX_ID))),
		ContentLength: *(*int32)(unsafe.Pointer(uintptr(p) + uintptr(INDEX_LENGTH))),
		ContentHash:   *(*uint64)(unsafe.Pointer(uintptr(p) + uintptr(INDEX_HASH))),
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
