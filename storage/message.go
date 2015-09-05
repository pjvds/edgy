package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/OneOfOne/xxhash"
	"github.com/pjvds/tidy"
)

// Message format:
//
// byte    magic            (1 bytes) 0 (START_VALUE)
// int64   message id       (8 bytes) 1
// int32   content-length   (4 bytes) 9
// int64   xxhash           (8 bytes) 13
// []byte  content          (n bytes) 21
type RawMessage []byte

var byteOrder = binary.LittleEndian

const (
	START_VALUE = byte('<')

	INDEX_START   = 0
	INDEX_ID      = 1
	INDEX_LENGTH  = 9
	INDEX_HASH    = 13
	INDEX_CONTENT = 21

	HEADER_LENGTH = 22
)

func (this RawMessage) UpdateId(id MessageId) {
	byteOrder.PutUint64(this, uint64(id))
}

func (this RawMessage) Len() int {
	return len(this)
}

type MessageId uint64

type MessageIdSequencer struct {
	current MessageId
}

func NewMessage(id MessageId, content []byte) RawMessage {
	contentLen := len(content)
	size := HEADER_LENGTH + contentLen
	buffer := make([]byte, size)

	buffer[INDEX_START] = START_VALUE
	byteOrder.PutUint64(buffer[INDEX_ID:], uint64(id))
	byteOrder.PutUint32(buffer[INDEX_LENGTH:], uint32(contentLen))
	byteOrder.PutUint64(buffer[INDEX_HASH:], xxhash.Checksum64(content))
	copy(buffer[INDEX_CONTENT:], content)

	return RawMessage(buffer)
}

func (this *MessageIdSequencer) GetNext() MessageId {
	id := this.current
	this.current = this.current.Next()

	return id
}

func (this MessageId) Next() MessageId {
	return MessageId(this + 1)
}

type MessageHash int64

var EmptyMessageSet = &MessageSet{
	buffer:  make([]byte, 0, 0),
	entries: make([]SetEntry, 0, 0),
}

type MessageSet struct {
	buffer  []byte
	entries []SetEntry
}

type SetEntry struct {
	Id     MessageId
	Offset int
	Length int
	Hash   int64
}

func NewMessageSetFromBuffer(buffer []byte) *MessageSet {
	logger.With("buffer", tidy.Stringify(buffer)).Debug("creating message set from buffer")
	position := 0

	// TODO: inspect capacity during iteration and grow smarter.
	entries := make([]SetEntry, 0, 5)

	for position+HEADER_LENGTH < len(buffer) {
		if buffer[position+INDEX_START] != START_VALUE {
			// TODO: return error
			panic(fmt.Errorf("unexpected byte value: expected start value %v at %v, but got %v", START_VALUE, position+INDEX_START, buffer[position+INDEX_START]))
		}

		entry := SetEntry{
			Id:     MessageId(byteOrder.Uint64(buffer[position+INDEX_ID:])),
			Length: int(byteOrder.Uint32(buffer[position+INDEX_LENGTH:])),
			Hash:   int64(byteOrder.Uint64(buffer[position+INDEX_HASH:])),
			Offset: int(position),
		}

		valueStart := position + INDEX_CONTENT
		valueEnd := valueStart + entry.Length

		if valueEnd > len(buffer) {
			break
		}

		entries = append(entries, entry)

		position = valueEnd + 1

		logger.Withs(tidy.Fields{
			"entry":    tidy.Stringify(entry),
			"position": entry.Offset,
			"next_at":  position,
		}).Debug("entry read")
	}

	return &MessageSet{
		buffer:  buffer,
		entries: entries,
	}
}

func NewMessageSet(messages []RawMessage) *MessageSet {
	offset := 0
	size := 0

	for _, message := range messages {
		size += message.Len()
	}

	buffer := make([]byte, size)

	for _, message := range messages {
		copy(buffer[offset:], message)
		offset += message.Len()
	}

	return NewMessageSetFromBuffer(buffer)
}

// Len returns the number of raw bytes of the total set.
func (this *MessageSet) Len() int {
	return len(this.buffer)
}

// Len returns the number of raw bytes of the total set.
func (this *MessageSet) Len64() int64 {
	return int64(this.Len())
}

func (this *MessageSet) Align(sequencer *MessageIdSequencer) {
	offset := int32(0)

	for index, entry := range this.entries {
		id := sequencer.GetNext()

		entry.Id = id
		byteOrder.PutUint64(this.buffer[entry.Offset+INDEX_ID:], uint64(id))

		this.entries[index] = entry

		offset += int32(entry.Offset)
	}
}
