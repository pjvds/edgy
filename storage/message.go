package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/OneOfOne/xxhash/native"
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

	HEADER_LENGTH = 21

	END_OF_SEGMENT = byte('>')
)

func (this RawMessage) UpdateId(id MessageId) {
	byteOrder.PutUint64(this, uint64(id))
}

func (this RawMessage) Len() int {
	return len(this)
}

type MessageId uint64

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

func (this MessageId) Next() MessageId {
	return MessageId(this + 1)
}

func (this MessageId) NextN(n int) MessageId {
	return this + MessageId(n)
}

func (this MessageId) String() string {
	value := uint64(this)
	return fmt.Sprint(value)
}

type MessageHash int64

var EmptyMessageSet = &MessageSet{
	buffer:  make([]byte, 0, 0),
	entries: make([]SetEntry, 0, 0),
}

type Offset struct {
	MessageId    MessageId
	SegmentId    SegmentId
	LastPosition int64
}

func (this Offset) String() string {
	if this.IsEmpty() {
		return "<empty>"
	}
}

func (this Offset) IsEmpty() bool {
	return this.MessageId == MessageId(0) &&
		this.SegmentId == SegmentId(0) &&
		this.LastPosition == 0
}

type MessageSet struct {
	buffer  []byte
	entries []SetEntry
}

func (this *MessageSet) Messages() []RawMessage {
	result := make([]RawMessage, len(this.entries))

	for index, entry := range this.entries {
		result[index] = RawMessage(this.buffer[entry.Offset : entry.Offset+entry.Length+HEADER_LENGTH])
	}

	return result
}
func (this *MessageSet) Buffer() []byte {
	if last, ok := this.LastEntry(); ok {
		return this.buffer[0 : last.Offset+last.Length]
	}

	return make([]byte, 0)
}

func (this *MessageSet) LastEntry() (SetEntry, bool) {
	if len(this.entries) == 0 {
		return SetEntry{}, false
	}

	return this.entries[len(this.entries)-1], true
}

type SetEntry struct {
	Id     MessageId
	Offset int
	Length int
	Hash   uint64
}

func NewMessageSetFromBuffer(buffer []byte) *MessageSet {
	position := 0

	// TODO: inspect capacity during iteration and grow smarter.
	entries := make([]SetEntry, 0, 5)

	for position+HEADER_LENGTH < len(buffer) {
		header := ReadHeader(buffer[position:])

		if header.Magic != START_VALUE {
			if header.Magic != 0 && header.Magic != END_OF_SEGMENT {
				// TODO: return error
				panic(fmt.Errorf("unexpected byte value: expected start value %v at %v, but got %v", START_VALUE, position+INDEX_START, buffer[position+INDEX_START]))
			}
			break
		}

		entry := SetEntry{
			Id:     header.MessageId,
			Length: int(header.ContentLength),
			Hash:   header.ContentHash,
			Offset: int(position),
		}

		valueStart := position + INDEX_CONTENT
		valueEnd := valueStart + entry.Length

		if valueEnd > len(buffer) {
			break
		}

		entries = append(entries, entry)
		position += int(HEADER_LENGTH + header.ContentLength)
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
func (this *MessageSet) DataLen() int {
	return len(this.buffer)
}

// Len returns the number of raw bytes of the total set.
func (this *MessageSet) DataLen64() int64 {
	return int64(this.DataLen())
}

func (this *MessageSet) MessageCount() int {
	return len(this.entries)
}

// Align the ids of the messages in the set with
// the provided start id.
//
// If there are 5 messages in the set, and the provided
// fromId is 12, the the messages will have the following
// ids:
// [0:13, 1:14, 2:15, 3:16, 4:17]
func (this *MessageSet) Align(fromId MessageId) {
	id := fromId.Next()

	for index, entry := range this.entries {
		entry.Id = id
		byteOrder.PutUint64(this.buffer[entry.Offset+INDEX_ID:], uint64(id))

		this.entries[index] = entry
		id = id.Next()
	}
}
