package storage

import (
	"testing"

	"github.com/OneOfOne/xxhash"
	"github.com/stretchr/testify/assert"
)

func TestNewMessage(t *testing.T) {
	id := MessageId(5)
	content := []byte("foobar")
	message := NewMessage(id, content)

	header := ReadHeader(message)
	assert.Equal(t, START_VALUE, header.Magic)
	assert.Equal(t, id, header.MessageId)
	assert.Equal(t, len(content), int(header.ContentLength))
	assert.Equal(t, string(content), string(message[INDEX_CONTENT:]))
	assert.Equal(t, xxhash.Checksum64(message[INDEX_CONTENT:]), header.ContentHash)
}

func TestMessageSetAlign(t *testing.T) {
	set := NewMessageSet([]RawMessage{
		NewMessage(0, []byte("foo")),
		NewMessage(0, []byte("bar")),
	})

	set.Align(5)

	assert.Equal(t, MessageId(6), set.entries[0].Id)
	assert.Equal(t, MessageId(7), set.entries[1].Id)

	assert.Equal(t, MessageId(6), MessageId(byteOrder.Uint64(set.buffer[set.entries[0].Offset+INDEX_ID:])))
	assert.Equal(t, MessageId(7), MessageId(byteOrder.Uint64(set.buffer[set.entries[1].Offset+INDEX_ID:])))
}
