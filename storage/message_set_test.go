package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageSetAlign(t *testing.T) {
	set := NewMessageSet([]RawMessage{
		NewMessage(0, []byte("foo")),
		NewMessage(0, []byte("bar")),
	})

	set.Align(5)

	assert.Equal(t, MessageId(6), set.entries[0].Id)
	assert.Equal(t, MessageId(7), set.entries[1].Id)

	assert.Equal(t, MessageId(6), byteOrder.Uint64(set.buffer[set.entries[0].Offset+INDEX_ID:]))
	assert.Equal(t, MessageId(7), byteOrder.Uint64(set.buffer[set.entries[1].Offset+INDEX_ID:]))
}
