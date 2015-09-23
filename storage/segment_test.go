package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckSegment(t *testing.T) {
	directory := filepath.Join(os.TempDir(), "edgy", "test_check_segment")
	os.RemoveAll(directory)
	os.MkdirAll(directory, 0744)

	filename := filepath.Join(directory, "1.sd")

	segment, err := CreateSegment(SegmentRef{
		Topic:     "my-topic",
		Partition: PartitionId(1),
		Segment:   SegmentId(1),
	}, filename, 50*1000)

	lastMessageContent := []byte("42")

	messages := NewMessageSet([]RawMessage{
		NewMessage(1, []byte("foo bar")),
		NewMessage(2, []byte("baz")),
		NewMessage(3, lastMessageContent),
	})

	err = segment.Append(messages)
	assert.Nil(t, err)

	err = segment.Sync()
	assert.Nil(t, err)

	err = segment.Close()
	assert.Nil(t, err)

	file, err := os.Open(filename)

	check, err := checkSegment(file)
	assert.Nil(t, err)

	assert.Equal(t, false, check.IsEmpty)
	assert.Equal(t, MessageId(3), check.LastMessageId)
	assert.Equal(t, len(lastMessageContent), int(check.LastMessageContentLength))
	assert.Equal(t, messages.entries[2].Offset, int(check.LastMessagePosition))
}
