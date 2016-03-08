package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOffsetEndOfStream(t *testing.T) {
	assert.True(t, Offset{
		MessageId: MessageId(^uint64(0)),
		SegmentId: SegmentId(^uint64(0)),
		Position:  ^int64(0),
	}.IsEndOfStream())
}
