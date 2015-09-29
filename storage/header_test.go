package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadHeader(t *testing.T) {
	header := Header{
		Magic:         START_VALUE,
		MessageId:     MessageId(42),
		ContentLength: 22,
		ContentHash:   88,
	}

	buffer := make([]byte, HEADER_LENGTH)
	header.Write(buffer)

	result := ReadHeader(buffer)
	assert.Equal(t, header.Magic, result.Magic)
	assert.Equal(t, header.MessageId, result.MessageId)
	assert.Equal(t, header.ContentLength, result.ContentLength)
	assert.Equal(t, header.ContentHash, result.ContentHash)
}
func TestReadHeaderUnsafe(t *testing.T) {
	header := Header{
		Magic:         START_VALUE,
		MessageId:     MessageId(42),
		ContentLength: 22,
		ContentHash:   88,
	}

	buffer := make([]byte, HEADER_LENGTH)
	header.Write(buffer)

	result := ReadHeaderUnsafe(buffer)
	assert.Equal(t, header.Magic, result.Magic)
	assert.Equal(t, header.MessageId, result.MessageId)
	assert.Equal(t, header.ContentLength, result.ContentLength)
	assert.Equal(t, header.ContentHash, result.ContentHash)
}

func BenchmarkReadHeader(b *testing.B) {
	header := Header{
		Magic:         START_VALUE,
		MessageId:     MessageId(42),
		ContentLength: 22,
		ContentHash:   88,
	}

	buffer := make([]byte, HEADER_LENGTH)
	header.Write(buffer)

	for i := 0; i < b.N; i++ {
		ReadHeader(buffer)
	}
}

func BenchmarkReadHeaderUnsafe(b *testing.B) {
	header := Header{
		Magic:         START_VALUE,
		MessageId:     MessageId(42),
		ContentLength: 22,
		ContentHash:   88,
	}

	buffer := make([]byte, HEADER_LENGTH)
	header.Write(buffer)

	for i := 0; i < b.N; i++ {
		ReadHeaderUnsafe(buffer)
	}
}
