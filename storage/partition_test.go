package storage

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/pjvds/randombytes"
	"github.com/stretchr/testify/assert"
)

func TestOpenPartition(t *testing.T) {
	directory := filepath.Join(os.TempDir(), "edgy", "test_open_partition")
	os.RemoveAll(directory)
	os.MkdirAll(directory, 0744)

	partitionRef := PartitionRef{
		Topic:     "my-topic",
		Partition: PartitionId(1),
	}

	config := PartitionConfig{
		SegmentSize: 100 * 1000,
	}

	init := func() error {
		partition, err := CreatePartition(partitionRef, config, directory)

		if err != nil {
			return err
		}
		defer partition.Close()

		for i := 0; i < 1000; i++ {
			if err = partition.Append(NewMessageSet([]RawMessage{
				//NewMessage(0, randombytes.Make(500-HEADER_LENGTH)),
				NewMessage(0, []byte("XxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXx")),
			})); err != nil {
				return err
			}
		}

		return nil
	}

	if err := init(); err != nil {
		t.Fatal(err)
	}

	partition, err := OpenPartition(partitionRef, config, directory)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, partition.segments.Last().ref, partitionRef.ToSegmentRef(801))
}

func TestAppendRollingSegments(t *testing.T) {
	directory, err := ioutil.TempDir("", "edgy_test_append_append_rolling_segments_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)

	partition, err := CreatePartition(
		PartitionRef{
			Topic:     "my-topic",
			Partition: PartitionId(1),
		},
		PartitionConfig{
			SegmentSize: 50,
		}, directory)

	if err != nil {
		t.Fatal(err)
	}
	defer partition.Close()

	err = partition.Append(NewMessageSet([]RawMessage{
		NewMessage(0, randombytes.Make(50-HEADER_LENGTH)),
	}))
	assert.Nil(t, err)

	err = partition.Append(NewMessageSet([]RawMessage{
		NewMessage(0, randombytes.Make(50-HEADER_LENGTH)),
	}))
	assert.Nil(t, err)

	fi, err := ioutil.ReadDir(directory)
	assert.Nil(t, err)
	assert.Len(t, fi, 2)
}

func TestAppendRoundtrip(t *testing.T) {
	directory, err := ioutil.TempDir("", "edgy_test_append_roundtrip_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)

	partition, err := CreatePartition(PartitionRef{
		Topic:     "my-topic",
		Partition: PartitionId(1),
	}, DefaultConfig, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer partition.Close()

	err = partition.Append(NewMessageSet([]RawMessage{
		NewMessage(0, []byte("foo bar")),
		NewMessage(0, []byte("baz")),
		NewMessage(0, []byte("42")),
	}))
	assert.Nil(t, err)

	readResult, err := partition.ReadFrom(Offset{}, 1000)
	assert.Nil(t, err)
	assert.Len(t, readResult.entries, 3)

	assert.Equal(t, MessageId(1), readResult.entries[0].Id)
	assert.Equal(t, MessageId(2), readResult.entries[1].Id)
	assert.Equal(t, MessageId(3), readResult.entries[2].Id)
}
