package storage

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/pjvds/randombytes"
	"github.com/stretchr/testify/assert"
)

func TestAppendRollingSegments(t *testing.T) {
	directory, err := ioutil.TempDir("", "edgy_test_append_append_rolling_segments_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)

	log, err := InitializeLog(LogConfig{
		SegmentSize: 50,
	}, directory)

	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	err = log.Append(NewMessageSet([]RawMessage{
		NewMessage(0, randombytes.Make(50-HEADER_LENGTH)),
	}))
	assert.Nil(t, err)

	err = log.Append(NewMessageSet([]RawMessage{
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

	log, err := InitializeLog(DefaultConfig, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	err = log.Append(NewMessageSet([]RawMessage{
		NewMessage(0, []byte("foo bar")),
		NewMessage(0, []byte("baz")),
		NewMessage(0, []byte("42")),
	}))
	assert.Nil(t, err)

	readResult, err := log.ReadFrom(MessageId(1), 1000)
	assert.Nil(t, err)
	assert.Len(t, readResult.entries, 2)

	assert.Equal(t, MessageId(1), readResult.entries[0].Id)
	assert.Equal(t, MessageId(2), readResult.entries[1].Id)
}
