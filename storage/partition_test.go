package storage

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppendRoundtrip(t *testing.T) {
	directory, err := ioutil.TempDir("", "edgy_test_append_roundtrip_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)

	log, err := InitializeLog(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	messages := NewMessageSet([]RawMessage{
		NewMessage(0, []byte("foo bar")),
		NewMessage(1, []byte("baz")),
		NewMessage(2, []byte("42")),
	})

	err = log.Append(messages)
	assert.Nil(t, err)

	readResult, err := log.ReadFrom(MessageId(1), 1000)
	assert.Nil(t, err)
	assert.Len(t, readResult.entries, 2)
}
