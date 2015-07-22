package storage

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/pjvds/tidy"
)

var logger = tidy.GetLogger()

var ErrClosed = errors.New("closed")

type Log struct {
	directory string
	segments  []*Segment

	index       *Index
	idSequencer *MessageIdSequencer

	lock *sync.RWMutex

	activeSegment *Segment

	closed bool
}

func createOrEnsureDirectoryIsEmpty(directory string) error {
	info, err := os.Stat(directory)

	if err != nil {
		if os.IsNotExist(err) {
			// 0644
			return os.MkdirAll(directory, 0644)
		}

		return err
	}

	if !info.IsDir() {
		return errors.New("not a directory")
	}

	files, err := ioutil.ReadDir(directory)
	if err != nil {
		return err
	}

	if len(files) > 0 {
		return errors.New("directory not empty")
	}

	return nil
}

func InitializeLog(directory string) (*Log, error) {
	// we expect the directory to be non-existing or empty.
	if err := createOrEnsureDirectoryIsEmpty(directory); err != nil {
		logger.WithError(err).Withs(tidy.Fields{
			"directory": directory,
		}).Error("log initialization failed")

		return nil, err
	}

	filename := path.Join(directory, "0000000001.sd")
	segment, err := CreateSegment(filename, 1000*1000)

	if err != nil {
		logger.WithError(err).Error("segment creation failed")

		return nil, err
	}

	return &Log{
		index:         &Index{},
		idSequencer:   &MessageIdSequencer{},
		activeSegment: segment,
		lock:          new(sync.RWMutex),
	}, nil
}

func (this *Log) Close() {
	this.lock.Lock()
	defer this.lock.Unlock()

	// TODO: close segments?
}

// Append writes the message in the message set to the file system. This is
// done sequentualy and order is preserved. It returns the number of messages
// written to disk. If this number is not equal to the size of the provided
// message set, an error is also returned.
func (this *Log) Append(messages *MessageSet) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	if err := this.activeSegment.Append(messages, this.idSequencer); err != nil {
		return err
	}

	this.index.Append(messages)
	return nil
}

func (this *Log) ReadFrom(fromId MessageId, eagerFetchUntilMaxBytes int) (*MessageSet, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	logger := logger.Withs(tidy.Fields{
		"from_id":                     fromId,
		"eager_fetch_until_max_bytes": eagerFetchUntilMaxBytes,
	})

	if this.closed {
		err := ErrClosed

		logger.WithError(err).Debug("ReadFrom called while closed")
		return nil, err
	}

	entry, ok := this.index.FindLastLessOrEqual(fromId)
	if !ok {
		logger.Debug("no index entry found")

		return EmptyMessageSet, nil
	}
	logger.With("index_entry", entry).Debug("index entry found")

	buffer := make([]byte, eagerFetchUntilMaxBytes, eagerFetchUntilMaxBytes)
	// TODO: defer this.buffers.Put(buffer)

	// TODO: offset should already be a int64
	read, err := this.activeSegment.ReadAt(buffer, int64(entry.Offset))

	if err != nil && err != io.EOF {
		logger.WithError(err).Withs(tidy.Fields{
			"at":            entry.Offset,
			"buffer_length": len(buffer),
			"bytes_read":    read,
		}).Warn("failed to read from partition file")

		return nil, err
	}

	logger.Withs(tidy.Fields{
		"buffer_size": len(buffer),
		"read":        read,
	}).Debug("read from partition data file")

	if read == 0 {
		return nil, err
	}

	return NewMessageSetFromBuffer(buffer[0:read]), nil
}
