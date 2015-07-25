package storage

import (
	"errors"
	"fmt"
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
	config LogConfig

	directory     string
	segments      []*Segment
	activeSegment *Segment

	index       *Index
	idSequencer *MessageIdSequencer

	lock *sync.RWMutex

	closed bool
}

type LogConfig struct {
	SegmentSize int64
}

var DefaultConfig = LogConfig{
	SegmentSize: 1000 * 1000,
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

func InitializeLog(config LogConfig, directory string) (*Log, error) {
	// we expect the directory to be non-existing or empty.
	if err := createOrEnsureDirectoryIsEmpty(directory); err != nil {
		logger.WithError(err).Withs(tidy.Fields{
			"directory": directory,
		}).Error("log initialization failed")

		return nil, err
	}

	filename := path.Join(directory, "1.sd")
	segment, err := CreateSegment(filename, config.SegmentSize)

	if err != nil {
		logger.WithError(err).Error("segment creation failed")

		return nil, err
	}

	return &Log{
		index:         &Index{},
		idSequencer:   &MessageIdSequencer{},
		segments:      []*Segment{segment},
		activeSegment: segment,
		lock:          new(sync.RWMutex),
		directory:     directory,
		config:        config,
	}, nil
}

func (this *Log) Close() {
	this.lock.Lock()
	defer this.lock.Unlock()

	// TODO: close segments?
}

func (this *Log) rollToNextSegment() error {
	filename := path.Join(this.directory, fmt.Sprint(len(this.segments)+1, ".sd"))
	nextSegment, err := CreateSegment(filename, this.config.SegmentSize)

	if err != nil {
		logger.WithError(err).Debug("segment creation failed")
		return err
	}

	this.segments = append(this.segments, nextSegment)
	this.activeSegment = nextSegment

	logger.With("segment", tidy.Stringify(this.activeSegment)).Debug("rolled to new segment")

	return nil
}

// Append writes the messages in the set to the file system. The order is preserved.
func (this *Log) Append(messages *MessageSet) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	if err := this.activeSegment.Append(messages, this.idSequencer); err != nil {
		logger.WithError(err).Debug("error appending to segment")

		if err == ErrSegmentFull {
			logger.With("name", this.activeSegment.file.Name()).Debug("creating new segment because the current one is full")

			if err := this.rollToNextSegment(); err != nil {
				logger.WithError(err).Debug("failed to roll to next segment")

				return err
			}

			return this.activeSegment.Append(messages, this.idSequencer)
		}
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
