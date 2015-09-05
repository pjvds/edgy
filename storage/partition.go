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

var ErrClosed = errors.New("closed")

type PartitionId struct {
	Topic     string
	Partition int32
}

func (this PartitionId) String() string {
	return fmt.Sprintf("%v/%v", this.Topic, this.Partition)
}

type Partition struct {
	id     PartitionId
	config PartitionConfig

	directory     string
	segments      []*Segment
	activeSegment *Segment

	index       *Index
	idSequencer *MessageIdSequencer

	lock *sync.RWMutex

	closed bool
	logger tidy.Logger
}

type PartitionConfig struct {
	SegmentSize int64
}

var DefaultConfig = PartitionConfig{
	SegmentSize: 1000 * 1000,
}

func createOrEnsureDirectoryIsEmpty(directory string) error {
	info, err := os.Stat(directory)

	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(directory, 0644)
		}

		return err
	}

	if !info.IsDir() {
		return errors.New("not a directory")
	}

	// TODO: validate access rights?

	files, err := ioutil.ReadDir(directory)
	if err != nil {
		return err
	}

	if len(files) > 0 {
		return errors.New("directory not empty")
	}

	return nil
}

func InitializePartition(id PartitionId, config PartitionConfig, directory string) (*Partition, error) {
	logger := tidy.GetLogger().With("partition", id.String())

	// we expect the directory to be non-existing or empty.
	if err := createOrEnsureDirectoryIsEmpty(directory); err != nil {
		logger.WithError(err).Withs(tidy.Fields{
			"directory": directory,
		}).Error("log initialization failed")

		return nil, err
	}

	filename := path.Join(directory, "1.sd")
	segment, err := CreateSegment(
		SegmentId{
			Topic:     id.Topic,
			Partition: id.Partition,
			Segment:   1,
		}, filename, config.SegmentSize)

	if err != nil {
		logger.
			WithError(err).
			With("filename", filename).
			Error("segment creation failed")

		return nil, err
	}

	return &Partition{
		id:            id,
		index:         &Index{},
		idSequencer:   &MessageIdSequencer{},
		segments:      []*Segment{segment},
		activeSegment: segment,
		lock:          new(sync.RWMutex),
		directory:     directory,
		config:        config,
		logger:        logger,
	}, nil
}

func (this *Partition) Close() {
	this.lock.Lock()
	defer this.lock.Unlock()

	// TODO: close segments?
}

func (this *Partition) rollToNextSegment() error {
	filename := path.Join(this.directory, fmt.Sprint(len(this.segments)+1, ".sd"))
	nextSegment, err := CreateSegment(this.activeSegment.id.Next(), filename, this.config.SegmentSize)

	if err != nil {
		this.logger.WithError(err).Debug("segment creation failed")
		return err
	}

	previous := this.activeSegment

	this.segments = append(this.segments, nextSegment)
	this.activeSegment = nextSegment

	this.logger.Withs(tidy.Fields{
		"new":      tidy.Stringify(this.activeSegment),
		"previous": tidy.Stringify(previous),
		"count":    len(this.segments),
	}).Debug("rolled to new segment")

	return nil
}

// Append writes the messages in the set to the file system. The order is preserved.
func (this *Partition) Append(messages *MessageSet) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	if !this.activeSegment.SpaceLeftFor(messages) {
		this.logger.With("segment", tidy.Stringify(this.activeSegment)).Debug("no space left for message set in active segment")

		this.rollToNextSegment()
	}

	if err := this.activeSegment.Append(messages, this.idSequencer); err != nil {
		return err
	}

	this.index.Append(messages)
	return nil
}

func (this *Partition) ReadFrom(fromId MessageId, eagerFetchUntilMaxBytes int) (*MessageSet, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	if this.closed {
		this.logger.WithError(ErrClosed).Debug("ReadFrom called while closed")
		return nil, ErrClosed
	}

	entry, ok := this.index.FindLastLessOrEqual(fromId)
	if !ok {
		this.logger.With("fromId", fromId).Debug("no index entry found")

		return EmptyMessageSet, nil
	}
	logger.Withs(tidy.Fields{
		"fromId":      fromId,
		"index_entry": entry,
	}).Debug("index entry found")

	buffer := make([]byte, eagerFetchUntilMaxBytes)
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

	if read == 0 {
		return nil, err
	}

	return NewMessageSetFromBuffer(buffer[0:read]), nil
}
