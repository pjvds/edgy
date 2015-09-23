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

type SegmentList struct {
	segments map[SegmentId]*Segment
	last     *Segment
	lock     sync.RWMutex
}

func NewSegmentList() *SegmentList {
	return &SegmentList{
		segments: make(map[SegmentId]*Segment),
	}
}

func (this *SegmentList) Append(id SegmentId, segment *Segment) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.segments[id] = segment
	this.last = segment
}

func (this *SegmentList) Last() (*Segment, bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	last := this.last
	ok := last != nil

	return last, ok
}

func (this *SegmentList) Get(id SegmentId) (*Segment, bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	segment, ok := this.segments[id]
	return segment, ok
}

type PartitionRef struct {
	Topic     string
	Partition PartitionId
}

func (this PartitionRef) ToSegmentRef(segment SegmentId) SegmentRef {
	return SegmentRef{
		PartitionRef: this,
		Segment:      segment,
	}
}

func (this PartitionRef) String() string {
	return fmt.Sprintf("%s/%s", this.Topic, this.Partition)
}

type PartitionId uint32

func (this PartitionId) String() string {
	value := uint32(this)
	return fmt.Sprint(value)
}

type Partition struct {
	ref    PartitionRef
	config PartitionConfig

	lastMessageId MessageId

	directory string

	segments *SegmentList

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
			return os.MkdirAll(directory, 0744)
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

func newPartition(ref PartitionRef, config PartitionConfig, directory string) *Partition {
	return &Partition{
		ref:       ref,
		segments:  NewSegmentList(),
		directory: directory,
		config:    config,
		logger:    tidy.GetLogger().With("partition", ref.String()),
	}
}

func CreatePartition(ref PartitionRef, config PartitionConfig, directory string) (*Partition, error) {
	partition := newPartition(ref, config, directory)

	// we expect the directory to be non-existing or empty.
	if err := createOrEnsureDirectoryIsEmpty(directory); err != nil {
		logger.WithError(err).Withs(tidy.Fields{
			"directory": directory,
		}).Error("initialization failed")

		return nil, err
	}

	return partition, nil
}

func (this *Partition) Close() {
	// TODO: close all segments
}

func (this *Partition) rollToNextSegment() (*Segment, error) {
	id := SegmentId(this.lastMessageId.Next())
	ref := this.ref.ToSegmentRef(id)

	filename := path.Join(this.directory, id.String()+".sd")

	segment, err := CreateSegment(ref, filename, this.config.SegmentSize)

	if err != nil {
		this.logger.With("segment", ref.String()).
			WithError(err).
			Debug("segment creation failed")

		return nil, err
	}

	this.segments.Append(id, segment)
	this.logger.With("segment", ref).Debug("rolled to new segment")

	return segment, nil
}

// Append writes the messages in the set to the file system. The order is preserved.
func (this *Partition) Append(messages *MessageSet) error {
	messages.Align(this.lastMessageId)
	segment, ok := this.segments.Last()

	if !ok {
		if rolledTo, err := this.rollToNextSegment(); err != nil {
			return err
		} else {
			segment = rolledTo
		}

	}

	if !segment.SpaceLeftFor(messages) {
		this.logger.With("segment", tidy.Stringify(segment)).Debug("no space left for message set in active segment")

		if rolledTo, err := this.rollToNextSegment(); err != nil {
			return err
		} else {
			segment = rolledTo
		}
	}

	if err := segment.Append(messages); err != nil {
		return err
	}

	this.lastMessageId = this.lastMessageId.NextN(messages.MessageCount())

	return nil
}

func (this *Partition) ReadFrom(offset Offset, eagerFetchUntilMaxBytes int) (*MessageSet, error) {
	if this.closed {
		this.logger.WithError(ErrClosed).Debug("ReadFrom called while closed")
		return nil, ErrClosed
	}

	this.logger.Withs(tidy.Fields{
		"offset": tidy.Stringify(offset),
	}).Debug("handling ReadFrom")

	// TODO: return empty set when offset is beyond lastMessageId of the writer.

	buffer := make([]byte, eagerFetchUntilMaxBytes)
	// TODO: defer this.buffers.Put(buffer)

	// TODO: offset should already be a int64
	// TODO: handle offset without only message id
	segmentId := offset.SegmentId
	position := offset.LastPosition + 1

	if offset.IsEmpty() {
		segmentId = SegmentId(1)
		position = 0
	}

	for {
		segment, ok := this.segments.Get(segmentId)
		if !ok {
			return nil, errors.New("segment not found")
		}

		read, err := segment.ReadAt(buffer, position)

		if err != nil && err != io.EOF {
			logger.WithError(err).Withs(tidy.Fields{
				"buffer_length": len(buffer),
				"bytes_read":    read,
			}).Warn("failed to read from partition file")

			return nil, err
		}

		if err == io.EOF {
			// we are end of file, try next segment.
			segmentId = SegmentId(offset.MessageId.Next())
			position = 0
			continue
		}

		// we are having something in the buffer
		if buffer[0] != START_VALUE {
			this.logger.Withs(tidy.Fields{
				"segment_id": segmentId,
				"position":   position,
				"offset":     tidy.Stringify(offset),
				"byte_value": buffer[0],
			}).Error("no message found at current position")

			return nil, errors.New("no message found at current position")
		}

		return NewMessageSetFromBuffer(buffer[0:read]), nil
	}
}
