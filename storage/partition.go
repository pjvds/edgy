package storage

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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

func (this *SegmentList) Last() *Segment {
	this.lock.RLock()
	defer this.lock.RUnlock()

	return this.last
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
		Topic:     this.Topic,
		Partition: this.Partition,
		Segment:   segment,
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
	SegmentSize: 1000 * 1000 * 1000,
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

func OpenPartition(ref PartitionRef, config PartitionConfig, directory string) (*Partition, error) {
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	partition := newPartition(ref, config, directory)

	segmentFiles := make([]string, 0, len(files))

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".sd" {
			segmentFiles = append(segmentFiles, filepath.Join(directory, file.Name()))
		}
	}

	if len(segmentFiles) == 0 {
		return nil, errors.New("no segment data files")
	}

	sort.Strings(segmentFiles)

	lastIndex := len(segmentFiles) - 1
	for index, filename := range segmentFiles {
		segmentId := ReadSegmentIdFromFilename(filename)

		file, err := os.OpenFile(filename, os.O_RDWR, 0777)
		if err != nil {
			return nil, err
		}
		stat, err := file.Stat()
		if err != nil {
			return nil, err
		}

		if index < lastIndex {
			segment := &Segment{
				ref:      ref.ToSegmentRef(segmentId),
				file:     file,
				filename: filename,
				size:     stat.Size(),
			}
			partition.segments.Append(segmentId, segment)
		} else {
			check, err := checkSegment(file)
			if err != nil {
				return nil, err
			}

			if check.IsEmpty {
				return nil, errors.New("last segment is empty")
			}

			segmentId := ReadSegmentIdFromFilename(file.Name())
			segment := &Segment{
				ref:      ref.ToSegmentRef(segmentId),
				file:     file,
				filename: file.Name(),
				position: check.LastMessagePosition + int64(HEADER_LENGTH+check.LastMessageContentLength),
				size:     stat.Size(),
			}

			partition.segments.Append(segmentId, segment)
			partition.lastMessageId = check.LastMessageId
		}
	}

	logger.Withs(tidy.Fields{
		"partition":           ref.String(),
		"directory":           directory,
		"lastMessageId":       partition.lastMessageId,
		"lastSegmentId":       partition.segments.Last().ref.Segment,
		"lastMessagePosition": partition.segments.Last().position,
	}).Debug("opened partition")

	return partition, nil
}

func ReadSegmentIdFromFilename(filename string) SegmentId {
	base := filepath.Base(filename)
	name := strings.TrimSuffix(base, filepath.Ext(filename))
	n, err := strconv.ParseInt(name, 10, 64)

	if err != nil {
		panic(err)
	}

	return SegmentId(n)
}

func OpenOrCreatePartition(ref PartitionRef, config PartitionConfig, directory string) (*Partition, error) {
	files, err := ioutil.ReadDir(directory)

	if err != nil {
		if os.IsNotExist(err) {
			return CreatePartition(ref, config, directory)
		}
		return nil, err
	}

	if len(files) == 0 {
		return CreatePartition(ref, config, directory)
	}

	return OpenPartition(ref, config, directory)
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
	// TODO: sync
	for _, segment := range this.segments.segments {
		if err := segment.Close(); err != nil {
			panic(err)
		}
	}
}

func (this *Partition) rollToNextSegment() (*Segment, error) {
	if current := this.segments.Last(); current != nil {
		// TODO: make finalize safer
		if err := current.Finalize(); err != nil {
			panic(err)
		}
	}

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

func (this *Partition) Sync() error {
	if segment := this.segments.Last(); segment != nil {
		return segment.Sync()
	}
	return nil
}

// Append writes the messages in the set to the file system. The order is preserved.
func (this *Partition) Append(messages *MessageSet) error {
	messages.Align(this.lastMessageId)
	segment := this.segments.Last()

	if segment == nil {
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

	this.logger.With("segment", segment.ref).Debug("writting to segment")

	if err := segment.Append(messages); err != nil {
		this.logger.With("segment", segment.ref).WithError(err).Warn("write to segment failed")

		return err
	}
	this.logger.With("segment", segment.ref).Debug("write to segment success")

	this.lastMessageId = this.lastMessageId.NextN(messages.MessageCount())
	this.logger.With("last_message_id", this.lastMessageId).Info("append success")

	return nil
}

type ReadResult struct {
	Messages []byte
	Next     Offset
}

func (this *Partition) ReadFrom(offset Offset, eagerFetchUntilMaxBytes int) (ReadResult, error) {

	next := offset
	if next.IsEmpty() {
		next.MessageId = MessageId(1)
		next.SegmentId = SegmentId(1)
		next.Position = 0
	}
	this.logger.With("offset", offset).With("next", next).Info("read request")

	if this.closed {
		this.logger.WithError(ErrClosed).Debug("ReadFrom called while closed")
		return ReadResult{Next: next}, ErrClosed
	}

	this.logger.Withs(tidy.Fields{
		"offset": tidy.Stringify(offset),
	}).Debug("handling ReadFrom")

	// TODO: return empty set when offset is beyond lastMessageId of the writer.

	buffer := make([]byte, eagerFetchUntilMaxBytes)

	for {
		segment, ok := this.segments.Get(next.SegmentId)
		if !ok {
			err := errors.New("segment not found")

			this.logger.Withs(tidy.Fields{
				"offset": offset,
			}).WithError(err).Error("ReadFrom failed")

			return ReadResult{}, err
		}

		read, err := segment.ReadAt(buffer, next.Position)

		if err != nil && err != io.EOF {
			logger.WithError(err).Withs(tidy.Fields{
				"segment":       segment.ref,
				"buffer_length": len(buffer),
				"bytes_read":    read,
			}).Error("failed to read from segment file")

			return ReadResult{}, err
		}

		if err == io.EOF && read == 0 {
			logger.WithError(err).Withs(tidy.Fields{
				"segment":       segment.ref,
				"buffer_length": len(buffer),
				"bytes_read":    read,
				"position":      next.Position,
			}).Error("unexpected EOF")

			return ReadResult{}, err
		}

		if buffer[0] == END_OF_SEGMENT {
			next = Offset{
				MessageId: offset.MessageId,
				SegmentId: SegmentId(offset.MessageId),
				Position:  0,
			}
			continue
		}

		if buffer[0] != START_VALUE {
			if buffer[0] == 0x00 {
				return ReadResult{
					Messages: make([]byte, 0),
					Next:     next,
				}, nil
			}

			err := errors.New("unexpected start value")

			logger.WithError(err).Withs(tidy.Fields{
				"segment":    segment.ref,
				"offset":     offset,
				"first_byte": buffer[0],
			}).Error("read failure")

			return ReadResult{}, err
		}

		// we are having something in the buffer
		// if buffer[0] != START_VALUE {
		// 	this.logger.Withs(tidy.Fields{
		// 		"segment_id": segmentId,
		// 		"position":   position,
		// 		"offset":     tidy.Stringify(offset),
		// 		"byte_value": buffer[0],
		// 	}).Error("no message found at current position")
		//
		// 	return result, errors.New("no message found at current position")
		// }

		position := 0

		for {
			if position > read-HEADER_LENGTH {
				break
			}

			header := ReadHeader(buffer[position:])

			if header.Magic != START_VALUE {
				// we reached a point where there is no message header
				// if header.Magic == END_OF_SEGMENT {
				// 	result.Next.SegmentId = SegmentId(result.Next.MessageId)
				// 	result.Next.Position = 0
				// }
				break
			}

			if header.MessageId != next.MessageId {
				this.logger.Withs(tidy.Fields{
					"offset":              offset,
					"next":                next,
					"expected_message_id": offset.MessageId,
					"actual_message_id":   header.MessageId,
					"position":            position,
					"segment":             segment.ref,
				}).Error("unexpected message id")

				return ReadResult{}, errors.New("data error")
			}

			if header.ContentLength > int32(read-position-HEADER_LENGTH) {
				// message content is not, or partial in buffer
				break
			}

			next.MessageId = header.MessageId.Next()
			next.Position += int64(HEADER_LENGTH + header.ContentLength)

			position += int(HEADER_LENGTH + header.ContentLength)
		}

		this.logger.Withs(tidy.Fields{
			"offset": offset,
			"next":   next,
		}).Info("read success")

		return ReadResult{
			Messages: buffer[0:position],
			Next:     next,
		}, nil
	}
}
