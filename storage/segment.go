package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/pjvds/tidy"
)

var (
	ErrSegmentFull = errors.New("segment full")
)

type SegmentId struct {
	Topic     string
	Partition int32
	Segment   int64
}

func (this SegmentId) Next() SegmentId {
	return SegmentId{
		Topic:     this.Topic,
		Partition: this.Partition,
		Segment:   this.Segment + 1,
	}
}

func (this SegmentId) String() string {
	return fmt.Sprintf("%v/%v/%v", this.Topic, this.Partition, this.Segment)
}

type Segment struct {
	id       SegmentId
	filename string
	lock     *sync.RWMutex
	file     *os.File
	position int64
	size     int64
	logger   tidy.Logger
}

func CreateSegment(id SegmentId, filename string, size int64) (*Segment, error) {
	logger := tidy.GetLogger().With("segment", id.String())

	file, err := os.Create(filename)
	if err != nil {
		logger.WithError(err).Withs(tidy.Fields{
			"filename": filename,
			"size":     size,
		}).Debug("segment file creation failed")

		return nil, err
	}

	if err := file.Truncate(size); err != nil {
		logger.WithError(err).Withs(tidy.Fields{
			"filename": filename,
			"size":     size,
		}).Debug("segment file truncation failed")
		file.Close()
		return nil, err
	}

	if logger.IsDebug() {
		logger.Withs(tidy.Fields{
			"filename": filename,
			"size":     size,
		}).Debug("segment created")
	}

	return &Segment{
		filename: file.Name(),
		lock:     new(sync.RWMutex),
		file:     file,
		position: 0,
		size:     size,
	}, nil
}

// SpaceLeftFor returns true when the message set can be appended
// to this segment; otherwise, false.
func (this *Segment) SpaceLeftFor(messages *MessageSet) bool {

	spaceLeft := this.size - this.position
	ok := spaceLeft >= messages.Len64()

	if !ok && logger.IsDebug() {
		this.logger.Withs(tidy.Fields{
			"file":       this.filename,
			"space_left": spaceLeft,
			"required":   messages.Len64(),
			"difference": spaceLeft - messages.Len64(),
		}).Debug("no space left for message set")
	}

	return ok
}

func (this *Segment) Append(messages *MessageSet, sequencer *MessageIdSequencer) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	spaceLeft := this.size - this.position
	if spaceLeft < int64(len(messages.buffer)) {
		this.logger.Withs(tidy.Fields{
			"filename":   this.filename,
			"position":   this.position,
			"size":       this.size,
			"write_size": len(messages.buffer),
			"space_left": spaceLeft,
		}).Debug("segment full")
		return ErrSegmentFull
	}

	// align messages in set with our sequencer.
	messages.Align(sequencer)

	// WriteAt tries to write all bytes, no need to check the written bytes count.
	// Because an error is returned when this is not equal to the number of bytes we
	// provided.
	if written, err := this.file.WriteAt(messages.buffer, int64(this.position)); err != nil {
		if this.logger.IsError() {
			this.logger.WithError(err).Withs(tidy.Fields{
				"file":       this.file.Name(),
				"position":   this.position,
				"written":    written,
				"space_left": spaceLeft,
			}).Error("write error")
		}

		return err
	} else {
		// the write succeeded, advance position
		this.position += int64(messages.Len())

		return nil
	}
}

func (this *Segment) ReadAt(buffer []byte, offset int64) (int, error) {
	available := this.position - offset

	if available > int64(len(buffer)) {
		return this.file.ReadAt(buffer, offset)
	}

	if available <= 0 {
		return 0, io.EOF
	}

	return this.file.ReadAt(buffer[0:available], offset)
}
