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

type Segment struct {
	lock     *sync.RWMutex
	file     *os.File
	position int64
	size     int64
}

func CreateSegment(filename string, size int64) (*Segment, error) {
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

	return createSegment(file, size)
}

func createSegment(file *os.File, size int64) (*Segment, error) {
	return &Segment{
		lock:     new(sync.RWMutex),
		file:     file,
		position: 0,
		size:     size,
	}, nil
}

func (this *Segment) Append(messages *MessageSet, sequencer *MessageIdSequencer) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	if this.size-this.position < int64(len(messages.buffer)) {
		return ErrSegmentFull
	}

	// align messages in set with our sequencer.
	messages.Align(sequencer)

	written, err := this.file.WriteAt(messages.buffer, int64(this.position))

	if err != nil {
		if logger.IsError() {
			logger.WithError(err).Withs(tidy.Fields{
				"file":     this.file.Name(),
				"position": this.position,
				"written":  written,
			}).Error("write error")
		}

		return err
	}

	if written != len(messages.buffer) {
		err := fmt.Errorf("unexpected write count")

		if logger.IsError() {
			logger.WithError(err).Withs(tidy.Fields{
				"file":     this.file.Name(),
				"position": this.position,
				"written":  written,
				"expected": len(messages.buffer),
				"diff":     len(messages.buffer) - written,
			}).Error("unexpected write count")
		}

		return err
	}

	// advance position
	this.position += int64(written)

	return nil
}

func (this *Segment) ReadAt(buffer []byte, offset int64) (int, error) {
	available := this.position - offset

	if available > int64(len(buffer)) {
		return this.file.ReadAt(buffer, offset)
	}

	if available < 0 {
		return 0, io.EOF
	}

	return this.file.ReadAt(buffer[0:available], offset)
}
