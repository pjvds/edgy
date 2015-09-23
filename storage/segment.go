package storage

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/OneOfOne/xxhash"
	"github.com/pjvds/tidy"
)

var (
	ErrSegmentFull = errors.New("segment full")
)

type SegmentRef struct {
	Topic     string
	Partition PartitionId
	Segment   SegmentId
}

func (this SegmentRef) String() string {
	return fmt.Sprintf("%s/%s/%s", this.Topic, this.Partition.String(), this.Segment.String())
}

type SegmentId uint64

func (this SegmentId) String() string {
	value := uint64(this)
	return fmt.Sprint(value)
}

type TopicPartition struct {
	Topic     string
	Partition PartitionId
}

type Segment struct {
	ref      SegmentRef
	filename string
	lock     *sync.RWMutex
	file     *os.File
	position int64
	size     int64
	logger   tidy.Logger
}

type checkResult struct {
	IsEmpty                  bool
	LastMessageId            MessageId
	LastMessagePosition      int64
	LastMessageContentLength int32
}

func checkSegment(r io.Reader) (checkResult, error) {
	reader := bufio.NewReader(r)
	hasher := xxhash.New64()

	headerBuf := make([]byte, HEADER_LENGTH)
	//copyBuf := make([]byte, 5*1000*1000)

	position := int64(0)

	result := checkResult{
		IsEmpty: true,
	}

	for {
		if read, err := reader.Read(headerBuf); err != nil {
			if err == io.EOF {
				logger.Withs(tidy.Fields{
					"read":        read,
					"header_size": len(headerBuf),
				}).WithError(err).Debug("EOF while reading header")
				break
			}
			return result, err
		}

		header := ReadHeader(headerBuf)

		if header.Magic != START_VALUE {
			logger.Withs(tidy.Fields{
				"start_value": START_VALUE,
				"magic":       header.Magic,
			}).Debug("magic is not a START_VALUE")
			break
		}

		contentBuf := make([]byte, header.ContentLength)
		read, err := reader.Read(contentBuf)

		logger.With("header", tidy.Stringify(header)).With("content", string(contentBuf)).Error("FOOBAR")

		//hasher.Reset()
		//contentReader := io.LimitReader(reader, int64(header.ContentLength))
		//read, err := io.CopyBuffer(hasher, contentReader, copyBuf)

		if err != nil {
			if err == io.EOF {
				logger.Withs(tidy.Fields{
					"content_lenght": header.ContentLength,
					"expected_hash":  header.ContentHash,
				}).WithError(err).Debug("EOF while hashing content")
				break
			}
			return result, err
		}

		if read != int(header.ContentLength) {
			logger.Withs(tidy.Fields{
				"content_lenght": header.ContentLength,
				"read":           read,
			}).WithError(err).Debug("read mismatch while hashing content")
			break
		}

		if xxhash.Checksum64(contentBuf) != header.ContentHash {
			logger.Withs(tidy.Fields{
				"actual_hash":   hasher.Sum64(),
				"expected_hash": header.ContentHash,
			}).Debug("content hash doesn't match header")
			break
		}

		position += int64(HEADER_LENGTH + header.ContentLength)

		result.IsEmpty = false
		result.LastMessageId = header.MessageId
		result.LastMessageContentLength = header.ContentLength
		result.LastMessagePosition = position
	}

	return result, nil
}

func CreateSegment(ref SegmentRef, filename string, size int64) (*Segment, error) {
	logger := tidy.GetLogger().Withs(tidy.Fields{
		"segment": ref.String(),
	})

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
		ref:      ref,
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
	required := messages.DataLen64()
	ok := spaceLeft >= required

	if !ok && this.logger.IsDebug() {
		this.logger.Withs(tidy.Fields{
			"file":       this.filename,
			"space_left": spaceLeft,
			"required":   required,
			"shortage":   required - spaceLeft,
		}).Debug("no space left for message set")
	}

	return ok
}

func (this *Segment) Sync() error {
	this.lock.Lock()
	defer this.lock.Unlock()

	return this.file.Sync()
}

func (this *Segment) Close() error {
	this.lock.Lock()
	defer this.lock.Unlock()

	return this.file.Close()
}

func (this *Segment) Append(messages *MessageSet) error {
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
		this.position += messages.DataLen64()

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
