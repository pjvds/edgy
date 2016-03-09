package server

import (
	"path/filepath"
	"time"

	"github.com/pjvds/backoff"
	"github.com/pjvds/edgy/api"
	"github.com/pjvds/edgy/storage"
	"github.com/pjvds/tidy"
	"github.com/rcrowley/go-metrics"
)

type AppendRequest struct {
	*api.AppendRequest
	MessageSet *storage.MessageSet
	Result     chan error
}

type PartitionController struct {
	bytesInRate  metrics.Counter
	bytesOutRate metrics.Counter

	messagesInRate  metrics.Counter
	messagesOutRate metrics.Counter

	ref     storage.PartitionRef
	rootDir string
	logger  tidy.Logger
	storage *storage.Partition

	ready chan struct{}

	appendRequests chan *AppendRequest
}

func NewPartitionController(ref storage.PartitionRef, rootDir string, metricsRegistry metrics.Registry) *PartitionController {
	controller := &PartitionController{
		bytesInRate:  metrics.GetOrRegisterCounter("bytes-in", metricsRegistry),
		bytesOutRate: metrics.GetOrRegisterCounter("bytes-out", metricsRegistry),

		messagesInRate:  metrics.GetOrRegisterCounter("messages-in", metricsRegistry),
		messagesOutRate: metrics.GetOrRegisterCounter("messages-out", metricsRegistry),

		ref:            ref,
		rootDir:        rootDir,
		logger:         tidy.GetLogger(),
		appendRequests: make(chan *AppendRequest),

		ready: make(chan struct{}),
	}
	go controller.initialize()
	go controller.appendLoop()

	// TODO: remove blocking after read isn't a hack
	<-controller.ready

	return controller
}

func (this *PartitionController) initialize() {
	// TODO: support closing
	this.logger.With("partition", this.ref.String()).Info("initializing partition controller")
	directory := filepath.Join(this.rootDir, this.ref.Topic, this.ref.Partition.String())

	delay := backoff.Exp(1*time.Millisecond, 15*time.Second)

	for {
		storage, err := storage.OpenOrCreatePartition(this.ref, storage.DefaultConfig, directory)

		if err != nil {
			this.logger.WithError(err).Withs(tidy.Fields{
				"partition": this.ref.String(),
				"directory": directory,
				"retry_in":  delay.Next(),
			}).Warn("failed to initialize partition storage")

			delay.Delay()
			continue
		}

		this.storage = storage
		close(this.ready)

		break
	}
}

func (this *PartitionController) HandleAppendRequest(request *api.AppendRequest) (*api.AppendReply, error) {

	// TODO: use method that validates the buffer
	messageSet := storage.NewMessageSetFromBuffer(request.Messages)

	this.logger.Withs(tidy.Fields{
		"topic":         request.Topic,
		"partition":     request.Partition,
		"message_count": messageSet.MessageCount(),
	}).Debug("handling append request")

	result := make(chan error, 1)
	this.appendRequests <- &AppendRequest{
		AppendRequest: request,
		MessageSet:    messageSet,
		Result:        result,
	}
	this.logger.Debug("append request send, waiting for result")

	err := <-result

	if err != nil {
		this.logger.WithError(err).Error("append request failed")
	}
	this.logger.WithError(err).Debug("result received")

	this.bytesInRate.Inc(messageSet.DataLen64())
	this.messagesInRate.Inc(int64(messageSet.MessageCount()))

	return &api.AppendReply{
		Ok: err == nil,
	}, nil
}

func (this *PartitionController) HandleReadRequest(request *api.ReadRequest) (*api.ReadReply, error) {
	this.logger.Withs(tidy.Fields{
		"topic":     request.Topic,
		"partition": request.Partition,
		"offset":    tidy.Stringify(request.Offset),
	}).Debug("handling read request")

	result, err := this.storage.ReadFrom(storage.Offset{
		MessageId: storage.MessageId(request.Offset.MessageId),
		SegmentId: storage.SegmentId(request.Offset.SegmentId),
		Position:  request.Offset.EndPosition,
	}, 2*1e6)
	if err != nil {
		return nil, err
	}

	this.bytesOutRate.Inc(int64(len(result.Messages)))
	this.messagesOutRate.Inc(int64(result.MessageCount))

	return &api.ReadReply{
		Messages: result.Messages,
		Offset: &api.OffsetData{
			MessageId:   uint64(result.Next.MessageId),
			EndPosition: result.Next.Position,
			SegmentId:   uint64(result.Next.SegmentId),
		},
	}, nil
}

func (this *PartitionController) appendLoop() {
	<-this.ready
	this.logger.Debug("append loop started")

	outstandingRequests := make([]*AppendRequest, 0)
	syncTicker := time.NewTicker(50 * time.Millisecond)

	for {
		select {
		case request := <-this.appendRequests:
			if err := this.storage.Append(request.MessageSet); err != nil {
				request.Result <- err
				continue
			}
			request.Result <- nil

			outstandingRequests = append(outstandingRequests, request)
		case <-syncTicker.C:
			// TODO: re-check data on failure
			_, err := this.storage.Sync()
			for _, request := range outstandingRequests {
				request.Result <- err
			}

			outstandingRequests = outstandingRequests[0:0]
		}
	}
}
