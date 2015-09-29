package server

import (
	"path/filepath"
	"time"

	"github.com/pjvds/backoff"
	"github.com/pjvds/edgy/api"
	"github.com/pjvds/edgy/storage"
	"github.com/pjvds/tidy"
)

type AppendRequest struct {
	*api.AppendRequest
	MessageSet *storage.MessageSet
	Result     chan error
}

type PartitionController struct {
	ref     storage.PartitionRef
	rootDir string
	logger  tidy.Logger
	storage *storage.Partition

	ready chan struct{}

	appendRequests chan *AppendRequest
}

func NewPartitionController(ref storage.PartitionRef, rootDir string) *PartitionController {
	controller := &PartitionController{
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

	this.logger.WithError(err).Debug("result received")
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
		MessageId:    storage.MessageId(request.Offset.MessageId),
		SegmentId:    storage.SegmentId(request.Offset.SegmentId),
		LastPosition: request.Offset.EndPosition,
	}, 5*1e6)
	if err != nil {
		return nil, err
	}

	return &api.ReadReply{
		Messages: result.Messages.Buffer(),
		Offset: &api.OffsetData{
			MessageId:   uint64(result.Next.MessageId),
			EndPosition: result.Next.LastPosition,
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

			outstandingRequests = append(outstandingRequests, request)
		case <-syncTicker.C:
			// TODO: re-check data on failure
			err := this.storage.Sync()
			for _, request := range outstandingRequests {
				request.Result <- err
			}

			outstandingRequests = outstandingRequests[0:0]
		}
	}
}
