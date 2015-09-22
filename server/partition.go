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
	Result chan error
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

	return controller
}

func (this *PartitionController) initialize() {
	// TODO: support closing
	this.logger.With("partition", this.ref.String()).Debug("initializing partition controller")
	directory := filepath.Join(this.rootDir, this.ref.Topic, this.ref.Partition.String())

	delay := backoff.Exp(1*time.Millisecond, 15*time.Second)

	for {
		storage, err := storage.InitializePartition(this.ref, storage.DefaultConfig, directory)

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
	result := make(chan error, 1)
	this.appendRequests <- &AppendRequest{
		AppendRequest: request,
		Result:        result,
	}

	err := <-result
	return &api.AppendReply{
		Ok: err == nil,
	}, nil
}

func (this *PartitionController) appendLoop() {
	<-this.ready
	this.logger.Debug("append loop started")

	for request := range this.appendRequests {
		messageSet := storage.NewMessageSetFromBuffer(request.Messages)
		request.Result <- this.storage.Append(messageSet)
	}
}
