package server

import (
	"errors"
	"sync"

	"github.com/pjvds/edgy/api"
	"github.com/pjvds/edgy/storage"
	"github.com/pjvds/tidy"
	"golang.org/x/net/context"
)

type Controller struct {
	logger tidy.Logger

	partitions     map[storage.PartitionRef]*PartitionController
	partitionsLock sync.RWMutex

	directory string
}

func NewController(directory string) *Controller {
	return &Controller{
		logger:     tidy.GetLogger(),
		partitions: make(map[storage.PartitionRef]*PartitionController),
		directory:  directory,
	}
}

func (this *Controller) Append(ctx context.Context, request *api.AppendRequest) (*api.AppendReply, error) {
	ref := storage.PartitionRef{
		Topic:     request.Topic,
		Partition: storage.PartitionId(request.Partition),
	}

	partition, err := this.getPartition(ref)
	if err != nil {
		this.logger.With("partition", ref.String()).WithError(err).Error("failed to get or create storage for partition")
		return nil, err
	}

	return partition.HandleAppendRequest(request)
}

func (this *Controller) Read(ctx context.Context, request *api.ReadRequest) (*api.ReadReply, error) {
	return nil, errors.New("not implemented")
}

func (this *Controller) Ping(ctx context.Context, request *api.PingRequest) (*api.PingReply, error) {
	return &api.PingReply{}, nil
}

func (this *Controller) getPartition(ref storage.PartitionRef) (*PartitionController, error) {
	this.partitionsLock.RLock()
	partition, ok := this.partitions[ref]
	this.partitionsLock.RUnlock()

	if !ok {
		this.partitionsLock.Lock()
		defer this.partitionsLock.Unlock()

		// check again, since there another one can have
		// created our partition in the time we where locked
		if partition, ok = this.partitions[ref]; ok {
			return partition, nil
		}

		partition = NewPartitionController(ref, this.directory)
		this.partitions[ref] = partition
	}

	return partition, nil
}
