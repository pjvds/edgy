package server

import (
	"errors"
	"path/filepath"
	"sync"

	"github.com/pjvds/edgy/api"
	"github.com/pjvds/edgy/storage"
	"github.com/pjvds/tidy"
	"golang.org/x/net/context"
)

type Controller struct {
	logger tidy.Logger

	partitions     map[storage.PartitionId]*storage.Partition
	partitionsLock sync.RWMutex

	directory string
}

func (this *Controller) Append(ctx context.Context, request *api.AppendRequest) (*api.AppendReply, error) {
	ref := storage.PartitionRef{
		Topic:     request.Topic,
		Partition: storage.PartitionId(request.Partition),
	}
	this.logger.With("partition", ref.String()).Debug("handling incoming append request")

	partition, err := this.getPartition(ref)
	if err != nil {
		this.logger.With("partition", ref.String()).WithError(err).Error("failed to get or create storage for partition")
		return nil, err
	}

	messageSet := storage.NewMessageSetFromBuffer(request.Messages)
	this.logger.With("partition", ref.String()).Debug("appending to partition")

	if err := partition.Append(messageSet); err != nil {
		this.logger.With("partition", ref.String()).WithError(err).Error("append failed")
		return nil, err
	}

	this.logger.With("partition", ref.String()).Debug("append success")

	return &api.AppendReply{
		Ok: true,
	}, nil
}

func (this *Controller) Read(ctx context.Context, request *api.ReadRequest) (*api.ReadReply, error) {
	return nil, errors.New("not implemented")
}

func (this *Controller) Ping(ctx context.Context, request *api.PingRequest) (*api.PingReply, error) {
	return &api.PingReply{}, nil
}

func (this *Controller) getPartition(ref storage.PartitionRef) (*storage.Partition, error) {
	this.partitionsLock.RLock()
	partition, ok := this.partitions[ref.Partition]
	this.partitionsLock.RUnlock()

	if !ok {
		this.partitionsLock.Lock()
		defer this.partitionsLock.Unlock()

		partitionDir := filepath.Join(this.directory, ref.Topic, ref.Partition.String())
		if newPartition, err := storage.InitializePartition(ref, storage.DefaultConfig, partitionDir); err != nil {
			return nil, err
		} else {
			partition = newPartition
		}
	}

	return partition, nil
}
