package server

import (
	"io"
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
	this.logger.Withs(tidy.Fields{
		"topic":     request.Topic,
		"partition": request.Partition,
	}).Debug("incoming append request")

	ref := storage.PartitionRef{
		Topic:     request.Topic,
		Partition: storage.PartitionId(request.Partition),
	}

	partition, err := this.getPartition(ref)
	if err != nil {
		this.logger.With("partition", ref.String()).WithError(err).Error("failed to get or create storage for partition")
		return nil, err
	}

	this.logger.With("partition", ref).Debug("dispatching request")

	return partition.HandleAppendRequest(request)
}

func (this *Controller) Read(request *api.ReadRequest, stream api.Edgy_ReadServer) error {
	this.logger.Withs(tidy.Fields{
		"topic":     request.Topic,
		"partition": request.Partition,
		"offset":    tidy.Stringify(request.Offset),
	}).Debug("incoming read request")

	ref := storage.PartitionRef{
		Topic:     request.Topic,
		Partition: storage.PartitionId(request.Partition),
	}

	partition, err := this.getPartition(ref)
	if err != nil {
		this.logger.With("partition", ref.String()).WithError(err).Error("failed to get or create storage for partition")
		return err
	}

	this.logger.With("partition", ref).Debug("dispatching request")

	for {
		reply, err := partition.HandleReadRequest(request)

		if err != nil {
			if err == io.EOF {
				this.logger.With("reply", reply).WithError(err).Debug("read finished")
			} else {
				this.logger.Withs(tidy.Fields{
					"topic":     request.Topic,
					"partition": request.Partition,
					"offset":    tidy.Stringify(request.Offset),
				}).WithError(err).Error("read failed")
			}
			return err
		}

		if len(reply.Messages) == 0 {
			this.logger.With("partition", ref).WithError(io.EOF).Debug("read finished")
			return io.EOF
		}

		if err := stream.Send(reply); err != nil {
			return err
		}

		request.Offset = &api.OffsetData{
			MessageId:   reply.Offset.MessageId,
			SegmentId:   reply.Offset.SegmentId,
			EndPosition: reply.Offset.EndPosition,
		}
	}
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

		// check again, since it might be that another one routine
		// created our partition in the time we where locked
		if partition, ok = this.partitions[ref]; ok {
			return partition, nil
		}

		partition = NewPartitionController(ref, this.directory)
		this.partitions[ref] = partition
	}

	return partition, nil
}
