package server

import (
	"errors"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/pjvds/backoff"
	"github.com/pjvds/edgy/api"
	"github.com/pjvds/edgy/storage"
	"github.com/pjvds/tidy"
	"github.com/rcrowley/go-metrics"
	"golang.org/x/net/context"
)

type RequestContext struct {
	Partition *PartitionController

	Request interface{}
	done    chan struct {
		Response interface{}
		Error    error
	}
}

func (this RequestContext) Wait() (response interface{}, err error) {
	result := <-this.done
	return result.Response, result.Error
}

func NewRequestContext(partition *PartitionController, request interface{}) RequestContext {
	return RequestContext{
		Partition: partition,
		Request:   request,
		done: make(chan struct {
			Response interface{}
			Error    error
		}, 1),
	}
}

type Controller struct {
	logger           tidy.Logger
	metricsRegistery metrics.Registry

	partitions     map[storage.PartitionRef]*PartitionController
	partitionsLock sync.RWMutex

	requests chan RequestContext

	directory string
}

func NewController(directory string, metricsRegistery metrics.Registry) *Controller {
	controller := &Controller{
		requests:         make(chan RequestContext),
		metricsRegistery: metricsRegistery,
		logger:           tidy.GetLogger(),
		partitions:       make(map[storage.PartitionRef]*PartitionController),
		directory:        directory,
	}
	controller.start(runtime.NumCPU())
	return controller
}

func (this *Controller) start(workerCount int) {
	requestRate := metrics.GetOrRegisterMeter("requests", this.metricsRegistery)
	requestLatency := metrics.GetOrRegisterTimer("request-latency", this.metricsRegistery)

	for i := 0; i < workerCount; i++ {
		go func() {
			// TODO: isn't this a great place to reuse buffers?
			for context := range this.requests {
				startedAt := time.Now()

				switch request := context.Request.(type) {
				case *api.AppendRequest:
					reply, err := context.Partition.HandleAppendRequest(request)
					context.done <- struct {
						Response interface{}
						Error    error
					}{
						reply, err,
					}

				case *api.ReadRequest:
					reply, err := context.Partition.HandleReadRequest(request)
					context.done <- struct {
						Response interface{}
						Error    error
					}{
						reply, err,
					}
				}

				requestRate.Mark(1)
				requestLatency.UpdateSince(startedAt)
			}
		}()
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

	context := NewRequestContext(partition, request)
	this.requests <- context

	reply, err := context.Wait()
	if err != nil {
		return nil, err
	} else {
		return reply.(*api.AppendReply), nil
	}
}

func (this *Controller) executeRead(context context.Context, partition *PartitionController, request *api.ReadRequest, stream api.Edgy_ReadServer) (*api.OffsetData, error) {
	requestContext := NewRequestContext(partition, request)
	select {
	case this.requests <- requestContext:
	case <-context.Done():
		err := context.Err()
		this.logger.WithError(err).Warn("unexpected context done signal")
		return nil, err
	}

	untypedReply, err := requestContext.Wait()
	if err != nil {
		return nil, err
	}

	reply := untypedReply.(*api.ReadReply)

	if len(reply.Messages) == 0 {
		// We are at the end of the stream
		return nil, io.EOF
	}

	if err := stream.Send(reply); err != nil {
		return nil, err
	}

	return reply.Offset, nil
}

func (this *Controller) Read(request *api.ReadRequest, stream api.Edgy_ReadServer) error {
	if len(request.Topic) == 0 {
		return errors.New("missing topic")
	}
	if request.Offset == nil {
		return errors.New("missing offset")
	}

	this.logger.Withs(tidy.Fields{
		"topic":      request.Topic,
		"partition":  request.Partition,
		"offset":     tidy.Stringify(request.Offset),
		"continuous": request.Continuous,
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

	if !request.Continuous {
		_, err := this.executeRead(stream.Context(), partition, request, stream)
		return err
	}

	receiver := partition.Notify(stream.Context())
	errDelay := backoff.Exp(1*time.Millisecond, 5*time.Second)

	for {
		offset, err := this.executeRead(stream.Context(), partition, request, stream)
		if err != nil {
			if err != io.EOF {
				select {
				case <-errDelay.DelayC():
					continue
				case <-stream.Context().Done():
					err := stream.Context().Err()
					this.logger.WithError(err).Warn("unexpected context done signal")
					return err
				}
			}
		} else {
			request.Offset = offset
		}
		errDelay.Reset()

		select {
		case signalOffset, ok := <-receiver.channel:
			this.logger.With("signal_offset", signalOffset).With("ok", ok).Debug("receiver.channel")
			if !ok {
				this.logger.Debug("signal channel closed")
				return nil
			}
			this.logger.With("offset", signalOffset)
			continue
		case <-stream.Context().Done():
			err := stream.Context().Err()
			this.logger.WithError(err).Debug("context done")
			return err
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

		partition = NewPartitionController(ref, this.directory, this.metricsRegistery)
		this.partitions[ref] = partition
	}

	return partition, nil
}
