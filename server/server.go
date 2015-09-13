package server

import (
	"net"
	"sync"

	"golang.org/x/net/context"

	"github.com/pjvds/edgy/api"
	"github.com/pjvds/edgy/storage"
	"github.com/pjvds/tidy"

	"google.golang.org/grpc"
)

type Server struct {
	logger tidy.Logger
}

type Controller struct {
	partitions     map[storage.PartitionId]*storage.Partition
	partitionsLock sync.RWMutex

	dataDirectory string
}

func ListenAndServe(address string) error {
	logger := tidy.GetLogger()
	server := &Server{
		logger: logger,
	}

	logger.With("address", address).Debug("creating listeners")
	listener, err := net.Listen("tcp", address)

	if err != nil {
		logger.With("address", address).WithError(err).Warn("listening failed")
		return err
	}

	return server.Serve(listener)
}

func (this *Server) Serve(listener net.Listener) error {
	grpcServer := grpc.NewServer()
	api.RegisterEdgyServer(grpcServer, &Controller{})

	return grpcServer.Serve(listener)
}

func (this *Controller) Append(ctx context.Context, request *api.AppendRequest) (*api.AppendReply, error) {
	id := storage.PartitionId{
		Topic:     request.Topic,
		Partition: request.Partition,
	}

	partition, err := this.getPartition(id)

	if err != nil {
		return nil, err
	}

	messageSet := storage.NewMessageSetFromBuffer(request.Messages)

	if err := partition.Append(messageSet); err != nil {
		return nil, err
	}

	return nil, nil
}

func (this *Controller) getPartition(id storage.PartitionId) (*storage.Partition, error) {
	this.partitionsLock.RLock()
	defer this.partitionsLock.RUnlock()

	partition, ok := this.partitions[id]

	if !ok {
		this.partitionsLock.Lock()
		defer this.partitionsLock.Unlock()

		if newPartition, err := storage.InitializePartition(id, storage.DefaultConfig, this.dataDirectory); err != nil {
			return nil, err
		} else {
			partition = newPartition
		}
	}

	return partition, nil
}
