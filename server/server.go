package server

import (
	"net"

	"github.com/pjvds/edgy/api"
	"github.com/pjvds/tidy"
	"github.com/rcrowley/go-metrics"

	"google.golang.org/grpc"
)

type Server struct {
	registry   metrics.Registry
	logger     tidy.Logger
	controller *Controller
}

func ListenAndServe(address string, directory string, registry metrics.Registry) error {
	logger := tidy.GetLogger()
	server := &Server{
		registry:   registry,
		logger:     logger,
		controller: NewController(directory, registry),
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
	api.RegisterEdgyServer(grpcServer, this.controller)

	return grpcServer.Serve(listener)
}
