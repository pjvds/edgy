package client

import (
	"bytes"
	"errors"
	"time"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	"github.com/pjvds/edgy/api"
	"github.com/pjvds/edgy/storage"
	"github.com/pjvds/tidy"
)

type ProducerConfig struct {
	QueueTime time.Duration
	QueueSize int
}

type Producer struct {
	logger   tidy.Logger
	client   api.EdgyClient
	requests chan appendRequest

	config ProducerConfig
}

func NewProducer(address string) (*Producer, error) {
	connection, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithTimeout(time.Second))
	if err != nil {
		return nil, err
	}

	client := api.NewEdgyClient(connection)

	if _, err := client.Ping(context.Background(), &api.PingRequest{}); err != nil {
		connection.Close()
		return nil, err
	}

	producer := &Producer{
		logger:   tidy.GetLogger(),
		client:   api.NewEdgyClient(connection),
		requests: make(chan appendRequest),
		config: ProducerConfig{
			QueueTime: 250 * time.Millisecond,
			QueueSize: 1000,
		},
	}
	go producer.run()

	return producer, nil
}

type AppendResult struct {
	result <-chan error
}

func (this AppendResult) Wait() error {
	return <-this.result
}

type appendRequest struct {
	topic     string
	partition int32

	message []byte

	result chan error
}

func (this *Producer) Append(topic string, partition int32, message []byte) AppendResult {
	this.logger.Withs(tidy.Fields{
		"topic":     topic,
		"partition": partition,
	}).Debug("handling append request")

	result := make(chan error, 1)
	this.requests <- appendRequest{
		topic:     topic,
		partition: partition,
		message:   message,
		result:    result,
	}

	return AppendResult{
		result: result,
	}
}

func (this *Producer) run() {
	var topic string
	var partition int32

	callbacks := make([]chan error, 0, this.config.QueueSize)
	buffer := new(bytes.Buffer)

	for {
		// receive first request
		request := <-this.requests
		topic = request.topic
		partition = request.partition

		this.logger.Withs(tidy.Fields{
			"topic":     topic,
			"partition": partition,
		}).Debug("received first request")

		callbacks = append(callbacks, request.result)
		buffer.Write(storage.NewMessage(0, request.message))

		// receive next requests until full or timed out
		timeout := time.After(this.config.QueueTime)

	enqueue:
		for index := 1; index < this.config.QueueSize; index++ {
			select {
			case request = <-this.requests:
				callbacks = append(callbacks, request.result)
				buffer.Write(storage.NewMessage(0, request.message))

			case <-timeout:
				this.logger.With("timeout", this.config.QueueTime).Debug("queue time expired")
				break enqueue
			}
		}

		this.logger.Withs(tidy.Fields{
			"queue_size": len(callbacks),
		}).Debug("")

		reply, err := this.client.Append(context.Background(), &api.AppendRequest{
			Topic:     topic,
			Partition: partition,
			Messages:  buffer.Bytes(),
		})

		var result error
		if err != nil {
			result = err
		} else if !reply.Ok {
			result = errors.New("not ok")
		}

		for _, callback := range callbacks {
			callback <- result
		}
	}
}
