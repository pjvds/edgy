package client

import (
	"bytes"
	"errors"
	"sync"
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
	connection, err := grpc.Dial(address, grpc.WithInsecure())
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
	return producer, nil
}

type AppendResult struct {
	result <-chan error
}

func (this AppendResult) Wait() error {
	return <-this.result
}

type appendRequest struct {
	Message []byte
	result  chan error
}

func (this *Producer) Append(topic string, partition int32, message []byte) AppendResult {
	result := make(chan error, 1)
	this.getSender(topic, partition) <- appendRequest{
		Message: message,
		result:  result,
	}

	return AppendResult{
		result: result,
	}
}

var senders = make(map[string]map[int32]chan appendRequest)
var sendersLock sync.Mutex
var counter int64
var lastPrint = time.Now()

func (this *Producer) getSender(topic string, partition int32) chan appendRequest {
	sendersLock.Lock()
	defer sendersLock.Unlock()

	topicSenders, ok := senders[topic]

	if !ok {
		topicSenders = make(map[int32]chan appendRequest)
		senders[topic] = topicSenders
	}

	partitionSender, ok := topicSenders[partition]

	if !ok {
		partitionSender = make(chan appendRequest, 5000)
		topicSenders[partition] = partitionSender

		go this.senderLoop(topic, partition, partitionSender)
	}

	return partitionSender
}

func (this *Producer) senderLoop(topic string, partition int32, requests chan appendRequest) {
	callbacks := make([]chan error, 0, this.config.QueueSize)
	buffer := new(bytes.Buffer)

	for {
		callbacks = callbacks[0:0]
		buffer.Reset()

		// receive first request
		request := <-requests

		callbacks = append(callbacks, request.result)
		buffer.Write(storage.NewMessage(0, request.Message))

		// receive next requests until full or timed out
		timeout := time.After(this.config.QueueTime)

	enqueue:
		for index := 1; index < this.config.QueueSize; index++ {
			select {
			case request = <-requests:
				callbacks = append(callbacks, request.result)
				buffer.Write(storage.NewMessage(0, request.Message))

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
