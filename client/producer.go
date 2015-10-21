package client

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

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
	cluster  Cluster
	logger   tidy.Logger
	requests chan appendRequest

	config ProducerConfig
}

func NewProducer(cluster Cluster, config ProducerConfig) (*Producer, error) {
	producer := &Producer{
		cluster:  cluster,
		logger:   tidy.GetLogger(),
		requests: make(chan appendRequest),
		config:   config,
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

func (this *Producer) Append(topic string, key int, message []byte) AppendResult {
	result := make(chan error, 1)
	partition := int32(key % this.cluster.Partitions())

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
	node, ok := this.cluster.GetNode(partition)
	if !ok {
		panic("no node for partition " + strconv.Itoa(int(partition)))
	}
	client := node.client
	logger := this.logger.With("sender", fmt.Sprintf("%v/%v@%v", topic, partition, node.IP))

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
		for index := 0; index < this.config.QueueSize; index++ {
			select {
			case request = <-requests:
				callbacks = append(callbacks, request.result)
				buffer.Write(storage.NewMessage(0, request.Message))

			case <-timeout:
				logger.With("timeout", this.config.QueueTime).Debug("queue time expired")
				break enqueue
			}
		}

		logger.Withs(tidy.Fields{
			"queue_size": len(callbacks),
		}).Debug("")

		reply, err := client.Append(context.Background(), &api.AppendRequest{
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
