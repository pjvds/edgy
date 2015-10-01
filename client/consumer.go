package client

import (
	"sync"

	"github.com/pjvds/edgy/api"
	"github.com/pjvds/edgy/storage"
	"github.com/pjvds/tidy"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type Consumer interface {
	Messages() <-chan []byte
	Close() error
}

type MergeConsumer struct {
	consumers []Consumer

	messages chan []byte
}

func MergeConsumers(consumers ...Consumer) *MergeConsumer {
	messages := make(chan []byte)
	var work sync.WaitGroup

	for _, consumer := range consumers {
		work.Add(1)

		go func(consumer Consumer) {
			defer work.Done()
			for value := range consumer.Messages() {
				messages <- value
			}
		}(consumer)
	}

	go func() {
		work.Wait()
		close(messages)
	}()

	return &MergeConsumer{
		consumers: consumers,
		messages:  messages,
	}
}

func (this *MergeConsumer) Messages() <-chan []byte {
	return this.messages
}

func (this *MergeConsumer) Close() error {
	// TODO: close all
	return nil
}

type TopicPartitionConsumer struct {
	host       string
	continuous bool

	logger    tidy.Logger
	topic     string
	partition int32
	client    api.EdgyClient
	messages  chan []byte

	dispatch  chan *storage.MessageSet
	close     chan struct{}
	closeOnce sync.Once
}

func (this *TopicPartitionConsumer) Close() error {
	// TODO: implement close
	return nil
}

func (this *TopicPartitionConsumer) Messages() <-chan []byte {
	return this.messages
}

func NewTopicPartitionConsumer(host string, topic string, partition int32, continuous bool) (*TopicPartitionConsumer, error) {
	connection, err := grpc.Dial(host, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	client := api.NewEdgyClient(connection)

	if _, err := client.Ping(context.Background(), &api.PingRequest{}); err != nil {
		connection.Close()
		return nil, err
	}

	consumer := &TopicPartitionConsumer{
		host:       host,
		continuous: continuous,
		logger:     tidy.GetLogger(),
		client:     client,
		topic:      topic,
		partition:  partition,
		messages:   make(chan []byte),
		dispatch:   make(chan *storage.MessageSet),
		close:      make(chan struct{}),
	}

	go consumer.doReading()
	go consumer.doDispatching()

	return consumer, nil
}

func (this *TopicPartitionConsumer) doDispatching() {
	defer close(this.messages)

	for messages := range this.dispatch {
		for _, message := range messages.Messages() {
			this.messages <- message
		}
	}
}

func (this *TopicPartitionConsumer) doReading() {
	offset := new(api.OffsetData)
	//delay := backoff.Exp(time.Millisecond, 1*time.Second)

	defer close(this.dispatch)

	logger := tidy.GetLogger().Withs(tidy.Fields{
		"host":      this.host,
		"topic":     this.topic,
		"partition": this.partition,
	})
	logger.Debug("reading started")

	replies, err := this.client.Read(context.Background(), &api.ReadRequest{Topic: this.topic, Partition: this.partition, Offset: offset})

	if err != nil {
		logger.WithError(err).Error("read request failed")
		return
	}

	for {
		reply, err := replies.Recv()
		if err != nil {
			logger.WithError(err).Error("read request failed")
			break
		}

		if len(reply.Messages) == 0 {
			logger.With("offset", offset).Warn("EOF")
			return
		}

		if this.logger.IsDebug() {
			logger.With("offset", offset).Debug("reply received")
		}

		this.dispatch <- storage.NewMessageSetFromBuffer(reply.Messages)
		offset = reply.Offset
	}
}
