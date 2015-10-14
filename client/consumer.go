package client

import (
	"sync"

	"github.com/pjvds/edgy/api"
	"github.com/pjvds/edgy/storage"
	"github.com/pjvds/tidy"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type IncomingMessage struct {
	// The offset, or closest offset, of the message.
	MessageId uint64
	Offset    Offset
	Topic     string
	Partition int
	Message   []byte
}

type incomingBatch struct {
	Offset   Offset
	Messages *storage.MessageSet
}

type Consumer interface {
	Messages() <-chan IncomingMessage
	Close() error
}

type MergeConsumer struct {
	consumers []Consumer

	messages chan IncomingMessage
}

func MergeConsumers(consumers ...Consumer) *MergeConsumer {
	messages := make(chan IncomingMessage)
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

func (this *MergeConsumer) Messages() <-chan IncomingMessage {
	return this.messages
}

func (this *MergeConsumer) Close() error {
	for _, consumer := range this.consumers {
		consumer.Close()
	}
	return nil
}

type TopicPartitionConsumer struct {
	host       string
	continuous bool

	logger    tidy.Logger
	topic     string
	partition int
	offset    Offset
	client    api.EdgyClient
	messages  chan IncomingMessage

	dispatch  chan incomingBatch
	close     chan struct{}
	closeOnce sync.Once
}

func (this *TopicPartitionConsumer) Close() error {
	// TODO: implement close
	return nil
}

func (this *TopicPartitionConsumer) Messages() <-chan IncomingMessage {
	return this.messages
}

func NewTopicPartitionConsumer(host string, topic string, partition int, offset Offset, continuous bool) (*TopicPartitionConsumer, error) {
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
		offset:     offset,
		logger:     tidy.GetLogger(),
		client:     client,
		topic:      topic,
		partition:  partition,
		messages:   make(chan IncomingMessage),
		dispatch:   make(chan incomingBatch),
		close:      make(chan struct{}),
	}

	go consumer.doReading()
	go consumer.doDispatching()

	return consumer, nil
}

func (this *TopicPartitionConsumer) doDispatching() {
	defer close(this.messages)

	for batch := range this.dispatch {
		for index, message := range batch.Messages.Messages() {
			entry := batch.Messages.Entry(index)

			this.messages <- IncomingMessage{
				MessageId: uint64(entry.Id),
				Offset:    batch.Offset,
				Topic:     this.topic,
				Partition: this.partition,
				Message:   message,
			}
		}
	}
}

func (this *TopicPartitionConsumer) doReading() {
	//delay := backoff.Exp(time.Millisecond, 1*time.Second)

	defer close(this.dispatch)

	logger := tidy.GetLogger().Withs(tidy.Fields{
		"host":      this.host,
		"topic":     this.topic,
		"partition": this.partition,
	})
	logger.Debug("reading started")

	replies, err := this.client.Read(context.Background(), &api.ReadRequest{
		Topic:     this.topic,
		Partition: int32(this.partition),
		Offset:    this.offset.toOffsetData(),
	})

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
			logger.With("offset", this.offset).Warn("EOF")
			return
		}

		if this.logger.IsDebug() {
			logger.With("offset", this.offset).Debug("reply received")
		}

		this.dispatch <- incomingBatch{
			Offset:   this.offset,
			Messages: storage.NewMessageSetFromBuffer(reply.Messages),
		}

		this.offset = offsetFromOffsetData(reply.Offset)
	}
}
