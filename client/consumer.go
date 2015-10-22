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
	MessageId uint64

	// The offset, or closest offset, of the message.
	Offset    Offset
	Topic     string
	Partition int
	Message   []byte
}

type IncomingBatch struct {
	Offset   Offset
	Messages *storage.MessageSet
}

type BatchConsumer interface {
	Messages() <-chan IncomingBatch
	Close() error
}

type MergeBatchConsumer struct {
	consumers []BatchConsumer

	messages chan IncomingBatch
}

func MergeConsumers(consumers ...BatchConsumer) *MergeBatchConsumer {
	messages := make(chan IncomingBatch)
	var work sync.WaitGroup

	for _, consumer := range consumers {
		work.Add(1)

		go func(consumer BatchConsumer) {
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

	return &MergeBatchConsumer{
		consumers: consumers,
		messages:  messages,
	}
}

func (this *MergeBatchConsumer) Messages() <-chan IncomingBatch {
	return this.messages
}

func (this *MergeBatchConsumer) Close() error {
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
	messages  chan IncomingBatch

	close     chan struct{}
	closeOnce sync.Once
}

func (this *TopicPartitionConsumer) Close() error {
	// TODO: implement close
	return nil
}

func (this *TopicPartitionConsumer) Messages() <-chan IncomingBatch {
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
		messages:   make(chan IncomingBatch),
		close:      make(chan struct{}),
	}

	go consumer.doReading()
	//go consumer.doDispatching()

	return consumer, nil
}

// func (this *TopicPartitionConsumer) doDispatching() {
// 	defer close(this.messages)
//
// 	for batch := range this.dispatch {
// 		for index, message := range batch.Messages.Messages() {
// 			entry := batch.Messages.Entry(index)
//
// 			this.messages <- IncomingMessage{
// 				MessageId: uint64(entry.Id),
// 				Offset:    batch.Offset,
// 				Topic:     this.topic,
// 				Partition: this.partition,
// 				Message:   message[21:], // TODO: this should not be hard coded
// 			}
// 		}
// 	}
// }

func (this *TopicPartitionConsumer) doReading() {
	//delay := backoff.Exp(time.Millisecond, 1*time.Second)

	//defer close(this.dispatch)
	defer close(this.messages)

	logger := tidy.GetLogger().Withs(tidy.Fields{
		"host":      this.host,
		"topic":     this.topic,
		"partition": this.partition,
	})
	logger.Debug("reading started")

	replies, err := this.client.Read(context.Background(), &api.ReadRequest{
		Topic:      this.topic,
		Partition:  int32(this.partition),
		Offset:     this.offset.toOffsetData(),
		Continuous: this.continuous,
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

		this.messages <- IncomingBatch{
			Offset:   this.offset,
			Messages: storage.NewMessageSetFromBuffer(reply.Messages),
		}

		this.offset = offsetFromOffsetData(reply.Offset)
	}
}
