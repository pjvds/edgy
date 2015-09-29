package client

import (
	"time"

	"github.com/pjvds/backoff"
	"github.com/pjvds/edgy/api"
	"github.com/pjvds/edgy/storage"
	"github.com/pjvds/tidy"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type Consumer struct {
	continuous bool

	logger   tidy.Logger
	topic    string
	client   api.EdgyClient
	Messages chan []byte

	dispatch chan *storage.MessageSet
}

func NewConsumer(host string, topic string, continuous bool) (*Consumer, error) {
	connection, err := grpc.Dial(host, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	client := api.NewEdgyClient(connection)

	if _, err := client.Ping(context.Background(), &api.PingRequest{}); err != nil {
		connection.Close()
		return nil, err
	}

	consumer := &Consumer{
		continuous: continuous,
		logger:     tidy.GetLogger(),
		client:     client,
		topic:      topic,
		Messages:   make(chan []byte),
		dispatch:   make(chan *storage.MessageSet),
	}

	go consumer.doReading()
	go consumer.doDispatching()

	return consumer, nil
}

func (this *Consumer) doDispatching() {
	defer close(this.Messages)

	for messages := range this.dispatch {
		for _, message := range messages.Messages() {
			this.Messages <- message
		}
	}
}

func (this *Consumer) doReading() {
	offset := new(api.OffsetData)
	delay := backoff.Exp(time.Millisecond, 1*time.Second)

	defer close(this.dispatch)

	for {
		//fmt.Printf("READING REQUEST FROM: %v@%v/%v\n", offset.MessageId, offset.SegmentId, offset.EndPosition)

		reply, err := this.client.Read(context.Background(), &api.ReadRequest{Topic: this.topic, Partition: 0, Offset: offset})

		if err != nil {
			this.logger.WithError(err).Warn("read request failed")
			delay.Delay()
			continue
		}

		if !this.continuous && len(reply.Messages) == 0 {
			return
		}

		this.dispatch <- storage.NewMessageSetFromBuffer(reply.Messages)
		offset = reply.Offset
	}
}
