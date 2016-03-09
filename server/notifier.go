package server

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/pjvds/edgy/storage"
)

type NotificationReceiver struct {
	channel chan storage.MessageId
	context context.Context
}

type WriteNotifier struct {
	receivers []*NotificationReceiver
	lock      sync.Mutex
}

func (this *WriteNotifier) Notify(context context.Context) *NotificationReceiver {
	this.lock.Lock()
	defer this.lock.Unlock()

	receiver := &NotificationReceiver{
		channel: make(chan storage.MessageId, 1),
		context: context,
	}
	this.receivers = append(this.receivers, receiver)
	return receiver
}

func (this *WriteNotifier) notifyAll(messageId storage.MessageId) {
	this.lock.Lock()
	defer this.lock.Unlock()

	var toRemove []int
	for index, receiver := range this.receivers {
		select {
		case receiver.channel <- messageId:
		case <-receiver.context.Done():
			toRemove = append(toRemove, index)
		default:
		}
	}

	if len(toRemove) > 0 {
		filteredReceivers := make([]*NotificationReceiver, 0, len(this.receivers)-len(toRemove))

		for index, receiver := range this.receivers {
			markedForRemoval := false

			for _, toRemoveIndex := range toRemove {
				if index == toRemoveIndex {
					markedForRemoval = true
					break
				}
			}

			if !markedForRemoval {
				filteredReceivers = append(filteredReceivers, receiver)
			}
		}

		this.receivers = filteredReceivers
	}
}
