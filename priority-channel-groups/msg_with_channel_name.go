package priority_channel_groups

import (
	"context"

	priority_channels "github.com/dimag-jfrog/priority-channels"
)

type msgWithChannelName[T any] struct {
	Msg         T
	ChannelName string
}

type priorityChannelOfMsgsWithChannelName[T any] struct {
	priorityChannel priority_channels.PriorityChannel[msgWithChannelName[T]]
}

func (pc *priorityChannelOfMsgsWithChannelName[T]) Receive() (msg T, channelName string, ok bool) {
	msgWithChannelName, _, ok := pc.priorityChannel.Receive()
	if !ok {
		return getZero[T](), "", false
	}
	return msgWithChannelName.Msg, msgWithChannelName.ChannelName, true
}

func (pc *priorityChannelOfMsgsWithChannelName[T]) ReceiveContext(ctx context.Context) (msg T, channelName string, status priority_channels.ReceiveStatus) {
	msgWithChannelName, _, status := pc.priorityChannel.ReceiveContext(ctx)
	if status != priority_channels.ReceiveSuccess {
		return getZero[T](), "", status
	}
	return msgWithChannelName.Msg, msgWithChannelName.ChannelName, status
}

func processPriorityChannelToMsgsWithChannelName[T any](ctx context.Context, priorityChannel priority_channels.PriorityChannel[T]) <-chan msgWithChannelName[T] {
	msgWithNameC := make(chan msgWithChannelName[T])
	go func() {
		for {
			message, channelName, ok := priorityChannel.Receive()
			if !ok {
				break
			}
			select {
			case <-ctx.Done():
				return
			case msgWithNameC <- msgWithChannelName[T]{Msg: message, ChannelName: channelName}:
			}
		}
	}()
	return msgWithNameC
}

func getZero[T any]() T {
	var result T
	return result
}
