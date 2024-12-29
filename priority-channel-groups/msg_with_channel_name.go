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

func (pc *priorityChannelOfMsgsWithChannelName[T]) Receive(ctx context.Context) (msg T, channelName string, ok bool) {
	msgWithChannelName, _, ok := pc.priorityChannel.Receive(ctx)
	if !ok {
		return getZero[T](), "", false
	}
	return msgWithChannelName.Msg, msgWithChannelName.ChannelName, true
}

func processPriorityChannelToMsgsWithChannelName[T any](ctx context.Context, priorityChannel priority_channels.PriorityChannel[T]) <-chan msgWithChannelName[T] {
	msgWithNameC := make(chan msgWithChannelName[T])
	go func() {
		for {
			message, channelName, ok := priorityChannel.Receive(ctx)
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
