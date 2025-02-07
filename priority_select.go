package priority_channels

import (
	"context"

	"github.com/dimag-jfrog/priority-channels/channels"
)

func Select[T any](ctx context.Context,
	channelsWithPriorities []channels.ChannelWithPriority[T],
	options ...func(*PriorityQueueOptions)) (msg T, channelName string, status ReceiveStatus) {
	pq := NewByHighestAlwaysFirst(context.Background(), channelsWithPriorities, options...)
	return pq.ReceiveWithContext(ctx)
}

func SelectWithDefaultCase[T any](
	channelsWithPriorities []channels.ChannelWithPriority[T],
	options ...func(*PriorityQueueOptions)) (msg T, channelName string, status ReceiveStatus) {
	pq := NewByHighestAlwaysFirst(context.Background(), channelsWithPriorities, options...)
	return pq.ReceiveWithDefaultCase()
}
