package channel_groups

import (
	"context"
	"github.com/dimag-jfrog/priority-channels"
	"sort"
)

func NewByHighestPriorityFirstAmongPriorityChannels[T any](ctx context.Context, priorityChannelsWithPriority []PriorityChannelWithPriority[T]) priority_channels.PriorityChannel[T] {
	channels := newPriorityChannelsGroupByHighestPriorityFirst[T](ctx, priorityChannelsWithPriority)
	return &priorityChannelOfMsgsWithChannelName[T]{
		ctx:             ctx,
		priorityChannel: priority_channels.NewWithHighestAlwaysFirst[msgWithChannelName[T]](channels),
	}
}

type PriorityChannelWithPriority[T any] struct {
	priority_channels.PriorityChannel[T]
	Priority int
}

func newPriorityChannelsGroupByHighestPriorityFirst[T any](
	ctx context.Context,
	priorityChannelsWithPriority []PriorityChannelWithPriority[T]) []priority_channels.ChannelWithPriority[msgWithChannelName[T]] {
	res := make([]priority_channels.ChannelWithPriority[msgWithChannelName[T]], 0, len(priorityChannelsWithPriority))

	for _, q := range priorityChannelsWithPriority {
		msgWithNameC := processPriorityChannelToMsgsWithChannelName(ctx, q.PriorityChannel)
		res = append(res, priority_channels.NewChannelWithPriority[msgWithChannelName[T]]("", msgWithNameC, q.Priority))
	}
	sort.Slice(res, func(i int, j int) bool {
		return res[i].Priority() > res[j].Priority()
	})
	return res
}

func ProcessMessagesByPriorityWithHighestAlwaysFirst[T any](
	ctx context.Context,
	priorityChannelsWithPriority []PriorityChannelWithPriority[T],
	msgProcessor func(ctx context.Context, msg T, channelName string)) priority_channels.ExitReason {
	channels := newPriorityChannelsGroupByHighestPriorityFirst(ctx, priorityChannelsWithPriority)
	msgProcessorNew := func(_ context.Context, msg msgWithChannelName[T], channelName string) {
		msgProcessor(ctx, msg.Msg, msg.ChannelName)
	}
	return priority_channels.ProcessMessagesByPriorityWithHighestAlwaysFirst[msgWithChannelName[T]](ctx, channels, msgProcessorNew)
}
