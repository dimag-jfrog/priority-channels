package priority_channel_groups

import (
	"context"
	"sort"

	"github.com/dimag-jfrog/priority-channels"
	"github.com/dimag-jfrog/priority-channels/channels"
)

func CombineByHighestPriorityFirst[T any](ctx context.Context, priorityChannelsWithPriority []PriorityChannelWithPriority[T]) priority_channels.PriorityChannel[T] {
	channels := newPriorityChannelsGroupByHighestPriorityFirst[T](ctx, priorityChannelsWithPriority)
	return &priorityChannelOfMsgsWithChannelName[T]{
		priorityChannel: priority_channels.NewByHighestAlwaysFirst[msgWithChannelName[T]](channels),
	}
}

type PriorityChannelWithPriority[T any] struct {
	priority_channels.PriorityChannel[T]
	Priority int
}

func newPriorityChannelsGroupByHighestPriorityFirst[T any](
	ctx context.Context,
	priorityChannelsWithPriority []PriorityChannelWithPriority[T]) []channels.ChannelWithPriority[msgWithChannelName[T]] {
	res := make([]channels.ChannelWithPriority[msgWithChannelName[T]], 0, len(priorityChannelsWithPriority))

	for _, q := range priorityChannelsWithPriority {
		msgWithNameC := processPriorityChannelToMsgsWithChannelName(ctx, q.PriorityChannel)
		res = append(res, channels.NewChannelWithPriority[msgWithChannelName[T]]("", msgWithNameC, q.Priority))
	}
	sort.Slice(res, func(i int, j int) bool {
		return res[i].Priority() > res[j].Priority()
	})
	return res
}

func ProcessPriorityChannelsByPriorityWithHighestAlwaysFirst[T any](
	ctx context.Context,
	priorityChannelsWithPriority []PriorityChannelWithPriority[T],
	msgProcessor func(ctx context.Context, msg T, channelName string)) priority_channels.ExitReason {
	channels := newPriorityChannelsGroupByHighestPriorityFirst(ctx, priorityChannelsWithPriority)
	msgProcessorNew := func(_ context.Context, msg msgWithChannelName[T], channelName string) {
		msgProcessor(ctx, msg.Msg, msg.ChannelName)
	}
	return priority_channels.ProcessMessagesByPriorityWithHighestAlwaysFirst[msgWithChannelName[T]](ctx, channels, msgProcessorNew)
}
