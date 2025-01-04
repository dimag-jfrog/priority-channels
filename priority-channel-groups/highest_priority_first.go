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
		priorityChannel: priority_channels.NewByHighestAlwaysFirst[msgWithChannelName[T]](ctx, channels),
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
		msgWithNameC, fnGetClosedChannelName := processPriorityChannelToMsgsWithChannelName(ctx, q.PriorityChannel)
		channel := channels.NewChannelWithPriority[msgWithChannelName[T]]("", msgWithNameC, q.Priority)
		res = append(res, newChannelWithPriorityAndClosedChannelName(channel, fnGetClosedChannelName))
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

type channelWithPriorityAndClosedChannelName[T any] struct {
	channel                channels.ChannelWithPriority[T]
	fnGetClosedChannelName func() string
}

func (c *channelWithPriorityAndClosedChannelName[T]) ChannelName() string {
	return c.channel.ChannelName()
}

func (c *channelWithPriorityAndClosedChannelName[T]) MsgsC() <-chan T {
	return c.channel.MsgsC()
}

func (c *channelWithPriorityAndClosedChannelName[T]) Priority() int {
	return c.channel.Priority()
}

func (c *channelWithPriorityAndClosedChannelName[T]) UnderlyingClosedChannelName() string {
	return c.fnGetClosedChannelName()
}

func newChannelWithPriorityAndClosedChannelName[T any](channel channels.ChannelWithPriority[T], fnGetClosedChannelName func() string) channels.ChannelWithPriority[T] {
	return &channelWithPriorityAndClosedChannelName[T]{
		channel:                channel,
		fnGetClosedChannelName: fnGetClosedChannelName,
	}
}
