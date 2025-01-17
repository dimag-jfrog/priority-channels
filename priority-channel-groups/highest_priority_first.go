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

type PriorityChannelWithPriority[T any] interface {
	Name() string
	PriorityChannel() priority_channels.PriorityChannel[T]
	Priority() int
}

type priorityChannelWithPriority[T any] struct {
	name            string
	priorityChannel priority_channels.PriorityChannel[T]
	priority        int
}

func (c *priorityChannelWithPriority[T]) Name() string {
	return c.name
}

func (c *priorityChannelWithPriority[T]) PriorityChannel() priority_channels.PriorityChannel[T] {
	return c.priorityChannel
}

func (c *priorityChannelWithPriority[T]) Priority() int {
	return c.priority
}

func NewPriorityChannelWithPriority[T any](name string, priorityChannel priority_channels.PriorityChannel[T], priority int) PriorityChannelWithPriority[T] {
	return &priorityChannelWithPriority[T]{
		name:            name,
		priorityChannel: priorityChannel,
		priority:        priority,
	}
}

func newPriorityChannelsGroupByHighestPriorityFirst[T any](
	ctx context.Context,
	priorityChannelsWithPriority []PriorityChannelWithPriority[T]) []channels.ChannelWithPriority[msgWithChannelName[T]] {
	res := make([]channels.ChannelWithPriority[msgWithChannelName[T]], 0, len(priorityChannelsWithPriority))

	for _, q := range priorityChannelsWithPriority {
		msgWithNameC, fnGetClosedChannelDetails, fnIsReady := processPriorityChannelToMsgsWithChannelName(ctx, q.Name(), q.PriorityChannel())
		channel := channels.NewChannelWithPriority[msgWithChannelName[T]]("", msgWithNameC, q.Priority())
		res = append(res, newChannelWithPriorityAndClosedChannelDetails(channel, fnGetClosedChannelDetails, fnIsReady))
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

type channelWithPriorityAndClosedChannelDetails[T any] struct {
	channel                   channels.ChannelWithPriority[T]
	fnGetClosedChannelDetails func() (string, priority_channels.ReceiveStatus)
	fnIsReady                 func() bool
}

func (c *channelWithPriorityAndClosedChannelDetails[T]) ChannelName() string {
	return c.channel.ChannelName()
}

func (c *channelWithPriorityAndClosedChannelDetails[T]) MsgsC() <-chan T {
	return c.channel.MsgsC()
}

func (c *channelWithPriorityAndClosedChannelDetails[T]) Priority() int {
	return c.channel.Priority()
}

func (c *channelWithPriorityAndClosedChannelDetails[T]) GetUnderlyingClosedChannelDetails() (string, priority_channels.ReceiveStatus) {
	return c.fnGetClosedChannelDetails()
}

func (c *channelWithPriorityAndClosedChannelDetails[T]) IsReady() bool {
	return c.fnIsReady()
}

func newChannelWithPriorityAndClosedChannelDetails[T any](
	channel channels.ChannelWithPriority[T],
	fnGetClosedChannelDetails func() (string, priority_channels.ReceiveStatus),
	fnIsReady func() bool) channels.ChannelWithPriority[T] {
	return &channelWithPriorityAndClosedChannelDetails[T]{
		channel:                   channel,
		fnGetClosedChannelDetails: fnGetClosedChannelDetails,
		fnIsReady:                 fnIsReady,
	}
}
