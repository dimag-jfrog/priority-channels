package priority_channel_groups

import (
	"context"
	"sort"

	"github.com/dimag-jfrog/priority-channels"
	"github.com/dimag-jfrog/priority-channels/channels"
)

func CombineByFrequencyRatio[T any](ctx context.Context, priorityChannelsWithFreqRatio []PriorityChannelWithFreqRatio[T]) priority_channels.PriorityChannel[T] {
	channels := newPriorityChannelsGroupByFreqRatio[T](ctx, priorityChannelsWithFreqRatio)
	return &priorityChannelOfMsgsWithChannelName[T]{
		priorityChannel: priority_channels.NewByFrequencyRatio[msgWithChannelName[T]](ctx, channels),
	}
}

type PriorityChannelWithFreqRatio[T any] interface {
	Name() string
	PriorityChannel() priority_channels.PriorityChannel[T]
	FreqRatio() int
}

type priorityChannelWithFreqRatio[T any] struct {
	name            string
	priorityChannel priority_channels.PriorityChannel[T]
	freqRatio       int
}

func (c *priorityChannelWithFreqRatio[T]) Name() string {
	return c.name
}

func (c *priorityChannelWithFreqRatio[T]) PriorityChannel() priority_channels.PriorityChannel[T] {
	return c.priorityChannel
}

func (c *priorityChannelWithFreqRatio[T]) FreqRatio() int {
	return c.freqRatio
}

func NewPriorityChannelWithFreqRatio[T any](name string, priorityChannel priority_channels.PriorityChannel[T], freqRatio int) PriorityChannelWithFreqRatio[T] {
	return &priorityChannelWithFreqRatio[T]{
		name:            name,
		priorityChannel: priorityChannel,
		freqRatio:       freqRatio,
	}
}

func newPriorityChannelsGroupByFreqRatio[T any](
	ctx context.Context,
	priorityChannelsWithFreqRatio []PriorityChannelWithFreqRatio[T]) []channels.ChannelFreqRatio[msgWithChannelName[T]] {
	res := make([]channels.ChannelFreqRatio[msgWithChannelName[T]], 0, len(priorityChannelsWithFreqRatio))

	for _, q := range priorityChannelsWithFreqRatio {
		msgWithNameC, fnGetClosedChannelDetails, fnIsReady := processPriorityChannelToMsgsWithChannelName(ctx, q.Name(), q.PriorityChannel())
		channel := channels.NewChannelWithFreqRatio[msgWithChannelName[T]]("", msgWithNameC, q.FreqRatio())
		res = append(res, newChannelFreqRatioWithClosedChannelDetails(channel, fnGetClosedChannelDetails, fnIsReady))
	}
	sort.Slice(res, func(i int, j int) bool {
		return res[i].FreqRatio() > res[j].FreqRatio()
	})
	return res
}

func ProcessPriorityChannelsByFrequencyRatio[T any](
	ctx context.Context,
	priorityChannelsWithFreqRatio []PriorityChannelWithFreqRatio[T],
	msgProcessor func(ctx context.Context, msg T, channelName string)) priority_channels.ExitReason {
	channels := newPriorityChannelsGroupByFreqRatio[T](ctx, priorityChannelsWithFreqRatio)
	msgProcessorNew := func(_ context.Context, msg msgWithChannelName[T], channelName string) {
		msgProcessor(ctx, msg.Msg, msg.ChannelName)
	}
	return priority_channels.ProcessMessagesByFrequencyRatio[msgWithChannelName[T]](ctx, channels, msgProcessorNew)
}

type channelFreqRatioWithClosedChannelDetails[T any] struct {
	channel                   channels.ChannelFreqRatio[T]
	fnGetClosedChannelDetails func() (string, priority_channels.ReceiveStatus)
	fnIsReady                 func() bool
}

func (c *channelFreqRatioWithClosedChannelDetails[T]) ChannelName() string {
	return c.channel.ChannelName()
}

func (c *channelFreqRatioWithClosedChannelDetails[T]) MsgsC() <-chan T {
	return c.channel.MsgsC()
}

func (c *channelFreqRatioWithClosedChannelDetails[T]) FreqRatio() int {
	return c.channel.FreqRatio()
}

func (c *channelFreqRatioWithClosedChannelDetails[T]) GetUnderlyingClosedChannelDetails() (string, priority_channels.ReceiveStatus) {
	return c.fnGetClosedChannelDetails()
}

func (c *channelFreqRatioWithClosedChannelDetails[T]) IsReady() bool {
	return c.fnIsReady()
}

func newChannelFreqRatioWithClosedChannelDetails[T any](
	channelWithFreqRatio channels.ChannelFreqRatio[T],
	fnGetClosedChannelDetails func() (string, priority_channels.ReceiveStatus),
	fnIsReady func() bool) channels.ChannelFreqRatio[T] {
	return &channelFreqRatioWithClosedChannelDetails[T]{
		channel:                   channelWithFreqRatio,
		fnGetClosedChannelDetails: fnGetClosedChannelDetails,
		fnIsReady:                 fnIsReady,
	}
}
