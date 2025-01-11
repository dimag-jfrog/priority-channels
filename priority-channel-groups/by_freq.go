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

type PriorityChannelWithFreqRatio[T any] struct {
	priority_channels.PriorityChannel[T]
	FreqRatio int
}

func newPriorityChannelsGroupByFreqRatio[T any](
	ctx context.Context,
	priorityChannelsWithFreqRatio []PriorityChannelWithFreqRatio[T]) []channels.ChannelFreqRatio[msgWithChannelName[T]] {
	res := make([]channels.ChannelFreqRatio[msgWithChannelName[T]], 0, len(priorityChannelsWithFreqRatio))

	for _, q := range priorityChannelsWithFreqRatio {
		msgWithNameC, fnGetClosedChanelName := processPriorityChannelToMsgsWithChannelName(ctx, q.PriorityChannel)
		channel := channels.NewChannelWithFreqRatio[msgWithChannelName[T]]("", msgWithNameC, q.FreqRatio)
		res = append(res, newChannelFreqRatioWithClosedChannelName(channel, fnGetClosedChanelName))
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

type channelFreqRatioWithClosedChannelName[T any] struct {
	channel                channels.ChannelFreqRatio[T]
	fnGetClosedChannelName func() string
}

func (c *channelFreqRatioWithClosedChannelName[T]) ChannelName() string {
	return c.channel.ChannelName()
}

func (c *channelFreqRatioWithClosedChannelName[T]) MsgsC() <-chan T {
	return c.channel.MsgsC()
}

func (c *channelFreqRatioWithClosedChannelName[T]) FreqRatio() int {
	return c.channel.FreqRatio()
}

func (c *channelFreqRatioWithClosedChannelName[T]) UnderlyingClosedChannelName() string {
	return c.fnGetClosedChannelName()
}

func newChannelFreqRatioWithClosedChannelName[T any](channelWithFreqRatio channels.ChannelFreqRatio[T], fnGetClosedChannelName func() string) channels.ChannelFreqRatio[T] {
	return &channelFreqRatioWithClosedChannelName[T]{
		channel:                channelWithFreqRatio,
		fnGetClosedChannelName: fnGetClosedChannelName,
	}
}
