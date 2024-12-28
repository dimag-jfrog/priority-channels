package channel_groups

import (
	"context"
	"github.com/dimag-jfrog/priority-channels"
	"sort"
)

func NewByFrequencyRatioAmongPriorityChannels[T any](ctx context.Context, priorityChannelsWithFreqRatio []PriorityChannelWithFreqRatio[T]) priority_channels.PriorityChannel[T] {
	channels := newPriorityChannelsGroupByFreqRatio[T](ctx, priorityChannelsWithFreqRatio)
	return &priorityChannelOfMsgsWithChannelName[T]{
		ctx:             ctx,
		priorityChannel: priority_channels.NewWithFrequencyRatio[msgWithChannelName[T]](channels),
	}
}

type PriorityChannelWithFreqRatio[T any] struct {
	priority_channels.PriorityChannel[T]
	FreqRatio int
}

func newPriorityChannelsGroupByFreqRatio[T any](
	ctx context.Context,
	priorityChannelsWithFreqRatio []PriorityChannelWithFreqRatio[T]) []priority_channels.ChannelFreqRatio[msgWithChannelName[T]] {
	res := make([]priority_channels.ChannelFreqRatio[msgWithChannelName[T]], 0, len(priorityChannelsWithFreqRatio))

	for _, q := range priorityChannelsWithFreqRatio {
		msgWithNameC := processPriorityChannelToMsgsWithChannelName(ctx, q.PriorityChannel)
		res = append(res, priority_channels.NewChannelWithFreqRatio[msgWithChannelName[T]]("", msgWithNameC, q.FreqRatio))
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
