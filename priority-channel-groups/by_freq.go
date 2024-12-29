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
		priorityChannel: priority_channels.NewByFrequencyRatio[msgWithChannelName[T]](channels),
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
		msgWithNameC := processPriorityChannelToMsgsWithChannelName(ctx, q.PriorityChannel)
		res = append(res, channels.NewChannelWithFreqRatio[msgWithChannelName[T]]("", msgWithNameC, q.FreqRatio))
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
