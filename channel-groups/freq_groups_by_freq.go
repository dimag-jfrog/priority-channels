package channel_groups

import (
	"context"
	"sort"

	"github.com/dimag-jfrog/priority-channels"
)

func NewByFreqRatioAmongFreqRatioChannelGroups[T any](ctx context.Context, channelsGroupsWithFreqRatio []FreqRatioChannelGroupWithFreqRatio[T]) priority_channels.PriorityChannel[T] {
	channels := newFreqRatioChannelsGroupByFreqRatio[T](ctx, channelsGroupsWithFreqRatio)
	return &priorityChannelsByFreqRatioAmongFreqRatioChannelGroups[T]{
		ctx:                         ctx,
		freqRatioChannelByFreqRatio: priority_channels.NewWithFrequencyRatio[msgWithChannelName[T]](channels),
	}
}

type priorityChannelsByFreqRatioAmongFreqRatioChannelGroups[T any] struct {
	ctx                         context.Context
	freqRatioChannelByFreqRatio priority_channels.PriorityChannel[msgWithChannelName[T]]
}

func (pc *priorityChannelsByFreqRatioAmongFreqRatioChannelGroups[T]) Receive(ctx context.Context) (msg T, channelName string, ok bool) {
	msgWithChannelName, _, ok := pc.freqRatioChannelByFreqRatio.Receive(ctx)
	if !ok {
		return getZero[T](), "", false
	}
	return msgWithChannelName.Msg, msgWithChannelName.ChannelName, true
}

type FreqRatioChannelGroupWithFreqRatio[T any] struct {
	ChannelsWithFreqRatios []priority_channels.ChannelFreqRatio[T]
	FreqRatio              int
}

func newFreqRatioChannelsGroupByFreqRatio[T any](
	ctx context.Context,
	freqRatioChannelsGroupsWithFreqRatio []FreqRatioChannelGroupWithFreqRatio[T]) []priority_channels.ChannelFreqRatio[msgWithChannelName[T]] {
	res := make([]priority_channels.ChannelFreqRatio[msgWithChannelName[T]], 0, len(freqRatioChannelsGroupsWithFreqRatio))

	for _, q := range freqRatioChannelsGroupsWithFreqRatio {
		aggregatedC := make(chan msgWithChannelName[T])
		if len(q.ChannelsWithFreqRatios) == 1 {
			channel := q.ChannelsWithFreqRatios[0]
			go messagesChannelToMessagesWithChannelNameChannel(ctx, channel.ChannelName(), channel.MsgsC(), aggregatedC)
		} else {
			msgProcessor := func(_ context.Context, msg T, ChannelName string) {
				aggregatedC <- msgWithChannelName[T]{Msg: msg, ChannelName: ChannelName}
			}
			go priority_channels.ProcessMessagesByFrequencyRatio(ctx, q.ChannelsWithFreqRatios, msgProcessor)
		}

		res = append(res, priority_channels.NewChannelWithFreqRatio[msgWithChannelName[T]]("", aggregatedC, q.FreqRatio))
	}
	sort.Slice(res, func(i int, j int) bool {
		return res[i].FreqRatio() > res[j].FreqRatio()
	})
	return res
}

func ProcessMessagesByFreqRatioAmongFreqRatioChannelGroups[T any](
	ctx context.Context,
	channelsGroupsWithFreqRatios []FreqRatioChannelGroupWithFreqRatio[T],
	msgProcessor func(ctx context.Context, msg T, channelName string)) priority_channels.ExitReason {
	channels := newFreqRatioChannelsGroupByFreqRatio(ctx, channelsGroupsWithFreqRatios)
	msgProcessorNew := func(_ context.Context, msg msgWithChannelName[T], channelName string) {
		msgProcessor(ctx, msg.Msg, msg.ChannelName)
	}
	return priority_channels.ProcessMessagesByFrequencyRatio[msgWithChannelName[T]](ctx, channels, msgProcessorNew)
}
