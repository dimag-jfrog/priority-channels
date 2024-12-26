package channel_groups

import (
	"context"
	"sort"

	"github.com/dimag-jfrog/priority-channels"
)

type ChannelGroupWithHighestPriorityFirst[T any] struct {
	ChannelsWithPriority []priority_channels.ChannelWithPriority[T]
	FreqRatio            int
}

func newPriorityChannelsGroupByFreqRatio[T any](
	ctx context.Context,
	channelsGroupsWithFreqRatio []ChannelGroupWithHighestPriorityFirst[T]) []priority_channels.ChannelFreqRatio[msgWithChannelName[T]] {
	res := make([]priority_channels.ChannelFreqRatio[msgWithChannelName[T]], 0, len(channelsGroupsWithFreqRatio))

	for _, q := range channelsGroupsWithFreqRatio {
		aggregatedC := make(chan msgWithChannelName[T])
		if len(q.ChannelsWithPriority) == 1 {
			channel := q.ChannelsWithPriority[0]
			go messagesChannelToMessagesWithChannelNameChannel(ctx, channel.ChannelName, channel.MsgsC, aggregatedC)
		} else {
			msgProcessor := func(_ context.Context, msg T, ChannelName string) {
				aggregatedC <- msgWithChannelName[T]{Msg: msg, ChannelName: ChannelName}
			}
			go priority_channels.ProcessMessagesByPriorityWithHighestAlwaysFirst(ctx, q.ChannelsWithPriority, msgProcessor)
		}

		res = append(res, priority_channels.ChannelFreqRatio[msgWithChannelName[T]]{
			MsgsC:     aggregatedC,
			FreqRatio: q.FreqRatio,
		})
	}
	sort.Slice(res, func(i int, j int) bool {
		return res[i].FreqRatio > res[j].FreqRatio
	})
	return res
}

func ProcessMessagesByFreqRatioAmongHighestFirstChannelGroups[T any](
	ctx context.Context,
	channelsGroupsWithHighestPriorityFirst []ChannelGroupWithHighestPriorityFirst[T],
	msgProcessor func(ctx context.Context, msg T, channelName string)) priority_channels.ExitReason {
	channels := newPriorityChannelsGroupByFreqRatio(ctx, channelsGroupsWithHighestPriorityFirst)
	msgProcessorNew := func(_ context.Context, msg msgWithChannelName[T], channelName string) {
		msgProcessor(ctx, msg.Msg, msg.ChannelName)
	}
	return priority_channels.ProcessMessagesByFrequencyRatio[msgWithChannelName[T]](ctx, channels, msgProcessorNew)
}
