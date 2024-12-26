package channel_groups

import (
	"context"
	"sort"

	"github.com/dimag-jfrog/priority-channels"
)

type PriorityChannelGroupWithFreqRatio[T any] struct {
	ChannelsWithFreqRatios []priority_channels.ChannelFreqRatio[T]
	Priority               int
}

func newPriorityChannelsGroupByPriority[T any](
	ctx context.Context,
	channelsGroupsWithPriorities []PriorityChannelGroupWithFreqRatio[T]) []priority_channels.ChannelWithPriority[msgWithChannelName[T]] {
	res := make([]priority_channels.ChannelWithPriority[msgWithChannelName[T]], 0, len(channelsGroupsWithPriorities))

	for _, q := range channelsGroupsWithPriorities {
		aggregatedC := make(chan msgWithChannelName[T])
		if len(q.ChannelsWithFreqRatios) == 1 {
			Channel := q.ChannelsWithFreqRatios[0]
			go messagesChannelToMessagesWithChannelNameChannel(ctx, Channel.ChannelName, Channel.MsgsC, aggregatedC)
		} else {
			msgProcessor := func(_ context.Context, msg T, ChannelName string) {
				aggregatedC <- msgWithChannelName[T]{Msg: msg, ChannelName: ChannelName}
			}
			go priority_channels.ProcessMessagesByFrequencyRatio(ctx, q.ChannelsWithFreqRatios, msgProcessor)
		}

		res = append(res, priority_channels.ChannelWithPriority[msgWithChannelName[T]]{
			MsgsC:    aggregatedC,
			Priority: q.Priority,
		})
	}
	sort.Slice(res, func(i int, j int) bool {
		return res[i].Priority > res[j].Priority
	})
	return res
}

func ProcessMessagesByPriorityAmongFreqRatioChannelGroups[T any](
	ctx context.Context,
	ChannelsGroupsWithFreqRatios []PriorityChannelGroupWithFreqRatio[T],
	msgProcessor func(ctx context.Context, msg T, ChannelName string)) priority_channels.ExitReason {
	Channels := newPriorityChannelsGroupByPriority(ctx, ChannelsGroupsWithFreqRatios)
	msgProcessorNew := func(_ context.Context, msg msgWithChannelName[T], ChannelName string) {
		msgProcessor(ctx, msg.Msg, msg.ChannelName)
	}
	return priority_channels.ProcessMessagesByPriorityWithHighestAlwaysFirst[msgWithChannelName[T]](ctx, Channels, msgProcessorNew)
}
