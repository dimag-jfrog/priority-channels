package channel_groups

import (
	"context"
	"sort"

	"github.com/dimag-jfrog/priority-channels"
)

func NewByHighestPriorityFirstAmongFreqRatioChannelGroups[T any](ctx context.Context, channelsGroupsWithFreqRatio []PriorityChannelGroupWithFreqRatio[T]) priority_channels.PriorityChannel[T] {
	channels := newPriorityChannelsGroupByPriority[T](ctx, channelsGroupsWithFreqRatio)
	return &priorityChannelsByHighestPriorityFirstAmongFreqRatioChannelGroups[T]{
		ctx:                       ctx,
		priorityChannelByPriority: priority_channels.NewWithHighestAlwaysFirst[msgWithChannelName[T]](channels),
	}
}

type priorityChannelsByHighestPriorityFirstAmongFreqRatioChannelGroups[T any] struct {
	ctx                       context.Context
	priorityChannelByPriority priority_channels.PriorityChannel[msgWithChannelName[T]]
}

func (pc *priorityChannelsByHighestPriorityFirstAmongFreqRatioChannelGroups[T]) Receive(ctx context.Context) (msg T, channelName string, ok bool) {
	msgWithChannelName, _, ok := pc.priorityChannelByPriority.Receive(ctx)
	if !ok {
		return getZero[T](), "", false
	}
	return msgWithChannelName.Msg, msgWithChannelName.ChannelName, true
}

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
			channel := q.ChannelsWithFreqRatios[0]
			go messagesChannelToMessagesWithChannelNameChannel(ctx, channel.ChannelName, channel.MsgsC, aggregatedC)
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
	channelsGroupsWithFreqRatios []PriorityChannelGroupWithFreqRatio[T],
	msgProcessor func(ctx context.Context, msg T, channelName string)) priority_channels.ExitReason {
	channels := newPriorityChannelsGroupByPriority(ctx, channelsGroupsWithFreqRatios)
	msgProcessorNew := func(_ context.Context, msg msgWithChannelName[T], channelName string) {
		msgProcessor(ctx, msg.Msg, msg.ChannelName)
	}
	return priority_channels.ProcessMessagesByPriorityWithHighestAlwaysFirst[msgWithChannelName[T]](ctx, channels, msgProcessorNew)
}
