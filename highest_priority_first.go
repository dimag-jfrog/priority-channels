package priority_channels

import (
	"context"
	"github.com/dimag-jfrog/priority-channels/channels"
	"github.com/dimag-jfrog/priority-channels/internal/selectable"
	"github.com/dimag-jfrog/priority-channels/strategies"
)

func NewByHighestAlwaysFirst[T any](ctx context.Context,
	channelsWithPriorities []channels.ChannelWithPriority[T],
	options ...func(*PriorityChannelOptions)) (*PriorityChannel[T], error) {
	selectableChannels := make([]selectable.ChannelWithWeight[T, int], 0, len(channelsWithPriorities))
	for _, c := range channelsWithPriorities {
		selectableChannels = append(selectableChannels, selectable.NewChannelWithWeight(
			channels.NewChannelWithWeight[T, int](c.ChannelName(), c.MsgsC(), c.Priority()),
		))
	}
	strategy := strategies.NewByHighestAlwaysFirst()
	return newByStrategy(ctx, strategy, selectableChannels, options...)
}
