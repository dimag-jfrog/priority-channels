package priority_channels

import (
	"context"

	"github.com/dimag-jfrog/priority-channels/channels"
	"github.com/dimag-jfrog/priority-channels/internal/selectable"
	"github.com/dimag-jfrog/priority-channels/strategies"
)

func NewByFrequencyRatio[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T],
	options ...func(*PriorityChannelOptions)) (*PriorityChannel[T], error) {
	selectableChannels := make([]selectable.ChannelWithWeight[T, int], 0, len(channelsWithFreqRatios))
	for _, c := range channelsWithFreqRatios {
		selectableChannels = append(selectableChannels, selectable.NewChannelWithWeight(
			channels.NewChannelWithWeight[T, int](c.ChannelName(), c.MsgsC(), c.FreqRatio()),
		))
	}
	strategy := strategies.NewByFreqRatio()
	return newByStrategy(ctx, strategy, selectableChannels, options...)
}
