package priority_channels

import (
	"context"
	"reflect"
	"sort"

	"github.com/dimag-jfrog/priority-channels/channels"
)

func NewByHighestAlwaysFirst[T any](ctx context.Context, channelsWithPriorities []channels.ChannelWithPriority[T]) PriorityChannel[T] {
	return newPriorityChannelByPriority[T](ctx, channelsWithPriorities)
}

func (pc *priorityChannelsHighestFirst[T]) Receive() (msg T, channelName string, ok bool) {
	msg, channelName, status := pc.receiveSingleMessage(context.Background(), false)
	if status != ReceiveSuccess {
		return getZero[T](), channelName, false
	}
	return msg, channelName, true
}

func (pc *priorityChannelsHighestFirst[T]) ReceiveWithContext(ctx context.Context) (msg T, channelName string, status ReceiveStatus) {
	return pc.receiveSingleMessage(ctx, false)
}

func (pc *priorityChannelsHighestFirst[T]) ReceiveWithDefaultCase() (msg T, channelName string, status ReceiveStatus) {
	return pc.receiveSingleMessage(context.Background(), true)
}

func (pc *priorityChannelsHighestFirst[T]) Context() context.Context {
	return pc.ctx
}

type priorityChannelsHighestFirst[T any] struct {
	ctx      context.Context
	channels []channels.ChannelWithPriority[T]
}

func newPriorityChannelByPriority[T any](
	ctx context.Context,
	channelsWithPriorities []channels.ChannelWithPriority[T]) *priorityChannelsHighestFirst[T] {
	pq := &priorityChannelsHighestFirst[T]{
		ctx:      ctx,
		channels: channelsWithPriorities,
	}
	sort.Slice(pq.channels, func(i int, j int) bool {
		return pq.channels[i].Priority() > pq.channels[j].Priority()
	})
	return pq
}

func ProcessMessagesByPriorityWithHighestAlwaysFirst[T any](
	ctx context.Context,
	channelsWithPriorities []channels.ChannelWithPriority[T],
	msgProcessor func(ctx context.Context, msg T, channelName string)) ExitReason {
	pq := newPriorityChannelByPriority(ctx, channelsWithPriorities)
	return processPriorityChannelMessages[T](pq, msgProcessor)
}

func (pc *priorityChannelsHighestFirst[T]) receiveSingleMessage(ctx context.Context, withDefaultCase bool) (msg T, channelName string, status ReceiveStatus) {
	lastPriorityChannelIndex := len(pc.channels) - 1
	for currPriorityChannelIndex := 0; currPriorityChannelIndex <= lastPriorityChannelIndex; currPriorityChannelIndex++ {
		chosen, recv, recvOk, selectStatus := selectCasesOfNextIteration(
			pc.ctx,
			ctx,
			pc.prepareSelectCases,
			currPriorityChannelIndex,
			lastPriorityChannelIndex,
			withDefaultCase)
		if selectStatus == ReceiveStatusUnknown {
			continue
		} else if selectStatus != ReceiveSuccess {
			return getZero[T](), "", selectStatus
		}
		channelName := pc.channels[chosen-2].ChannelName()
		if !recvOk {
			// no more messages in channel
			if c, ok := pc.channels[chosen-2].(ChannelWithUnderlyingClosedChannelName); ok {
				channelName = c.UnderlyingClosedChannelName()
			}
			return getZero[T](), channelName, ReceiveChannelClosed
		}
		// Message received successfully
		msg := recv.Interface().(T)
		return msg, channelName, ReceiveSuccess
	}
	return getZero[T](), "", ReceiveStatusUnknown
}

func (pc *priorityChannelsHighestFirst[T]) prepareSelectCases(currPriorityChannelIndex int) []reflect.SelectCase {
	var selectCases []reflect.SelectCase
	for i := 0; i <= currPriorityChannelIndex; i++ {
		selectCases = append(selectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(pc.channels[i].MsgsC()),
		})
	}
	return selectCases
}
