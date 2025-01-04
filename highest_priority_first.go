package priority_channels

import (
	"context"
	"reflect"
	"sort"
	"time"

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
		channels: make([]channels.ChannelWithPriority[T], 0, len(channelsWithPriorities)),
	}

	for _, q := range channelsWithPriorities {
		pq.channels = append(pq.channels, channels.NewChannelWithPriority[T](
			q.ChannelName(),
			q.MsgsC(),
			q.Priority()))
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
	for nextPriorityChannelIndex := 0; nextPriorityChannelIndex < len(pc.channels); nextPriorityChannelIndex++ {
		selectCases := pc.prepareSelectCases(ctx, nextPriorityChannelIndex, withDefaultCase)
		chosen, recv, recvOk := reflect.Select(selectCases)
		if chosen == 0 {
			// context of the priority channel is done
			return getZero[T](), "", ReceiveChannelClosed
		}
		if chosen == 1 {
			// context of the specific request is done
			return getZero[T](), "", ReceiveContextCancelled
		}
		isLastIteration := nextPriorityChannelIndex == len(pc.channels)-1
		if chosen == len(selectCases)-1 {
			if !isLastIteration {
				// Default case - go to next iteration to increase the range of allowed minimal priority channels
				// on last iteration - blocking wait on all receive channels without default case
				continue
			} else if withDefaultCase {
				return getZero[T](), "", ReceiveDefaultCase
			}
		}
		channelName := pc.channels[chosen-2].ChannelName()
		if !recvOk {
			// no more messages in channel
			return getZero[T](), channelName, ReceiveChannelClosed
		}
		// Message received successfully
		msg := recv.Interface().(T)
		return msg, channelName, ReceiveSuccess
	}
	return getZero[T](), "", ReceiveStatusUnknown
}

func (pc *priorityChannelsHighestFirst[T]) prepareSelectCases(ctx context.Context, currPriorityChannelIndex int, withDefaultCase bool) []reflect.SelectCase {
	var selectCases []reflect.SelectCase
	selectCases = append(selectCases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(pc.ctx.Done()),
	})
	selectCases = append(selectCases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	})
	for i := 0; i <= currPriorityChannelIndex; i++ {
		selectCases = append(selectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(pc.channels[i].MsgsC()),
		})
	}
	isLastIteration := currPriorityChannelIndex == len(pc.channels)-1
	if !isLastIteration || withDefaultCase {
		selectCases = append(selectCases, reflect.SelectCase{
			// The default option without any sleep did not pass tests
			// short sleep is needed to guarantee that we do not enter default case when there are still messages
			// in the deliveries channel that can be retrieved
			//Dir: reflect.SelectDefault,
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(time.After(100 * time.Microsecond)),
		})
	}
	return selectCases
}
