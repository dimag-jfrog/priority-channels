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
	msgReceived, noMoreMessages := pc.ReceiveSingleMessage(context.Background())
	if noMoreMessages != nil {
		return getZero[T](), "", false
	}
	return msgReceived.Msg, msgReceived.ChannelName, true
}

func (pc *priorityChannelsHighestFirst[T]) ReceiveContext(ctx context.Context) (msg T, channelName string, status ReceiveStatus) {
	msgReceived, noMoreMessages := pc.ReceiveSingleMessage(ctx)
	if noMoreMessages != nil {
		return getZero[T](), "", noMoreMessages.Reason.ReceiveStatus()
	}
	return msgReceived.Msg, msgReceived.ChannelName, ReceiveSuccess
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
	return processPriorityChannelMessages[T](ctx, pq, msgProcessor)
}

func (pc *priorityChannelsHighestFirst[T]) ReceiveSingleMessage(ctx context.Context) (msgReceived *msgReceivedEvent[T], noMoreMessages *noMoreMessagesEvent) {
	for nextPriorityChannelIndex := 0; nextPriorityChannelIndex < len(pc.channels); nextPriorityChannelIndex++ {
		selectCases := pc.prepareSelectCases(ctx, nextPriorityChannelIndex)
		chosen, recv, recvOk := reflect.Select(selectCases)
		if chosen == 0 {
			// context of the priority channel is done
			return nil, &noMoreMessagesEvent{Reason: ChannelClosed}
		}
		if chosen == 1 {
			// context of the specific request is done
			return nil, &noMoreMessagesEvent{Reason: ContextCancelled}
		}
		isLastIteration := nextPriorityChannelIndex == len(pc.channels)-1
		if !isLastIteration && chosen == len(selectCases)-1 {
			// Default case - go to next iteration to increase the range of allowed minimal priority channels
			// on last iteration - blocking wait on all receive channels without default case
			continue
		}
		if !recvOk {
			// no more messages in channel
			return nil, &noMoreMessagesEvent{Reason: ChannelClosed}
		}
		// Message received successfully
		msg := recv.Interface().(T)
		res := &msgReceivedEvent[T]{Msg: msg, ChannelName: pc.channels[chosen-2].ChannelName()}
		return res, nil
	}
	return nil, nil
}

func (pc *priorityChannelsHighestFirst[T]) prepareSelectCases(ctx context.Context, currPriorityChannelIndex int) []reflect.SelectCase {
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
	if !isLastIteration {
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
