package priority_channels

import (
	"context"
	"reflect"
	"sort"
	"time"
)

func NewWithHighestAlwaysFirst[T any](channelsWithPriorities []ChannelWithPriority[T]) PriorityChannel[T] {
	return newPriorityChannelByPriority[T](channelsWithPriorities)
}

func (pc *priorityChannelsHighestFirst[T]) Receive(ctx context.Context) (msg T, channelName string, ok bool) {
	msgReceived, noMoreMessages := pc.ReceiveSingleMessage(ctx)
	if noMoreMessages != nil {
		return getZero[T](), "", false
	}
	return msgReceived.Msg, msgReceived.ChannelName, true
}

type channelWithPriority[T any] struct {
	channelName string
	msgsC       <-chan T
	priority    int
}

func (c *channelWithPriority[T]) ChannelName() string {
	return c.channelName
}

func (c *channelWithPriority[T]) MsgsC() <-chan T {
	return c.msgsC
}

func (c *channelWithPriority[T]) Priority() int {
	return c.priority
}

func NewChannelWithPriority[T any](channelName string, msgsC <-chan T, priority int) ChannelWithPriority[T] {
	return &channelWithPriority[T]{
		channelName: channelName,
		msgsC:       msgsC,
		priority:    priority,
	}
}

type ChannelWithPriority[T any] interface {
	ChannelName() string
	MsgsC() <-chan T
	Priority() int
}

type priorityChannelsHighestFirst[T any] struct {
	channels []ChannelWithPriority[T]
}

func newPriorityChannelByPriority[T any](
	channelsWithPriorities []ChannelWithPriority[T]) *priorityChannelsHighestFirst[T] {
	pq := &priorityChannelsHighestFirst[T]{
		channels: make([]ChannelWithPriority[T], 0, len(channelsWithPriorities)),
	}

	for _, q := range channelsWithPriorities {
		pq.channels = append(pq.channels, &channelWithPriority[T]{
			channelName: q.ChannelName(),
			msgsC:       q.MsgsC(),
			priority:    q.Priority(),
		})
	}
	sort.Slice(pq.channels, func(i int, j int) bool {
		return pq.channels[i].Priority() > pq.channels[j].Priority()
	})
	return pq
}

func ProcessMessagesByPriorityWithHighestAlwaysFirst[T any](
	ctx context.Context,
	channelsWithPriorities []ChannelWithPriority[T],
	msgProcessor func(ctx context.Context, msg T, channelName string)) ExitReason {
	pq := newPriorityChannelByPriority(channelsWithPriorities)
	return processPriorityChannelMessages[T](ctx, pq, msgProcessor)
}

func (pc *priorityChannelsHighestFirst[T]) ReceiveSingleMessage(ctx context.Context) (msgReceived *msgReceivedEvent[T], noMoreMessages *noMoreMessagesEvent) {
	for nextPriorityChannelIndex := 0; nextPriorityChannelIndex < len(pc.channels); nextPriorityChannelIndex++ {
		selectCases := pc.prepareSelectCases(ctx, nextPriorityChannelIndex)
		chosen, recv, recvOk := reflect.Select(selectCases)
		if chosen == 0 {
			// ctx.Done Channel
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
		res := &msgReceivedEvent[T]{Msg: msg, ChannelName: pc.channels[chosen-1].ChannelName()}
		return res, nil
	}
	return nil, nil
}

func (pc *priorityChannelsHighestFirst[T]) prepareSelectCases(ctx context.Context, currPriorityChannelIndex int) []reflect.SelectCase {
	var selectCases []reflect.SelectCase
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
