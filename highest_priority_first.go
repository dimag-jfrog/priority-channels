package priority_channels

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync/atomic"
	"time"

	"github.com/dimag-jfrog/priority-channels/channels"
)

func NewByHighestAlwaysFirst[T any](ctx context.Context,
	channelsWithPriorities []channels.ChannelWithPriority[T],
	options ...func(*PriorityQueueOptions)) (PriorityChannel[T], error) {
	if len(channelsWithPriorities) == 0 {
		return nil, ErrNoChannels
	}
	channelNames := make(map[string]struct{})
	for _, c := range channelsWithPriorities {
		if c.ChannelName() == "" {
			return nil, ErrEmptyChannelName
		}
		if _, ok := channelNames[c.ChannelName()]; ok {
			return nil, &DuplicateChannelError{ChannelName: c.ChannelName()}
		}
		channelNames[c.ChannelName()] = struct{}{}
		if c.Priority() < 0 {
			return nil, fmt.Errorf("channel %s: priority cannot be negative", c.ChannelName())
		}
	}
	return newPriorityChannelByPriority[T](ctx, channelsWithPriorities, options...), nil
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
	ctx                        context.Context
	channels                   []channels.ChannelWithPriority[T]
	isPreparing                atomic.Bool
	channelReceiveWaitInterval *time.Duration
}

func newPriorityChannelByPriority[T any](
	ctx context.Context,
	channelsWithPriorities []channels.ChannelWithPriority[T],
	options ...func(*PriorityQueueOptions)) *priorityChannelsHighestFirst[T] {
	pqOptions := &PriorityQueueOptions{}
	for _, option := range options {
		option(pqOptions)
	}

	pq := &priorityChannelsHighestFirst[T]{
		ctx:                        ctx,
		channels:                   make([]channels.ChannelWithPriority[T], 0, len(channelsWithPriorities)),
		channelReceiveWaitInterval: pqOptions.channelReceiveWaitInterval,
	}
	for _, c := range channelsWithPriorities {
		pq.channels = append(pq.channels, c)
	}
	sort.Slice(pq.channels, func(i int, j int) bool {
		return pq.channels[i].Priority() > pq.channels[j].Priority()
	})
	return pq
}

func (pc *priorityChannelsHighestFirst[T]) receiveSingleMessage(ctx context.Context, withDefaultCase bool) (msg T, channelName string, status ReceiveStatus) {
	pc.isPreparing.Store(true)
	defer pc.isPreparing.Store(false)
	lastPriorityChannelIndex := len(pc.channels) - 1
	for currPriorityChannelIndex := 0; currPriorityChannelIndex <= lastPriorityChannelIndex; currPriorityChannelIndex++ {
		chosen, recv, recvOk, selectStatus := selectCasesOfNextIteration(
			pc.ctx,
			ctx,
			pc.prepareSelectCases,
			currPriorityChannelIndex,
			lastPriorityChannelIndex,
			withDefaultCase,
			&pc.isPreparing,
			pc.channelReceiveWaitInterval)
		if selectStatus == ReceiveStatusUnknown {
			continue
		} else if selectStatus != ReceiveSuccess {
			return getZero[T](), "", selectStatus
		}
		channelName := pc.channels[chosen-2].ChannelName()
		if !recvOk {
			// no more messages in channel
			if c, ok := pc.channels[chosen-2].(ChannelWithUnderlyingClosedChannelDetails); ok {
				underlyingChannelName, closeStatus := c.GetUnderlyingClosedChannelDetails()
				if underlyingChannelName == "" {
					underlyingChannelName = channelName
				}
				return getZero[T](), underlyingChannelName, closeStatus
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
		waitForReadyStatus(pc.channels[i])
		selectCases = append(selectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(pc.channels[i].MsgsC()),
		})
	}
	return selectCases
}

func (pc *priorityChannelsHighestFirst[T]) IsReady() bool {
	return pc.isPreparing.Load() == false
}
