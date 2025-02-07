package priority_channels

import (
	"context"
	"reflect"
	"sync/atomic"
	"time"
)

type PriorityChannel[T any] interface {
	Receive() (msg T, channelName string, ok bool)
	ReceiveWithContext(ctx context.Context) (msg T, channelName string, status ReceiveStatus)
	ReceiveWithDefaultCase() (msg T, channelName string, status ReceiveStatus)
}

type PriorityChannelWithContext[T any] interface {
	PriorityChannel[T]
	Context() context.Context
}

type ReceiveStatus int

const (
	ReceiveSuccess ReceiveStatus = iota
	ReceiveContextCancelled
	ReceiveChannelClosed
	ReceiveDefaultCase
	ReceivePriorityChannelCancelled
	ReceiveStatusUnknown
)

func (r ReceiveStatus) ExitReason() ExitReason {
	switch r {
	case ReceiveContextCancelled:
		return ContextCancelled
	case ReceiveChannelClosed:
		return ChannelClosed
	case ReceivePriorityChannelCancelled:
		return PriorityChannelCancelled
	default:
		return UnknownExitReason
	}
}

type ExitReason int

const (
	ContextCancelled ExitReason = iota
	ChannelClosed
	PriorityChannelCancelled
	UnknownExitReason
)

type PriorityQueueOptions struct {
	channelReceiveWaitInterval *time.Duration
}

const defaultChannelReceiveWaitInterval = 100 * time.Microsecond

func ChannelWaitInterval(d time.Duration) func(opt *PriorityQueueOptions) {
	return func(opt *PriorityQueueOptions) {
		opt.channelReceiveWaitInterval = &d
	}
}

type ChannelWithUnderlyingClosedChannelDetails interface {
	GetUnderlyingClosedChannelDetails() (channelName string, closeStatus ReceiveStatus)
}

type ReadinessChecker interface {
	IsReady() bool
}

func processPriorityChannelMessages[T any](
	msgReceiver PriorityChannelWithContext[T],
	msgProcessor func(ctx context.Context, msg T, channelName string)) ExitReason {
	for {
		// There is no context per-message, but there is a single context for the entire priority-channel
		// On receiving the message we do not pass any specific context,
		// but on processing the message we pass the priority-channel context
		msg, channelName, status := msgReceiver.ReceiveWithContext(context.Background())
		if status != ReceiveSuccess {
			return status.ExitReason()
		}
		msgProcessor(msgReceiver.Context(), msg, channelName)
	}
}

func getZero[T any]() T {
	var result T
	return result
}

func WrapAsPriorityChannel[T any](ctx context.Context, channelName string, msgsC <-chan T) PriorityChannel[T] {
	return &wrappedChannel[T]{ctx: ctx, channelName: channelName, msgsC: msgsC}
}

type wrappedChannel[T any] struct {
	ctx         context.Context
	channelName string
	msgsC       <-chan T
}

func (w *wrappedChannel[T]) Receive() (msg T, channelName string, ok bool) {
	select {
	case <-w.ctx.Done():
		return getZero[T](), "", false
	case msg, ok = <-w.msgsC:
		return msg, w.channelName, ok
	}
}

func (w *wrappedChannel[T]) ReceiveWithContext(ctx context.Context) (msg T, channelName string, status ReceiveStatus) {
	select {
	case <-w.ctx.Done():
		return getZero[T](), w.channelName, ReceivePriorityChannelCancelled
	case <-ctx.Done():
		return getZero[T](), "", ReceiveContextCancelled
	case msg, ok := <-w.msgsC:
		if !ok {
			return getZero[T](), w.channelName, ReceiveChannelClosed
		}
		return msg, w.channelName, ReceiveSuccess
	}
}

func (w *wrappedChannel[T]) ReceiveWithDefaultCase() (msg T, channelName string, status ReceiveStatus) {
	select {
	case <-w.ctx.Done():
		return getZero[T](), w.channelName, ReceivePriorityChannelCancelled
	case msg, ok := <-w.msgsC:
		if !ok {
			return getZero[T](), w.channelName, ReceiveChannelClosed
		}
		return msg, w.channelName, ReceiveSuccess
	default:
		return getZero[T](), "", ReceiveDefaultCase
	}
}

func selectCasesOfNextIteration(
	priorityChannelContext context.Context,
	currRequestContext context.Context,
	fnPrepareChannelsSelectCases func(currIterationIndex int) []reflect.SelectCase,
	currIterationIndex int,
	lastIterationIndex int,
	withDefaultCase bool,
	isPreparing *atomic.Bool,
	channelReceiveWaitInterval *time.Duration) (chosen int, recv reflect.Value, recvOk bool, status ReceiveStatus) {

	isLastIteration := currIterationIndex == lastIterationIndex
	channelsSelectCases := fnPrepareChannelsSelectCases(currIterationIndex)

	selectCases := make([]reflect.SelectCase, 0, len(channelsSelectCases)+3)
	selectCases = append(selectCases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(priorityChannelContext.Done()),
	})
	selectCases = append(selectCases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(currRequestContext.Done()),
	})
	selectCases = append(selectCases, channelsSelectCases...)
	if !isLastIteration || withDefaultCase || isPreparing.Load() {
		waitInterval := defaultChannelReceiveWaitInterval
		if channelReceiveWaitInterval != nil {
			waitInterval = *channelReceiveWaitInterval
		}
		if waitInterval > 0 {
			selectCases = append(selectCases, reflect.SelectCase{
				// The default behavior without a wait interval may not work
				// if receiving a message from the channel takes some time.
				// In such cases, a short wait is needed to ensure that the default case
				// is not triggered while messages are still available in the channel.
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(time.After(waitInterval)),
			})
		} else {
			selectCases = append(selectCases, reflect.SelectCase{
				Dir: reflect.SelectDefault,
			})
		}
	}

	chosen, recv, recvOk = reflect.Select(selectCases)
	if chosen == 0 {
		// context of the priority channel is done
		status = ReceivePriorityChannelCancelled
		return
	}
	if chosen == 1 {
		// context of the specific request is done
		status = ReceiveContextCancelled
		return
	}
	if chosen == len(selectCases)-1 {
		if !isLastIteration {
			// Default case - go to next iteration to increase the range of allowed minimal priority channels
			// on last iteration - blocking wait on all receive channels without default case
			status = ReceiveStatusUnknown
			return
		} else if withDefaultCase {
			status = ReceiveDefaultCase
			return
		} else if isPreparing.Load() {
			isPreparing.Store(false)
			// recursive call for last iteration - this time will issue a blocking wait on all channels
			return selectCasesOfNextIteration(priorityChannelContext,
				currRequestContext,
				fnPrepareChannelsSelectCases,
				currIterationIndex,
				lastIterationIndex,
				withDefaultCase,
				isPreparing,
				channelReceiveWaitInterval)
		}
	}
	status = ReceiveSuccess
	return
}

func waitForReadyStatus(ch interface{}) {
	if ch == nil {
		return
	}
	if checker, ok := ch.(ReadinessChecker); ok {
		for !checker.IsReady() {
			time.Sleep(100 * time.Microsecond)
		}
	}
	return
}
