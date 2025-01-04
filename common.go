package priority_channels

import (
	"context"
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
	ReceiveStatusUnknown
)

func (r ReceiveStatus) ExitReason() ExitReason {
	switch r {
	case ReceiveContextCancelled:
		return ContextCancelled
	case ReceiveChannelClosed:
		return ChannelClosed
	default:
		return UnknownExitReason
	}
}

type ExitReason int

const (
	ContextCancelled ExitReason = iota
	ChannelClosed
	UnknownExitReason
)

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
		return getZero[T](), w.channelName, ReceiveChannelClosed
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
		return getZero[T](), w.channelName, ReceiveChannelClosed
	case msg, ok := <-w.msgsC:
		if !ok {
			return getZero[T](), w.channelName, ReceiveChannelClosed
		}
		return msg, w.channelName, ReceiveSuccess
	default:
		return getZero[T](), "", ReceiveDefaultCase
	}
}
