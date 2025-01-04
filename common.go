package priority_channels

import (
	"context"
)

type PriorityChannel[T any] interface {
	Receive() (msg T, channelName string, ok bool)
	ReceiveWithContext(ctx context.Context) (msg T, channelName string, status ReceiveStatus)
}

type ReceiveStatus int

const (
	ReceiveSuccess ReceiveStatus = iota
	ReceiveContextCancelled
	ReceiveChannelClosed
)

type priorityChannelMsgReceiver[T any] interface {
	ReceiveSingleMessage(ctx context.Context) (msgReceived *msgReceivedEvent[T], noMoreMessages *noMoreMessagesEvent)
}

type msgReceivedEvent[T any] struct {
	Msg         T
	ChannelName string
}

type ExitReason int

const (
	ContextCancelled ExitReason = iota
	ChannelClosed
)

func (r ExitReason) ReceiveStatus() ReceiveStatus {
	switch r {
	case ContextCancelled:
		return ReceiveContextCancelled
	case ChannelClosed:
		return ReceiveChannelClosed
	default:
		return ReceiveChannelClosed
	}
}

type noMoreMessagesEvent struct {
	Reason ExitReason
}

func processPriorityChannelMessages[T any](
	ctx context.Context,
	msgReceiver priorityChannelMsgReceiver[T],
	msgProcessor func(ctx context.Context, msg T, channelName string)) ExitReason {
	for {
		msgReceived, noMoreMessages := msgReceiver.ReceiveSingleMessage(ctx)
		if noMoreMessages != nil {
			return noMoreMessages.Reason
		}
		if msgReceived == nil {
			continue
		}
		msgProcessor(ctx, msgReceived.Msg, msgReceived.ChannelName)
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
		return getZero[T](), "", ReceiveChannelClosed
	case <-ctx.Done():
		return getZero[T](), "", ReceiveContextCancelled
	case msg, ok := <-w.msgsC:
		if !ok {
			return getZero[T](), "", ReceiveChannelClosed
		}
		return msg, w.channelName, ReceiveSuccess
	}
}
