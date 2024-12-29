package priority_channels

import (
	"context"
)

type PriorityChannel[T any] interface {
	Receive(ctx context.Context) (msg T, channelName string, ok bool)
}

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

func WrapAsPriorityChannel[T any](channelName string, msgsC <-chan T) PriorityChannel[T] {
	return &wrappedChannel[T]{channelName: channelName, msgsC: msgsC}
}

type wrappedChannel[T any] struct {
	channelName string
	msgsC       <-chan T
}

func (w *wrappedChannel[T]) Receive(ctx context.Context) (msg T, channelName string, ok bool) {
	select {
	case <-ctx.Done():
		return getZero[T](), "", false
	case msg, ok = <-w.msgsC:
		return msg, w.channelName, ok
	}
}
