package priority_channels

import (
	"context"
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

type noMoreMessagesEvent struct {
	Reason ExitReason
}

func processPriorityChannelMessages[T any](
	ctx context.Context,
	msgReceiver priorityChannelMsgReceiver[T],
	msgProcessor func(ctx context.Context, msg T, ChannelName string)) ExitReason {
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
