package channel_groups

import "context"

type msgWithChannelName[T any] struct {
	Msg         T
	ChannelName string
}

func messagesChannelToMessagesWithChannelNameChannel[T any](
	ctx context.Context,
	channelName string,
	msgsC <-chan T,
	msgsWithChannelNameC chan<- msgWithChannelName[T]) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgsC:
			if !ok {
				close(msgsWithChannelNameC)
				return
			}
			msgsWithChannelNameC <- msgWithChannelName[T]{Msg: msg, ChannelName: channelName}
		}
	}
}
