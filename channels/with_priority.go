package channels

type ChannelWithPriority[T any] interface {
	ChannelName() string
	MsgsC() <-chan T
	Priority() int
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
