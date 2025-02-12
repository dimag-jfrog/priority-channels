package channels

import (
	"errors"
)

type ChannelFreqRatio[T any] interface {
	Channel[T]
	FreqRatio() int
}

type channelFreqRatio[T any] struct {
	channelName string
	msgsC       <-chan T
	freqRatio   int
}

func (c *channelFreqRatio[T]) ChannelName() string {
	return c.channelName
}

func (c *channelFreqRatio[T]) MsgsC() <-chan T {
	return c.msgsC
}

func (c *channelFreqRatio[T]) FreqRatio() int {
	return c.freqRatio
}

func (c *channelFreqRatio[T]) Validate() error {
	if c.freqRatio <= 0 {
		return errors.New("frequency ratio must be greater than 0")
	}
	return nil
}

func NewChannelWithFreqRatio[T any](channelName string, msgsC <-chan T, freqRatio int) ChannelFreqRatio[T] {
	return &channelFreqRatio[T]{
		channelName: channelName,
		msgsC:       msgsC,
		freqRatio:   freqRatio,
	}
}
