package channels

import (
	"errors"
)

var ErrFreqRatioMustBeGreaterThanZero = errors.New("frequency ratio must be greater than 0")

type ChannelWithFreqRatio[T any] struct {
	channelName string
	msgsC       <-chan T
	freqRatio   int
}

func (c *ChannelWithFreqRatio[T]) ChannelName() string {
	return c.channelName
}

func (c *ChannelWithFreqRatio[T]) MsgsC() <-chan T {
	return c.msgsC
}

func (c *ChannelWithFreqRatio[T]) FreqRatio() int {
	return c.freqRatio
}

func (c *ChannelWithFreqRatio[T]) Validate() error {
	if c.freqRatio <= 0 {
		return ErrFreqRatioMustBeGreaterThanZero
	}
	return nil
}

func NewChannelWithFreqRatio[T any](channelName string, msgsC <-chan T, freqRatio int) ChannelWithFreqRatio[T] {
	return ChannelWithFreqRatio[T]{
		channelName: channelName,
		msgsC:       msgsC,
		freqRatio:   freqRatio,
	}
}
