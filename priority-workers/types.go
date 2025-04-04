package priority_workers

import (
	"context"

	"github.com/dimag-jfrog/priority-channels"
)

type ChannelWithCancelFunc[T any] struct {
	Channel    <-chan T
	CancelFunc context.CancelFunc
}

type ReceiveResult[T any] struct {
	Msg         T
	ChannelName string
	Status      priority_channels.ReceiveStatus
}

func (r ReceiveResult[T]) GetMsg() T {
	return r.Msg
}

func (r ReceiveResult[T]) GetChannelName() string {
	return r.ChannelName
}

func (r ReceiveResult[T]) GetStatus() priority_channels.ReceiveStatus {
	return r.Status
}

type ReceiveResulter[T any] interface {
	GetMsg() T
	GetChannelName() string
	GetStatus() priority_channels.ReceiveStatus
}

type ReceiveResultEx[T any] struct {
	Msg            T
	ReceiveDetails priority_channels.ReceiveDetails
	Status         priority_channels.ReceiveStatus
}

func (r ReceiveResultEx[T]) GetMsg() T {
	return r.Msg
}

func (r ReceiveResultEx[T]) GetReceiveDetails() priority_channels.ReceiveDetails {
	return r.ReceiveDetails
}

func (r ReceiveResultEx[T]) GetStatus() priority_channels.ReceiveStatus {
	return r.Status
}

type ReceiveResulterEx[T any] interface {
	GetMsg() T
	GetReceiveDetails() priority_channels.ReceiveDetails
	GetStatus() priority_channels.ReceiveStatus
}

type ResultChannelWithFreqRatio[T any] struct {
	channel    <-chan ReceiveResult[T]
	cancelFunc context.CancelFunc
	name       string
	freqRatio  int
}

func (c *ResultChannelWithFreqRatio[T]) Name() string {
	return c.name
}

func (c *ResultChannelWithFreqRatio[T]) ResultChannel() <-chan ReceiveResult[T] {
	return c.channel
}

func (c *ResultChannelWithFreqRatio[T]) Cancel() {
	c.cancelFunc()
}

func (c *ResultChannelWithFreqRatio[T]) FreqRatio() int {
	return c.freqRatio
}

func NewResultChannelWithFreqRatio[T any](name string, channel <-chan ReceiveResult[T], cancelFunc context.CancelFunc, freqRatio int) ResultChannelWithFreqRatio[T] {
	return ResultChannelWithFreqRatio[T]{
		name:       name,
		channel:    channel,
		cancelFunc: cancelFunc,
		freqRatio:  freqRatio,
	}
}

type ResultChannelWithFreqRatioEx[T any] struct {
	channel    <-chan ReceiveResultEx[T]
	cancelFunc context.CancelFunc
	name       string
	freqRatio  int
}

func (c *ResultChannelWithFreqRatioEx[T]) Name() string {
	return c.name
}

func (c *ResultChannelWithFreqRatioEx[T]) ResultChannel() <-chan ReceiveResultEx[T] {
	return c.channel
}

func (c *ResultChannelWithFreqRatioEx[T]) Cancel() {
	c.cancelFunc()
}

func (c *ResultChannelWithFreqRatioEx[T]) FreqRatio() int {
	return c.freqRatio
}

func NewResultChannelWithFreqRatioEx[T any](name string, channel <-chan ReceiveResultEx[T], cancelFunc context.CancelFunc, freqRatio int) ResultChannelWithFreqRatioEx[T] {
	return ResultChannelWithFreqRatioEx[T]{
		name:       name,
		channel:    channel,
		cancelFunc: cancelFunc,
		freqRatio:  freqRatio,
	}
}

type ResultChannelWithPriority[T any] struct {
	channel    <-chan ReceiveResult[T]
	cancelFunc context.CancelFunc
	name       string
	priority   int
}

func (c *ResultChannelWithPriority[T]) Name() string {
	return c.name
}

func (c *ResultChannelWithPriority[T]) ResultChannel() <-chan ReceiveResult[T] {
	return c.channel
}

func (c *ResultChannelWithPriority[T]) Cancel() {
	c.cancelFunc()
}

func (c *ResultChannelWithPriority[T]) Priority() int {
	return c.priority
}

func NewResultChannelWithPriority[T any](name string, channel <-chan ReceiveResult[T], cancelFunc context.CancelFunc, priority int) ResultChannelWithPriority[T] {
	return ResultChannelWithPriority[T]{
		name:       name,
		channel:    channel,
		priority:   priority,
		cancelFunc: cancelFunc,
	}
}

type ResultChannelWithPriorityEx[T any] struct {
	channel    <-chan ReceiveResultEx[T]
	cancelFunc context.CancelFunc
	name       string
	priority   int
}

func (c *ResultChannelWithPriorityEx[T]) Name() string {
	return c.name
}

func (c *ResultChannelWithPriorityEx[T]) ResultChannel() <-chan ReceiveResultEx[T] {
	return c.channel
}

func (c *ResultChannelWithPriorityEx[T]) Cancel() {
	c.cancelFunc()
}

func (c *ResultChannelWithPriorityEx[T]) Priority() int {
	return c.priority
}

func NewResultChannelWithPriorityEx[T any](name string, channel <-chan ReceiveResultEx[T], cancelFunc context.CancelFunc, priority int) ResultChannelWithPriorityEx[T] {
	return ResultChannelWithPriorityEx[T]{
		name:       name,
		channel:    channel,
		cancelFunc: cancelFunc,
		priority:   priority,
	}
}
