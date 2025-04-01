package priority_workers

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/dimag-jfrog/priority-channels"
	"github.com/dimag-jfrog/priority-channels/channels"
)

type getReceiveResultFunc[T any, R any] func(
	msg T,
	channelName string,
	channelIndex int,
	receiveDetails priority_channels.ReceiveDetails,
	status priority_channels.ReceiveStatus) R

func getReceiveResult[T any](
	msg T, channelName string, _ int, receiveDetails priority_channels.ReceiveDetails,
	status priority_channels.ReceiveStatus) ReceiveResult[T] {
	resChannelName := receiveDetails.ChannelName
	if resChannelName == "" {
		resChannelName = channelName
	}
	return ReceiveResult[T]{
		Msg:         msg,
		ChannelName: resChannelName,
		Status:      status,
	}
}

func getReceiveResultEx[T any](
	msg T,
	channelName string, channelIndex int,
	receiveDetails priority_channels.ReceiveDetails,
	status priority_channels.ReceiveStatus) ReceiveResultEx[T] {
	var pathInTree []priority_channels.ChannelNode
	if channelIndex != -1 {
		if receiveDetails.ChannelName == "" && len(receiveDetails.PathInTree) == 0 {
			receiveDetails.ChannelName = channelName
			receiveDetails.ChannelIndex = channelIndex
		} else {
			pathInTree = append([]priority_channels.ChannelNode{{
				ChannelName:  channelName,
				ChannelIndex: channelIndex,
			}}, receiveDetails.PathInTree...)
		}
	} else {
		pathInTree = receiveDetails.PathInTree
	}
	return ReceiveResultEx[T]{
		Msg: msg,
		ReceiveDetails: priority_channels.ReceiveDetails{
			ChannelName:  receiveDetails.ChannelName,
			ChannelIndex: receiveDetails.ChannelIndex,
			PathInTree:   pathInTree,
		},
		Status: status,
	}
}

func processWithCallbackToChannel[T any, R any](ctx context.Context, fnProcessWithCallback func(func(r R, closeChannelAfter bool)), fnGetReceiveResult getReceiveResultFunc[T, R]) <-chan R {
	resChannel := make(chan R, 1)
	senderChannel := make(chan R)
	closeChannel := make(chan struct{})
	fnCallback := func(r R, closeChannelAfter bool) {
		select {
		case <-ctx.Done():
			return
		case senderChannel <- r:
			if closeChannelAfter {
				close(closeChannel)
			}
		}
	}
	go fnProcessWithCallback(fnCallback)
	go func() {
		defer close(resChannel)
		for {
			select {
			case <-ctx.Done():
				resChannel <- fnGetReceiveResult(
					getZero[T](), "", -1,
					priority_channels.ReceiveDetails{},
					priority_channels.ReceiveContextCancelled)
				return
			case <-closeChannel:
				return
			case msg := <-senderChannel:
				resChannel <- msg
			}
		}
	}()
	return resChannel
}

func ProcessByFrequencyRatio[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T]) <-chan ReceiveResult[T] {
	return processByFrequencyRatio(ctx, channelsWithFreqRatios, getReceiveResult)
}

func ProcessByFrequencyRatioEx[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T]) <-chan ReceiveResultEx[T] {
	return processByFrequencyRatio(ctx, channelsWithFreqRatios, getReceiveResultEx)
}

func processByFrequencyRatio[T any, R any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T], fnGetReceiveResult getReceiveResultFunc[T, R]) <-chan R {
	return processWithCallbackToChannel(ctx, func(fnCallback func(r R, closeChannel bool)) {
		processByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, fnGetReceiveResult, false)
	}, fnGetReceiveResult)
}

func ProcessByFrequencyRatioWithCallback[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T], fnCallback func(ReceiveResult[T], bool)) {
	processByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, getReceiveResult, true)
}

func ProcessByFrequencyRatioWithCallbackEx[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T], fnCallback func(ReceiveResultEx[T], bool)) {
	processByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, getReceiveResultEx, true)
}

func processByFrequencyRatioWithCallback[T any, R any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T],
	fnCallback func(R, bool),
	fnGetReceiveResult getReceiveResultFunc[T, R],
	notifyOnContextCancelled bool) {
	var openChannelsNum atomic.Int32
	openChannelsNum.Store(int32(len(channelsWithFreqRatios)))
	for i := range channelsWithFreqRatios {
		var closeChannelOnce sync.Once
		for j := 0; j < channelsWithFreqRatios[i].FreqRatio(); j++ {
			go func(c channels.ChannelWithFreqRatio[T], i int) {
				for {
					select {
					case <-ctx.Done():
						return
					case msg, ok := <-c.MsgsC():
						if !ok {
							closeChannelOnce.Do(func() {
								fnCallback(fnGetReceiveResult(getZero[T](), "", -1,
									priority_channels.ReceiveDetails{ChannelName: c.ChannelName(), ChannelIndex: i},
									priority_channels.ReceiveChannelClosed), false)
								openChannelsNum.Add(-1)
								if openChannelsNum.Load() == 0 {
									fnCallback(fnGetReceiveResult(getZero[T](), "", -1,
										priority_channels.ReceiveDetails{},
										priority_channels.ReceiveNoOpenChannels), true)
								}
							})
							return
						}
						fnCallback(fnGetReceiveResult(
							msg, "", -1,
							priority_channels.ReceiveDetails{ChannelName: c.ChannelName(), ChannelIndex: i},
							priority_channels.ReceiveSuccess), false)
					}
				}
			}(channelsWithFreqRatios[i], i)
		}
	}
	if notifyOnContextCancelled {
		go func() {
			<-ctx.Done()
			fnCallback(fnGetReceiveResult(
				getZero[T](), "", -1,
				priority_channels.ReceiveDetails{},
				priority_channels.ReceiveContextCancelled), false)
		}()
	}
}

func ProcessPriorityChannelsByFrequencyRatio[T any](ctx context.Context,
	channelsWithFreqRatios []priority_channels.PriorityChannelWithFreqRatio[T]) <-chan ReceiveResult[T] {
	return processPriorityChannelsByFrequencyRatio(ctx, channelsWithFreqRatios, getReceiveResult)
}

func ProcessPriorityChannelsByFrequencyRatioEx[T any](ctx context.Context,
	channelsWithFreqRatios []priority_channels.PriorityChannelWithFreqRatio[T]) <-chan ReceiveResultEx[T] {
	return processPriorityChannelsByFrequencyRatio(ctx, channelsWithFreqRatios, getReceiveResultEx)
}

func processPriorityChannelsByFrequencyRatio[T any, R any](ctx context.Context,
	channelsWithFreqRatios []priority_channels.PriorityChannelWithFreqRatio[T],
	fnGetReceiveResult getReceiveResultFunc[T, R]) <-chan R {
	return processWithCallbackToChannel(ctx, func(fnCallback func(r R, closeChannel bool)) {
		processPriorityChannelsByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, fnGetReceiveResult, false)
	}, fnGetReceiveResult)
}

func ProcessPriorityChannelsByFrequencyRatioWithCallback[T any](ctx context.Context,
	channelsWithFreqRatios []priority_channels.PriorityChannelWithFreqRatio[T], fnCallback func(ReceiveResult[T], bool)) {
	processPriorityChannelsByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, getReceiveResult, true)
}

func ProcessPriorityChannelsByFrequencyRatioWithCallbackEx[T any](ctx context.Context,
	channelsWithFreqRatios []priority_channels.PriorityChannelWithFreqRatio[T], fnCallback func(ReceiveResultEx[T], bool)) {
	processPriorityChannelsByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, getReceiveResultEx, true)
}

func processPriorityChannelsByFrequencyRatioWithCallback[T any, R any](ctx context.Context,
	channelsWithFreqRatios []priority_channels.PriorityChannelWithFreqRatio[T], fnCallback func(R, bool),
	fnGetReceiveResult getReceiveResultFunc[T, R],
	notifyOnContextCancelled bool) {
	var openChannelsNum atomic.Int32
	openChannelsNum.Store(int32(len(channelsWithFreqRatios)))
	for i := range channelsWithFreqRatios {
		var closeChannelOnce sync.Once
		for j := 0; j < channelsWithFreqRatios[i].FreqRatio(); j++ {
			go func(c priority_channels.PriorityChannelWithFreqRatio[T], i int) {
				for {
					msg, receiveDetails, status := c.PriorityChannel().ReceiveWithContextEx(ctx)
					if status == priority_channels.ReceiveContextCancelled {
						return
					} else if status == priority_channels.ReceivePriorityChannelClosed {
						closeChannelOnce.Do(func() {
							fnCallback(fnGetReceiveResult(getZero[T](), "", -1,
								priority_channels.ReceiveDetails{ChannelName: c.Name(), ChannelIndex: i},
								priority_channels.ReceivePriorityChannelClosed), false)
							openChannelsNum.Add(-1)
							if openChannelsNum.Load() == 0 {
								fnCallback(fnGetReceiveResult(getZero[T](), "", -1,
									priority_channels.ReceiveDetails{},
									priority_channels.ReceiveNoOpenChannels), true)
							}
						})
						return
					}
					fnCallback(fnGetReceiveResult(msg, c.Name(), i,
						receiveDetails,
						status), false)
					if status != priority_channels.ReceiveSuccess {
						return
					}
				}
			}(channelsWithFreqRatios[i], i)
		}
	}
	if notifyOnContextCancelled {
		go func() {
			<-ctx.Done()
			fnCallback(fnGetReceiveResult(getZero[T](), "", -1,
				priority_channels.ReceiveDetails{},
				priority_channels.ReceiveContextCancelled), false)
		}()
	}
}

func CombineByFrequencyRatio[T any](ctx context.Context,
	channelsWithFreqRatios []ResultChannelWithFreqRatio[T]) <-chan ReceiveResult[T] {
	return processWithCallbackToChannel(ctx, func(fnCallback func(r ReceiveResult[T], closeChannel bool)) {
		CombineByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, false)
	}, getReceiveResult)
}

func CombineByFrequencyRatioEx[T any](ctx context.Context,
	channelsWithFreqRatios []ResultChannelWithFreqRatioEx[T]) <-chan ReceiveResultEx[T] {
	return processWithCallbackToChannel(ctx, func(fnCallback func(r ReceiveResultEx[T], closeChannel bool)) {
		CombineByFrequencyRatioWithCallbackEx(ctx, channelsWithFreqRatios, fnCallback, false)
	}, getReceiveResultEx)
}

func CombineByFrequencyRatioWithCallback[T any](ctx context.Context,
	channelsWithFreqRatios []ResultChannelWithFreqRatio[T], fnCallback func(ReceiveResult[T], bool), notifyOnContextCancelled bool) {
	var openChannelsNum atomic.Int32
	openChannelsNum.Store(int32(len(channelsWithFreqRatios)))
	for i := range channelsWithFreqRatios {
		var closeChannelOnce sync.Once
		for j := 0; j < channelsWithFreqRatios[i].FreqRatio(); j++ {
			go func(c ResultChannelWithFreqRatio[T]) {
				for {
					select {
					case <-ctx.Done():
						return
					case msg, ok := <-c.ResultChannel():
						if !ok {
							closeChannelOnce.Do(func() {
								fnCallback(ReceiveResult[T]{
									Msg:         getZero[T](),
									ChannelName: c.Name(),
									Status:      priority_channels.ReceivePriorityChannelClosed,
								}, false)
								openChannelsNum.Add(-1)
								if openChannelsNum.Load() == 0 {
									fnCallback(ReceiveResult[T]{
										Msg:         getZero[T](),
										ChannelName: "",
										Status:      priority_channels.ReceiveNoOpenChannels,
									}, true)
								}
							})
							return
						}
						fnCallback(ReceiveResult[T]{
							Msg:         msg.Msg,
							ChannelName: msg.ChannelName,
							Status:      msg.Status,
						}, false)
					}
				}
			}(channelsWithFreqRatios[i])
		}
	}
	if notifyOnContextCancelled {
		go func() {
			<-ctx.Done()
			fnCallback(ReceiveResult[T]{
				Msg:    getZero[T](),
				Status: priority_channels.ReceiveContextCancelled,
			}, false)
		}()
	}
}

func CombineByFrequencyRatioWithCallbackEx[T any](ctx context.Context,
	channelsWithFreqRatios []ResultChannelWithFreqRatioEx[T], fnCallback func(ReceiveResultEx[T], bool), notifyOnContextCancelled bool) {
	var openChannelsNum atomic.Int32
	openChannelsNum.Store(int32(len(channelsWithFreqRatios)))
	for i := range channelsWithFreqRatios {
		var closeChannelOnce sync.Once
		for j := 0; j < channelsWithFreqRatios[i].FreqRatio(); j++ {
			go func(c ResultChannelWithFreqRatioEx[T], i int) {
				for {
					select {
					case <-ctx.Done():
						return
					case msg, ok := <-c.ResultChannel():
						if !ok {
							closeChannelOnce.Do(func() {
								fnCallback(ReceiveResultEx[T]{
									Msg: getZero[T](),
									ReceiveDetails: priority_channels.ReceiveDetails{
										ChannelName:  c.Name(),
										ChannelIndex: i,
									},
									Status: priority_channels.ReceiveChannelClosed,
								}, false)
								openChannelsNum.Add(-1)
								if openChannelsNum.Load() == 0 {
									fnCallback(ReceiveResultEx[T]{
										Msg:            getZero[T](),
										ReceiveDetails: priority_channels.ReceiveDetails{},
										Status:         priority_channels.ReceiveNoOpenChannels,
									}, true)
								}
							})
							return
						}
						fnCallback(getReceiveResultEx(
							msg.Msg, c.Name(), i,
							msg.ReceiveDetails,
							msg.Status), false)
						//if msg.Status != priority_channels.ReceiveSuccess {
						//	return
						//}
					}
				}
			}(channelsWithFreqRatios[i], i)
		}
	}
	if notifyOnContextCancelled {
		go func() {
			<-ctx.Done()
			fnCallback(ReceiveResultEx[T]{
				Msg:    getZero[T](),
				Status: priority_channels.ReceiveContextCancelled,
			}, false)
		}()
	}
}

func CombineByHighestAlwaysFirstEx[T any](ctx context.Context,
	resultChannelsWithPriority []ResultChannelWithPriorityEx[T]) (<-chan ReceiveResultEx[T], error) {
	channelsWithPriority := make([]channels.ChannelWithPriority[ReceiveResultEx[T]], 0, len(resultChannelsWithPriority))
	for _, resultChannelWithPriority := range resultChannelsWithPriority {
		channelsWithPriority = append(channelsWithPriority, channels.NewChannelWithPriority(
			resultChannelWithPriority.Name(),
			resultChannelWithPriority.ResultChannel(),
			resultChannelWithPriority.Priority()))
	}
	ch, err := priority_channels.NewByHighestAlwaysFirst(ctx, channelsWithPriority, priority_channels.AutoDisableClosedChannels())
	if err != nil {
		return nil, err
	}
	return doProcessPriorityChannelWithUnwrap(ctx, ch, getReceiveResultEx), nil
}

func ProcessChannel[T any](ctx context.Context, name string, c <-chan T) <-chan ReceiveResult[T] {
	return processChannel(ctx, name, c, getReceiveResult)
}

func ProcessChannelEx[T any](ctx context.Context, name string, c <-chan T) <-chan ReceiveResultEx[T] {
	return processChannel(ctx, name, c, getReceiveResultEx)
}

func processChannel[T any, R any](ctx context.Context, name string, c <-chan T, fnGetReceiveResult getReceiveResultFunc[T, R]) <-chan R {
	var resChannel = make(chan R, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				resChannel <- fnGetReceiveResult(
					getZero[T](), "", -1,
					priority_channels.ReceiveDetails{},
					priority_channels.ReceiveContextCancelled)
				close(resChannel)
				return
			case msg, ok := <-c:
				if !ok {
					resChannel <- fnGetReceiveResult(getZero[T](), "", -1,
						priority_channels.ReceiveDetails{ChannelName: name, ChannelIndex: 0},
						priority_channels.ReceiveChannelClosed)
					close(resChannel)
					return
				}
				resChannel <- fnGetReceiveResult(
					msg, "", -1,
					priority_channels.ReceiveDetails{ChannelName: name, ChannelIndex: 0},
					priority_channels.ReceiveSuccess)
			}
		}
	}()
	return resChannel
}

func ProcessPriorityChannel[T any](ctx context.Context, c *priority_channels.PriorityChannel[T]) <-chan ReceiveResult[T] {
	return processPriorityChannel(ctx, c, getReceiveResult)
}

func ProcessPriorityChannelEx[T any](ctx context.Context, c *priority_channels.PriorityChannel[T]) <-chan ReceiveResultEx[T] {
	return processPriorityChannel(ctx, c, getReceiveResultEx)
}

func processPriorityChannel[T any, R any](ctx context.Context, c *priority_channels.PriorityChannel[T],
	fnGetReceiveResult getReceiveResultFunc[T, R]) <-chan R {
	resChannel := make(chan R, 1)
	go func() {
		defer close(resChannel)
		for {
			msg, receiveDetails, status := c.ReceiveWithContextEx(ctx)
			if status == priority_channels.ReceiveContextCancelled && receiveDetails.ChannelName == "" {
				return
			}
			select {
			case <-ctx.Done():
				return
			case resChannel <- fnGetReceiveResult(msg, "", -1, receiveDetails, status):
			}
			if status != priority_channels.ReceiveSuccess {
				return
			}
		}
	}()
	return resChannel
}

func processPriorityChannelWithUnwrap[W ReceiveResulterEx[T], T any](ctx context.Context, c *priority_channels.PriorityChannel[W]) <-chan ReceiveResult[T] {
	return doProcessPriorityChannelWithUnwrap(ctx, c, getReceiveResult)
}

func processPriorityChannelWithUnwrapEx[W ReceiveResulterEx[T], T any](ctx context.Context, c *priority_channels.PriorityChannel[W]) <-chan ReceiveResultEx[T] {
	return doProcessPriorityChannelWithUnwrap(ctx, c, getReceiveResultEx)
}

func doProcessPriorityChannelWithUnwrap[W ReceiveResulterEx[T], T any, R any](ctx context.Context, c *priority_channels.PriorityChannel[W],
	fnGetReceiveResult getReceiveResultFunc[T, R]) <-chan R {
	resChannel := make(chan R, 1)
	go func() {
		defer close(resChannel)
		for {
			msg, receiveDetails, status := receiveUnwrappedEx(ctx, c)
			if status == priority_channels.ReceiveContextCancelled && receiveDetails.ChannelName == "" && len(receiveDetails.PathInTree) == 0 {
				return
			}
			select {
			case <-ctx.Done():
				return
			case resChannel <- fnGetReceiveResult(msg, "", -1, receiveDetails, status):
			}
			//if status != priority_channels.ReceiveSuccess {
			//	return
			//}
		}
	}()
	return resChannel
}

func receiveUnwrapped[R ReceiveResulter[T], T any](ctx context.Context, pc *priority_channels.PriorityChannel[R]) (msg T, channelName string, status priority_channels.ReceiveStatus) {
	result, channelName, status := pc.ReceiveWithContext(ctx)
	if status != priority_channels.ReceiveSuccess {
		return getZero[T](), channelName, status
	}
	return result.GetMsg(), result.GetChannelName(), result.GetStatus()
}

func receiveUnwrappedEx[R ReceiveResulterEx[T], T any](ctx context.Context, pc *priority_channels.PriorityChannel[R]) (msg T, details priority_channels.ReceiveDetails, status priority_channels.ReceiveStatus) {
	result, receiveDetails, status := pc.ReceiveWithContextEx(ctx)
	if status != priority_channels.ReceiveSuccess {
		return getZero[T](), receiveDetails, status
	}
	var combinedReceiveDetails priority_channels.ReceiveDetails
	if result.GetReceiveDetails().ChannelName == "" && len(result.GetReceiveDetails().PathInTree) == 0 {
		combinedReceiveDetails = priority_channels.ReceiveDetails{
			ChannelName:  receiveDetails.ChannelName,
			ChannelIndex: receiveDetails.ChannelIndex,
		}
	} else {
		combinedReceiveDetails = priority_channels.ReceiveDetails{
			ChannelName:  result.GetReceiveDetails().ChannelName,
			ChannelIndex: result.GetReceiveDetails().ChannelIndex,
			PathInTree: append(append(receiveDetails.PathInTree, priority_channels.ChannelNode{
				ChannelName:  receiveDetails.ChannelName,
				ChannelIndex: receiveDetails.ChannelIndex,
			}), result.GetReceiveDetails().PathInTree...),
		}
	}
	return result.GetMsg(), combinedReceiveDetails, result.GetStatus()
}

func getZero[T any]() T {
	var result T
	return result
}
