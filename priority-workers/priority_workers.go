package priority_workers

import (
	"context"
	"sync"

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

func processWithCallbackToChannel[R any](fnProcessWithCallback func(func(r R), func()) context.CancelFunc) (<-chan R, context.CancelFunc) {
	resChannel := make(chan R, 1)
	senderChannel := make(chan R)
	closeChannel := make(chan struct{})
	fnCallback := func(r R) {
		senderChannel <- r
	}
	fnClose := func() {
		close(closeChannel)
	}
	cancelFunc := fnProcessWithCallback(fnCallback, fnClose)
	go func() {
		defer close(resChannel)
		for {
			select {
			case <-closeChannel:
				return
			case msg := <-senderChannel:
				resChannel <- msg
			}
		}
	}()
	return resChannel, cancelFunc
}

func ProcessByFrequencyRatio[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T]) (<-chan ReceiveResult[T], context.CancelFunc) {
	return processByFrequencyRatio(ctx, channelsWithFreqRatios, getReceiveResult)
}

func ProcessByFrequencyRatioEx[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T]) (<-chan ReceiveResultEx[T], context.CancelFunc) {
	return processByFrequencyRatio(ctx, channelsWithFreqRatios, getReceiveResultEx)
}

func processByFrequencyRatio[T any, R any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T], fnGetReceiveResult getReceiveResultFunc[T, R]) (<-chan R, context.CancelFunc) {
	return processWithCallbackToChannel(func(fnCallback func(r R), fnClose func()) context.CancelFunc {
		return processByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, fnClose, fnGetReceiveResult)
	})
}

func ProcessByFrequencyRatioWithCallback[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T], fnCallback func(ReceiveResult[T])) {
	processByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, nil, getReceiveResult)
}

func ProcessByFrequencyRatioWithCallbackEx[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T], fnCallback func(ReceiveResultEx[T])) {
	processByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, nil, getReceiveResultEx)
}

func processByFrequencyRatioWithCallback[T any, R any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T],
	fnCallback func(R),
	fnClose func(),
	fnGetReceiveResult getReceiveResultFunc[T, R]) context.CancelFunc {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i := range channelsWithFreqRatios {
		var closeChannelOnce sync.Once
		for j := 0; j < channelsWithFreqRatios[i].FreqRatio(); j++ {
			wg.Add(1)
			go func(c channels.ChannelWithFreqRatio[T], i int) {
				defer wg.Done()
				for {
					select {
					case <-ctxWithCancel.Done():
						// don't receive any new messages after the context is cancelled
						return
					case msg, ok := <-c.MsgsC():
						if !ok {
							closeChannelOnce.Do(func() {
								fnCallback(fnGetReceiveResult(getZero[T](), "", -1,
									priority_channels.ReceiveDetails{ChannelName: c.ChannelName(), ChannelIndex: i},
									priority_channels.ReceiveChannelClosed))
							})
							return
						}
						fnCallback(fnGetReceiveResult(
							msg, "", -1,
							priority_channels.ReceiveDetails{ChannelName: c.ChannelName(), ChannelIndex: i},
							priority_channels.ReceiveSuccess))
					}
				}
			}(channelsWithFreqRatios[i], i)
		}
	}
	go func() {
		wg.Wait()
		select {
		case <-ctxWithCancel.Done():
			fnCallback(fnGetReceiveResult(
				getZero[T](), "", -1,
				priority_channels.ReceiveDetails{},
				priority_channels.ReceiveContextCancelled))
		default:
			fnCallback(fnGetReceiveResult(getZero[T](), "", -1,
				priority_channels.ReceiveDetails{},
				priority_channels.ReceiveNoOpenChannels))
		}
		if fnClose != nil {
			fnClose()
		}
	}()
	return cancel
}

func ProcessPriorityChannelsByFrequencyRatio[T any](ctx context.Context,
	channelsWithFreqRatios []priority_channels.PriorityChannelWithFreqRatio[T]) (<-chan ReceiveResult[T], context.CancelFunc) {
	return processPriorityChannelsByFrequencyRatio(ctx, channelsWithFreqRatios, getReceiveResult)
}

func ProcessPriorityChannelsByFrequencyRatioEx[T any](ctx context.Context,
	channelsWithFreqRatios []priority_channels.PriorityChannelWithFreqRatio[T]) (<-chan ReceiveResultEx[T], context.CancelFunc) {
	return processPriorityChannelsByFrequencyRatio(ctx, channelsWithFreqRatios, getReceiveResultEx)
}

func processPriorityChannelsByFrequencyRatio[T any, R any](ctx context.Context,
	channelsWithFreqRatios []priority_channels.PriorityChannelWithFreqRatio[T],
	fnGetReceiveResult getReceiveResultFunc[T, R]) (<-chan R, context.CancelFunc) {
	return processWithCallbackToChannel(func(fnCallback func(r R), fnClose func()) context.CancelFunc {
		return processPriorityChannelsByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, fnClose, fnGetReceiveResult)
	})
}

func ProcessPriorityChannelsByFrequencyRatioWithCallback[T any](ctx context.Context,
	channelsWithFreqRatios []priority_channels.PriorityChannelWithFreqRatio[T], fnCallback func(ReceiveResult[T])) {
	processPriorityChannelsByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, nil, getReceiveResult)
}

func ProcessPriorityChannelsByFrequencyRatioWithCallbackEx[T any](ctx context.Context,
	channelsWithFreqRatios []priority_channels.PriorityChannelWithFreqRatio[T], fnCallback func(ReceiveResultEx[T])) {
	processPriorityChannelsByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, nil, getReceiveResultEx)
}

func processPriorityChannelsByFrequencyRatioWithCallback[T any, R any](ctx context.Context,
	channelsWithFreqRatios []priority_channels.PriorityChannelWithFreqRatio[T], fnCallback func(R), fnClose func(),
	fnGetReceiveResult getReceiveResultFunc[T, R]) context.CancelFunc {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i := range channelsWithFreqRatios {
		var closeChannelOnce sync.Once
		for j := 0; j < channelsWithFreqRatios[i].FreqRatio(); j++ {
			wg.Add(1)
			go func(c priority_channels.PriorityChannelWithFreqRatio[T], i int) {
				defer wg.Done()
				for {
					msg, receiveDetails, status := c.PriorityChannel().ReceiveWithContextEx(ctxWithCancel)
					if status == priority_channels.ReceiveContextCancelled {
						return
					} else if status == priority_channels.ReceivePriorityChannelClosed {
						closeChannelOnce.Do(func() {
							fnCallback(fnGetReceiveResult(getZero[T](), "", -1,
								priority_channels.ReceiveDetails{ChannelName: c.Name(), ChannelIndex: i},
								priority_channels.ReceivePriorityChannelClosed))
						})
						return
					}
					fnCallback(fnGetReceiveResult(msg, c.Name(), i,
						receiveDetails,
						status))
				}
			}(channelsWithFreqRatios[i], i)
		}
	}
	go func() {
		wg.Wait()
		select {
		case <-ctxWithCancel.Done():
			fnCallback(fnGetReceiveResult(getZero[T](), "", -1,
				priority_channels.ReceiveDetails{},
				priority_channels.ReceiveContextCancelled))
		default:
			fnCallback(fnGetReceiveResult(getZero[T](), "", -1,
				priority_channels.ReceiveDetails{},
				priority_channels.ReceiveNoOpenChannels))

		}
		if fnClose != nil {
			fnClose()
		}
	}()
	return cancel
}

func CombineByFrequencyRatio[T any](ctx context.Context,
	channelsWithFreqRatios []ResultChannelWithFreqRatio[T]) (<-chan ReceiveResult[T], context.CancelFunc) {
	return processWithCallbackToChannel(func(fnCallback func(r ReceiveResult[T]), fnClose func()) context.CancelFunc {
		return combineByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, fnClose)
	})
}

func CombineByFrequencyRatioEx[T any](ctx context.Context,
	channelsWithFreqRatios []ResultChannelWithFreqRatioEx[T]) (<-chan ReceiveResultEx[T], context.CancelFunc) {
	return processWithCallbackToChannel(func(fnCallback func(r ReceiveResultEx[T]), fnClose func()) context.CancelFunc {
		return combineByFrequencyRatioWithCallbackEx(ctx, channelsWithFreqRatios, fnCallback, fnClose)
	})
}

func CombineByFrequencyRatioWithCallback[T any](ctx context.Context,
	channelsWithFreqRatios []ResultChannelWithFreqRatio[T], fnCallback func(ReceiveResult[T])) context.CancelFunc {
	return combineByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, nil)
}

func combineByFrequencyRatioWithCallback[T any](ctx context.Context,
	channelsWithFreqRatios []ResultChannelWithFreqRatio[T], fnCallback func(ReceiveResult[T]), fnClose func()) context.CancelFunc {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i := range channelsWithFreqRatios {
		var closeChannelOnce sync.Once
		var closeUnderlyingChannelsOnce sync.Once
		for j := 0; j < channelsWithFreqRatios[i].FreqRatio(); j++ {
			wg.Add(1)
			go func(c ResultChannelWithFreqRatio[T]) {
				defer wg.Done()
				for {
					select {
					case <-ctxWithCancel.Done():
						closeUnderlyingChannelsOnce.Do(func() {
							for _, c := range channelsWithFreqRatios {
								c.Cancel()
							}
						})
						// read all remaining messages from the channel
						for msg := range c.ResultChannel() {
							if msg.Status != priority_channels.ReceiveSuccess {
								// after channel cancellation - call callback only for successful messages of underlying channels
								continue
							}
							fnCallback(ReceiveResult[T]{
								Msg:         msg.Msg,
								ChannelName: msg.ChannelName,
								Status:      msg.Status,
							})
						}
						return

					case msg, ok := <-c.ResultChannel():
						if !ok {
							closeChannelOnce.Do(func() {
								fnCallback(ReceiveResult[T]{
									Msg:         getZero[T](),
									ChannelName: c.Name(),
									Status:      priority_channels.ReceivePriorityChannelClosed,
								})
							})
							return
						}
						fnCallback(ReceiveResult[T]{
							Msg:         msg.Msg,
							ChannelName: msg.ChannelName,
							Status:      msg.Status,
						})
					}
				}
			}(channelsWithFreqRatios[i])
		}
	}
	go func() {
		wg.Wait()
		select {
		case <-ctxWithCancel.Done():
			fnCallback(ReceiveResult[T]{
				Msg:    getZero[T](),
				Status: priority_channels.ReceiveContextCancelled,
			})
		default:
			fnCallback(ReceiveResult[T]{
				Msg:         getZero[T](),
				ChannelName: "",
				Status:      priority_channels.ReceiveNoOpenChannels,
			})
		}
		if fnClose != nil {
			fnClose()
		}
	}()
	return cancel
}

func CombineByFrequencyRatioWithCallbackEx[T any](ctx context.Context,
	channelsWithFreqRatios []ResultChannelWithFreqRatioEx[T], fnCallback func(ReceiveResultEx[T])) context.CancelFunc {
	return combineByFrequencyRatioWithCallbackEx(ctx, channelsWithFreqRatios, fnCallback, nil)
}

func combineByFrequencyRatioWithCallbackEx[T any](ctx context.Context,
	channelsWithFreqRatios []ResultChannelWithFreqRatioEx[T], fnCallback func(ReceiveResultEx[T]), fnClose func()) context.CancelFunc {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i := range channelsWithFreqRatios {
		var closeChannelOnce sync.Once
		var closeUnderlyingChannelsOnce sync.Once
		for j := 0; j < channelsWithFreqRatios[i].FreqRatio(); j++ {
			wg.Add(1)
			go func(c ResultChannelWithFreqRatioEx[T], i int) {
				defer wg.Done()
				for {
					select {
					case <-ctxWithCancel.Done():
						closeUnderlyingChannelsOnce.Do(func() {
							for _, c := range channelsWithFreqRatios {
								c.Cancel()
							}
						})
						// read all remaining messages from the channel
						for msg := range c.ResultChannel() {
							if msg.Status != priority_channels.ReceiveSuccess {
								// after channel cancellation - call callback only for successful messages of underlying channels
								continue
							}
							fnCallback(getReceiveResultEx(
								msg.Msg, c.Name(), i,
								msg.ReceiveDetails,
								msg.Status))
						}
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
								})
							})
							return
						}
						fnCallback(getReceiveResultEx(
							msg.Msg, c.Name(), i,
							msg.ReceiveDetails,
							msg.Status))
					}
				}
			}(channelsWithFreqRatios[i], i)
		}
	}
	go func() {
		wg.Wait()
		select {
		case <-ctxWithCancel.Done():
			fnCallback(ReceiveResultEx[T]{
				Msg:    getZero[T](),
				Status: priority_channels.ReceiveContextCancelled,
			})
		default:
			fnCallback(ReceiveResultEx[T]{
				Msg:            getZero[T](),
				ReceiveDetails: priority_channels.ReceiveDetails{},
				Status:         priority_channels.ReceiveNoOpenChannels,
			})
		}
		if fnClose != nil {
			fnClose()
		}
	}()
	return cancel
}

func CombineByHighestAlwaysFirstEx[T any](ctx context.Context,
	resultChannelsWithPriority []ResultChannelWithPriorityEx[T]) (<-chan ReceiveResultEx[T], context.CancelFunc, error) {
	channelsWithPriority := make([]channels.ChannelWithPriority[ReceiveResultEx[T]], 0, len(resultChannelsWithPriority))
	for _, resultChannelWithPriority := range resultChannelsWithPriority {
		channelsWithPriority = append(channelsWithPriority, channels.NewChannelWithPriority(
			resultChannelWithPriority.Name(),
			resultChannelWithPriority.ResultChannel(),
			resultChannelWithPriority.Priority()))
	}
	ch, err := priority_channels.NewByHighestAlwaysFirst(context.Background(), channelsWithPriority, priority_channels.AutoDisableClosedChannels())
	if err != nil {
		return nil, nil, err
	}
	cancelUnderlyingChannelsFunc := func() {
		for _, resultChannelWithPriority := range resultChannelsWithPriority {
			resultChannelWithPriority.Cancel()
		}
	}
	wrappedCh, cancel := doProcessPriorityChannelWithUnwrap(ctx, ch, getReceiveResultEx, cancelUnderlyingChannelsFunc)
	return wrappedCh, cancel, nil
}

func ProcessChannel[T any](ctx context.Context, name string, c <-chan T) (<-chan ReceiveResult[T], context.CancelFunc) {
	return processChannel(ctx, name, c, getReceiveResult)
}

func ProcessChannelEx[T any](ctx context.Context, name string, c <-chan T) (<-chan ReceiveResultEx[T], context.CancelFunc) {
	return processChannel(ctx, name, c, getReceiveResultEx)
}

func processChannel[T any, R any](ctx context.Context, name string, c <-chan T, fnGetReceiveResult getReceiveResultFunc[T, R]) (<-chan R, context.CancelFunc) {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	var resChannel = make(chan R, 1)
	go func() {
		defer close(resChannel)
		for {
			select {
			case <-ctxWithCancel.Done():
				resChannel <- fnGetReceiveResult(
					getZero[T](), "", -1,
					priority_channels.ReceiveDetails{},
					priority_channels.ReceiveContextCancelled)
				return
			case msg, ok := <-c:
				if !ok {
					resChannel <- fnGetReceiveResult(getZero[T](), "", -1,
						priority_channels.ReceiveDetails{ChannelName: name, ChannelIndex: 0},
						priority_channels.ReceiveChannelClosed)
					return
				}
				resChannel <- fnGetReceiveResult(
					msg, "", -1,
					priority_channels.ReceiveDetails{ChannelName: name, ChannelIndex: 0},
					priority_channels.ReceiveSuccess)
			}
		}
	}()
	return resChannel, cancel
}

func ProcessPriorityChannel[T any](ctx context.Context, c *priority_channels.PriorityChannel[T]) (<-chan ReceiveResult[T], context.CancelFunc) {
	return processPriorityChannel(ctx, c, getReceiveResult)
}

func ProcessPriorityChannelEx[T any](ctx context.Context, c *priority_channels.PriorityChannel[T]) (<-chan ReceiveResultEx[T], context.CancelFunc) {
	return processPriorityChannel(ctx, c, getReceiveResultEx)
}

func processPriorityChannel[T any, R any](ctx context.Context, c *priority_channels.PriorityChannel[T],
	fnGetReceiveResult getReceiveResultFunc[T, R]) (<-chan R, context.CancelFunc) {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	resChannel := make(chan R, 1)
	go func() {
		defer close(resChannel)
		for {
			msg, receiveDetails, status := c.ReceiveWithContextEx(ctxWithCancel)
			if status == priority_channels.ReceiveContextCancelled && receiveDetails.ChannelName == "" {
				return
			}
			resChannel <- fnGetReceiveResult(msg, "", -1, receiveDetails, status)
			//select {
			//case <-ctxWithCancel.Done():
			//	return
			//case resChannel <- fnGetReceiveResult(msg, "", -1, receiveDetails, status):
			//}
		}
	}()
	return resChannel, cancel
}

func processPriorityChannelWithUnwrap[W ReceiveResulterEx[T], T any](ctx context.Context, c *priority_channels.PriorityChannel[W],
	cancelUnderlyingChannelsFunc context.CancelFunc) (<-chan ReceiveResult[T], context.CancelFunc) {
	return doProcessPriorityChannelWithUnwrap(ctx, c, getReceiveResult, cancelUnderlyingChannelsFunc)
}

func processPriorityChannelWithUnwrapEx[W ReceiveResulterEx[T], T any](ctx context.Context, c *priority_channels.PriorityChannel[W], cancelUnderlyingChannelsFunc context.CancelFunc) (<-chan ReceiveResultEx[T], context.CancelFunc) {
	return doProcessPriorityChannelWithUnwrap(ctx, c, getReceiveResultEx, cancelUnderlyingChannelsFunc)
}

func doProcessPriorityChannelWithUnwrap[W ReceiveResulterEx[T], T any, R any](ctx context.Context, c *priority_channels.PriorityChannel[W],
	fnGetReceiveResult getReceiveResultFunc[T, R],
	cancelUnderlyingChannelsFunc context.CancelFunc) (<-chan R, context.CancelFunc) {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	resChannel := make(chan R, 1)
	go func() {
		defer close(resChannel)
		for {
			msg, receiveDetails, status := receiveUnwrappedEx(ctxWithCancel, c)
			if status == priority_channels.ReceiveContextCancelled && receiveDetails.ChannelName == "" && len(receiveDetails.PathInTree) == 0 {
				cancelUnderlyingChannelsFunc()
				// read all remaining messages from the channel until all underlying channels are closed
				for {
					msg, receiveDetails, status = receiveUnwrappedEx(context.Background(), c)
					if status != priority_channels.ReceiveSuccess &&
						(receiveDetails.ChannelName != "" || len(receiveDetails.PathInTree) != 0) {
						// after channel cancellation - call callback only for successful messages of underlying channels
						continue
					}
					resChannel <- fnGetReceiveResult(msg, "", -1, receiveDetails, status)
					if status == priority_channels.ReceiveNoOpenChannels {
						break
					}
				}
				return
			}
			resChannel <- fnGetReceiveResult(msg, "", -1, receiveDetails, status)
			//select {
			//case <-ctxWithCancel.Done():
			//	return
			//case resChannel <- fnGetReceiveResult(msg, "", -1, receiveDetails, status):
			//}
		}
	}()
	return resChannel, cancel
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
