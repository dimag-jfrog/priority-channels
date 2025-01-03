package priority_channels

import (
	"context"
	"reflect"
	"sort"
	"time"

	"github.com/dimag-jfrog/priority-channels/channels"
)

func NewByFrequencyRatio[T any](ctx context.Context, channelsWithFreqRatios []channels.ChannelFreqRatio[T]) PriorityChannel[T] {
	return newPriorityChannelByFrequencyRatio[T](ctx, channelsWithFreqRatios)
}

func (pc *priorityChannelsByFreq[T]) Receive() (msg T, channelName string, ok bool) {
	msgReceived, noMoreMessages := pc.ReceiveSingleMessage(context.Background())
	if noMoreMessages != nil {
		return getZero[T](), "", false
	}
	return msgReceived.Msg, msgReceived.ChannelName, true
}

func (pc *priorityChannelsByFreq[T]) ReceiveContext(ctx context.Context) (msg T, channelName string, status ReceiveStatus) {
	msgReceived, noMoreMessages := pc.ReceiveSingleMessage(ctx)
	if noMoreMessages != nil {
		return getZero[T](), "", noMoreMessages.Reason.ReceiveStatus()
	}
	return msgReceived.Msg, msgReceived.ChannelName, ReceiveSuccess
}

type priorityBucket[T any] struct {
	ChannelName string
	Value       int
	Capacity    int
	MsgsC       <-chan T
}

type level[T any] struct {
	TotalValue    int
	TotalCapacity int
	Buckets       []*priorityBucket[T]
}

type priorityChannelsByFreq[T any] struct {
	ctx          context.Context
	levels       []*level[T]
	totalBuckets int
}

func newPriorityChannelByFrequencyRatio[T any](
	ctx context.Context,
	channelsWithFreqRatios []channels.ChannelFreqRatio[T]) *priorityChannelsByFreq[T] {
	zeroLevel := &level[T]{}
	zeroLevel.Buckets = make([]*priorityBucket[T], 0, len(channelsWithFreqRatios))
	for _, q := range channelsWithFreqRatios {
		zeroLevel.Buckets = append(zeroLevel.Buckets, &priorityBucket[T]{
			Value:       0,
			Capacity:    q.FreqRatio(),
			MsgsC:       q.MsgsC(),
			ChannelName: q.ChannelName(),
		})
		zeroLevel.TotalCapacity += q.FreqRatio()
	}
	sort.Slice(zeroLevel.Buckets, func(i int, j int) bool {
		return zeroLevel.Buckets[i].Capacity > zeroLevel.Buckets[j].Capacity
	})
	return &priorityChannelsByFreq[T]{
		ctx:          ctx,
		levels:       []*level[T]{zeroLevel},
		totalBuckets: len(channelsWithFreqRatios),
	}
}

func ProcessMessagesByFrequencyRatio[T any](
	ctx context.Context,
	channelsWithFreqRatios []channels.ChannelFreqRatio[T],
	msgProcessor func(ctx context.Context, msg T, channelName string)) ExitReason {
	pq := newPriorityChannelByFrequencyRatio(ctx, channelsWithFreqRatios)
	return processPriorityChannelMessages[T](ctx, pq, msgProcessor)
}

func (pq *priorityChannelsByFreq[T]) ReceiveSingleMessage(ctx context.Context) (msgReceived *msgReceivedEvent[T], noMoreMessages *noMoreMessagesEvent) {
	for numOfBucketsToProcess := 1; numOfBucketsToProcess <= pq.totalBuckets; numOfBucketsToProcess++ {
		selectCases := pq.prepareSelectCases(ctx, numOfBucketsToProcess)
		chosen, recv, recvOk := reflect.Select(selectCases)
		if chosen == 0 {
			// context of the priority channel is done
			return nil, &noMoreMessagesEvent{Reason: ChannelClosed}
		}
		if chosen == 1 {
			// context of the specific request is done
			return nil, &noMoreMessagesEvent{Reason: ContextCancelled}
		}
		isLastIteration := numOfBucketsToProcess == pq.totalBuckets
		if !isLastIteration && chosen == len(selectCases)-1 {
			// Default case - go to next iteration to increase the range of allowed minimal priority channels
			// on last iteration - blocking wait on all receive channels without default case
			continue
		}
		if !recvOk {
			// no more messages in channel
			return nil, &noMoreMessagesEvent{Reason: ChannelClosed}
		}
		// Message received successfully
		msg := recv.Interface().(T)
		levelIndex, bucketIndex := pq.getLevelAndBucketIndexByChosenChannelIndex(chosen)
		chosenBucket := pq.levels[levelIndex].Buckets[bucketIndex]
		res := &msgReceivedEvent[T]{Msg: msg, ChannelName: chosenBucket.ChannelName}
		pq.updateStateOnReceivingMessageToBucket(levelIndex, bucketIndex)
		return res, nil
	}
	return nil, nil
}

func (pq *priorityChannelsByFreq[T]) prepareSelectCases(ctx context.Context, numOfBucketsToProcess int) []reflect.SelectCase {
	addedBuckets := 0
	selectCases := make([]reflect.SelectCase, 0, numOfBucketsToProcess+2)
	selectCases = append(selectCases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(pq.ctx.Done()),
	})
	selectCases = append(selectCases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	})
	for _, level := range pq.levels {
		for _, b := range level.Buckets {
			selectCases = append(selectCases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(b.MsgsC),
			})
			addedBuckets++
			if addedBuckets == numOfBucketsToProcess {
				break
			}
		}
		if addedBuckets == numOfBucketsToProcess {
			break
		}
	}
	isLastIteration := numOfBucketsToProcess == pq.totalBuckets
	if !isLastIteration {
		selectCases = append(selectCases, reflect.SelectCase{
			// The default option without any sleep did not pass tests
			// short sleep is needed to guarantee that we do not enter default case when there are still messages
			// in the deliveries channel that can be retrieved
			//Dir: reflect.SelectDefault,
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(time.After(100 * time.Microsecond)),
		})
	}
	return selectCases
}

func (pq *priorityChannelsByFreq[T]) getLevelAndBucketIndexByChosenChannelIndex(chosen int) (levelIndex int, bucketIndex int) {
	currIndex := 2
	for i := range pq.levels {
		for j := range pq.levels[i].Buckets {
			if currIndex == chosen {
				return i, j
			}
			currIndex++
		}
	}
	return -1, -1
}

func (pq *priorityChannelsByFreq[T]) updateStateOnReceivingMessageToBucket(levelIndex int, bucketIndex int) {
	chosenLevel := pq.levels[levelIndex]
	chosenBucket := chosenLevel.Buckets[bucketIndex]
	chosenBucket.Value++
	chosenLevel.TotalValue++

	if chosenLevel.TotalValue == chosenLevel.TotalCapacity {
		pq.mergeAllNextLevelsBackIntoCurrentLevel(levelIndex)
		return
	}
	if chosenBucket.Value == chosenBucket.Capacity {
		pq.moveBucketToNextLevel(levelIndex, bucketIndex)
		return
	}
}

func (pq *priorityChannelsByFreq[T]) mergeAllNextLevelsBackIntoCurrentLevel(levelIndex int) {
	chosenLevel := pq.levels[levelIndex]
	if levelIndex < len(pq.levels)-1 {
		for nextLevelIndex := levelIndex + 1; nextLevelIndex <= len(pq.levels)-1; nextLevelIndex++ {
			nextLevel := pq.levels[nextLevelIndex]
			chosenLevel.Buckets = append(chosenLevel.Buckets, nextLevel.Buckets...)
		}
		sort.Slice(chosenLevel.Buckets, func(i int, j int) bool {
			return chosenLevel.Buckets[i].Capacity > chosenLevel.Buckets[j].Capacity
		})
		pq.levels = pq.levels[0 : levelIndex+1]
	}
	chosenLevel.TotalValue = 0
	for i := range chosenLevel.Buckets {
		chosenLevel.Buckets[i].Value = 0
	}
}

func (pq *priorityChannelsByFreq[T]) moveBucketToNextLevel(levelIndex int, bucketIndex int) {
	chosenLevel := pq.levels[levelIndex]
	chosenBucket := chosenLevel.Buckets[bucketIndex]
	chosenBucket.Value = 0
	if len(chosenLevel.Buckets) == 1 {
		// if this bucket is the only one on its level - no need to move it to next level
		chosenLevel.TotalValue = 0
		return
	}
	if levelIndex == len(pq.levels)-1 {
		pq.levels = append(pq.levels, &level[T]{})
	}
	nextLevel := pq.levels[levelIndex+1]
	nextLevel.TotalCapacity += chosenBucket.Capacity
	chosenLevel.Buckets = append(chosenLevel.Buckets[:bucketIndex], chosenLevel.Buckets[bucketIndex+1:]...)
	i := sort.Search(len(nextLevel.Buckets), func(i int) bool {
		return nextLevel.Buckets[i].Capacity < chosenBucket.Capacity
	})
	nextLevel.Buckets = append(nextLevel.Buckets, &priorityBucket[T]{})
	copy(nextLevel.Buckets[i+1:], nextLevel.Buckets[i:])
	nextLevel.Buckets[i] = chosenBucket
}
