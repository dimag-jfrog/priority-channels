package priority_channels

import (
	"context"
	"reflect"
	"sort"
	"sync/atomic"
	"time"

	"github.com/dimag-jfrog/priority-channels/channels"
)

func NewByFrequencyRatio[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelFreqRatio[T],
	options ...func(*PriorityQueueOptions)) PriorityChannel[T] {
	return newPriorityChannelByFrequencyRatio[T](ctx, channelsWithFreqRatios, options...)
}

func (pc *priorityChannelsByFreq[T]) Receive() (msg T, channelName string, ok bool) {
	msg, channelName, status := pc.receiveSingleMessage(context.Background(), false)
	if status != ReceiveSuccess {
		return getZero[T](), channelName, false
	}
	return msg, channelName, true
}

func (pc *priorityChannelsByFreq[T]) ReceiveWithContext(ctx context.Context) (msg T, channelName string, status ReceiveStatus) {
	return pc.receiveSingleMessage(ctx, false)
}

func (pc *priorityChannelsByFreq[T]) ReceiveWithDefaultCase() (msg T, channelName string, status ReceiveStatus) {
	return pc.receiveSingleMessage(context.Background(), true)
}

func (pc *priorityChannelsByFreq[T]) Context() context.Context {
	return pc.ctx
}

type priorityBucket[T any] struct {
	Channel channels.ChannelFreqRatio[T]
	Value   int
}

func (pb *priorityBucket[T]) ChannelName() string {
	return pb.Channel.ChannelName()
}

func (pb *priorityBucket[T]) MsgsC() <-chan T {
	return pb.Channel.MsgsC()
}

func (pb *priorityBucket[T]) Capacity() int {
	return pb.Channel.FreqRatio()
}

type level[T any] struct {
	TotalValue    int
	TotalCapacity int
	Buckets       []*priorityBucket[T]
}

type FreqRatioOrderMode int

const (
	Unordered FreqRatioOrderMode = iota
	Ordered
)

type priorityChannelsByFreq[T any] struct {
	ctx                        context.Context
	levels                     []*level[T]
	totalBuckets               int
	isPreparing                atomic.Bool
	channelReceiveWaitInterval *time.Duration
	orderMode                  FreqRatioOrderMode
}

func newPriorityChannelByFrequencyRatio[T any](
	ctx context.Context,
	channelsWithFreqRatios []channels.ChannelFreqRatio[T],
	options ...func(*PriorityQueueOptions)) *priorityChannelsByFreq[T] {
	pqOptions := &PriorityQueueOptions{}
	for _, option := range options {
		option(pqOptions)
	}

	zeroLevel := &level[T]{}
	zeroLevel.Buckets = make([]*priorityBucket[T], 0, len(channelsWithFreqRatios))
	for _, q := range channelsWithFreqRatios {
		bucket := &priorityBucket[T]{
			Channel: q,
			Value:   0,
		}
		zeroLevel.Buckets = append(zeroLevel.Buckets, bucket)
		zeroLevel.TotalCapacity += bucket.Capacity()
	}
	sort.Slice(zeroLevel.Buckets, func(i int, j int) bool {
		return zeroLevel.Buckets[i].Capacity() > zeroLevel.Buckets[j].Capacity()
	})
	return &priorityChannelsByFreq[T]{
		ctx:                        ctx,
		levels:                     []*level[T]{zeroLevel},
		totalBuckets:               len(channelsWithFreqRatios),
		channelReceiveWaitInterval: pqOptions.channelReceiveWaitInterval,
		orderMode:                  pqOptions.freqRatioOrderMode,
	}
}

func ProcessMessagesByFrequencyRatio[T any](
	ctx context.Context,
	channelsWithFreqRatios []channels.ChannelFreqRatio[T],
	msgProcessor func(ctx context.Context, msg T, channelName string),
	options ...func(*PriorityQueueOptions)) ExitReason {
	pq := newPriorityChannelByFrequencyRatio(ctx, channelsWithFreqRatios, options...)
	return processPriorityChannelMessages[T](pq, msgProcessor)
}

func (pq *priorityChannelsByFreq[T]) receiveSingleMessage(ctx context.Context, withDefaultCase bool) (resMsg T, resChannelName string, resStatus ReceiveStatus) {
	pq.isPreparing.Store(true)
	defer pq.isPreparing.Store(false)

	lastNumberOfBucketsToProcess := pq.totalBuckets

	var msg T
	var channelName string
	var selectStatus ReceiveStatus
	if pq.orderMode == Unordered {
		currNumOfBucketsToProcess := 0
		for i := 0; i < len(pq.levels); i++ {
			currNumOfBucketsToProcess += len(pq.levels[i].Buckets)
			msg, channelName, selectStatus = pq.selectByNumOfBuckets(ctx, withDefaultCase, currNumOfBucketsToProcess, lastNumberOfBucketsToProcess)
			if selectStatus != ReceiveStatusUnknown {
				break
			}
		}
	} else {
		for currNumOfBucketsToProcess := 1; currNumOfBucketsToProcess <= lastNumberOfBucketsToProcess; currNumOfBucketsToProcess++ {
			msg, channelName, selectStatus = pq.selectByNumOfBuckets(ctx, withDefaultCase, currNumOfBucketsToProcess, lastNumberOfBucketsToProcess)
			if selectStatus != ReceiveStatusUnknown {
				break
			}
		}
	}
	return msg, channelName, selectStatus
}

func (pq *priorityChannelsByFreq[T]) selectByNumOfBuckets(ctx context.Context, withDefaultCase bool,
	currNumOfBucketsToProcess int,
	lastNumberOfBucketsToProcess int) (resMsg T, resChannelName string, resStatus ReceiveStatus) {
	chosen, recv, recvOk, selectStatus := selectCasesOfNextIteration(
		pq.ctx,
		ctx,
		pq.prepareSelectCases,
		currNumOfBucketsToProcess,
		lastNumberOfBucketsToProcess,
		withDefaultCase,
		&pq.isPreparing,
		pq.channelReceiveWaitInterval)
	if selectStatus != ReceiveSuccess {
		return getZero[T](), "", selectStatus
	}
	levelIndex, bucketIndex := pq.getLevelAndBucketIndexByChosenChannelIndex(chosen)
	chosenBucket := pq.levels[levelIndex].Buckets[bucketIndex]
	channelName := chosenBucket.ChannelName()
	if !recvOk {
		// no more messages in channel
		if c, ok := chosenBucket.Channel.(ChannelWithUnderlyingClosedChannelDetails); ok {
			underlyingChannelName, closeStatus := c.GetUnderlyingClosedChannelDetails()
			return getZero[T](), underlyingChannelName, closeStatus
		}
		return getZero[T](), channelName, ReceiveChannelClosed
	}
	// Message received successfully
	msg := recv.Interface().(T)
	pq.updateStateOnReceivingMessageToBucket(levelIndex, bucketIndex)
	return msg, channelName, ReceiveSuccess
}

func (pq *priorityChannelsByFreq[T]) prepareSelectCases(numOfBucketsToProcess int) []reflect.SelectCase {
	addedBuckets := 0
	selectCases := make([]reflect.SelectCase, 0, numOfBucketsToProcess)
	for _, level := range pq.levels {
		for _, b := range level.Buckets {
			waitForReadyStatus(b.Channel)
			selectCases = append(selectCases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(b.MsgsC()),
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
	if chosenBucket.Value == chosenBucket.Capacity() {
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
			return chosenLevel.Buckets[i].Capacity() > chosenLevel.Buckets[j].Capacity()
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
	nextLevel.TotalCapacity += chosenBucket.Capacity()
	chosenLevel.Buckets = append(chosenLevel.Buckets[:bucketIndex], chosenLevel.Buckets[bucketIndex+1:]...)
	i := sort.Search(len(nextLevel.Buckets), func(i int) bool {
		return nextLevel.Buckets[i].Capacity() < chosenBucket.Capacity()
	})
	nextLevel.Buckets = append(nextLevel.Buckets, &priorityBucket[T]{})
	copy(nextLevel.Buckets[i+1:], nextLevel.Buckets[i:])
	nextLevel.Buckets[i] = chosenBucket
}

func (pc *priorityChannelsByFreq[T]) IsReady() bool {
	return pc.isPreparing.Load() == false
}
