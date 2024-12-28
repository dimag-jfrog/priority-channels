# priority-channels
Process Go channels by priority. 


The following use cases are supported:

Main use cases:
- **Highest priority always first** - when we always want to process messages in order of priority
- **Processing by frequency ratio** - when we want to prevent starvation of lower priority messages

Combinations of main use cases - channel groups:
- Channel groups by highest priority first inside group and choose among groups by frequency ratio
- Channel groups by frequency ratio inside group and choose among groups by highest priority first
- Channel groups by frequency ratio inside group and choose among groups by frequency ratio


## Usage

### Full example

```go

package main

import (
	"context"
	"fmt"
	"time"
	
	"github.com/dimag-jfrog/priority-channels"
	"github.com/dimag-jfrog/priority-channels/channels-groups"
)

type UsagePattern int

const (
	HighestPriorityAlwaysFirst UsagePattern = iota
	FrequencyRatioForAll
	PayingCustomerAlwaysFirst_NoStarvationOfLowMessagesForSameUser
	NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirstForSameUser
	FrequencyRatioBetweenUsers_AndFreqRatioBetweenMessagesForSameUser
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	payingCustomerHighPriorityC := make(chan string)
	payingCustomerLowPriorityC := make(chan string)
	freeUserHighPriorityC := make(chan string)
	freeUserLowPriorityC := make(chan string)

	ch := getPriorityChannelByUsagePattern(
		ctx,
		HighestPriorityAlwaysFirst,
		payingCustomerHighPriorityC,
		payingCustomerLowPriorityC,
		freeUserHighPriorityC,
		freeUserLowPriorityC)
	if ch == nil {
		cancel()
		return
	}

	// sending messages to individual channels
	go func() {
		for i := 1; i <= 20; i++ {
			payingCustomerHighPriorityC <- fmt.Sprintf("high priority message %d", i)
		}
	}()
	go func() {
		for i := 1; i <= 20; i++ {
			payingCustomerLowPriorityC <- fmt.Sprintf("low priority message %d", i)
		}
	}()
	go func() {
		for i := 1; i <= 20; i++ {
			freeUserHighPriorityC <- fmt.Sprintf("high priority message %d", i)
		}
	}()
	go func() {
		for i := 1; i <= 20; i++ {
			freeUserLowPriorityC <- fmt.Sprintf("low priority message %d", i)
		}
	}()

	go func() {
		time.Sleep(3 * time.Second)
		cancel()
	}()

	// receiving messages from the priority channel
	for {
		message, channelName, ok := ch.Receive(ctx)
		if !ok {
			break
		}
		fmt.Printf("%s: %s\n", channelName, message)
		time.Sleep(10 * time.Millisecond)
	}
}

func getPriorityChannelByUsagePattern(
	ctx context.Context,
	usage UsagePattern,
	payingCustomerHighPriorityC chan string,
	payingCustomerLowPriorityC chan string,
	freeUserHighPriorityC chan string,
	freeUserLowPriorityC chan string,
) priority_channels.PriorityChannel[string] {

	switch usage {

	case HighestPriorityAlwaysFirst:
		channelsWithPriority := []priority_channels.ChannelWithPriority[string]{
			priority_channels.NewChannelWithPriority(
				"Paying Customer - High Priority",
				payingCustomerHighPriorityC,
				10),
			priority_channels.NewChannelWithPriority(
				"Paying Customer - Low Priority",
				payingCustomerLowPriorityC,
				6),
			priority_channels.NewChannelWithPriority(
				"Free User - High Priority",
				freeUserHighPriorityC,
				5),
			priority_channels.NewChannelWithPriority(
				"Free User - Low Priority",
				freeUserLowPriorityC,
				1),
		}
		return priority_channels.NewWithHighestAlwaysFirst[string](channelsWithPriority)

	case FrequencyRatioForAll:
		channelsWithFrequencyRatio := []priority_channels.ChannelFreqRatio[string]{
			priority_channels.NewChannelWithFreqRatio(
				"Paying Customer - High Priority",
				payingCustomerHighPriorityC,
				50),
			priority_channels.NewChannelWithFreqRatio(
				"Paying Customer - Low Priority",
				payingCustomerLowPriorityC,
				10),
			priority_channels.NewChannelWithFreqRatio(
				"Free User - High Priority",
				freeUserHighPriorityC,
				5),
			priority_channels.NewChannelWithFreqRatio(
				"Free User - Low Priority",
				freeUserLowPriorityC,
				1),
		}
		return priority_channels.NewWithFrequencyRatio[string](channelsWithFrequencyRatio)

	case PayingCustomerAlwaysFirst_NoStarvationOfLowMessagesForSameUser:
		channelsWithPriority := []channel_groups.PriorityChannelGroupWithFreqRatio[string]{
			{
				ChannelsWithFreqRatios: []priority_channels.ChannelFreqRatio[string]{
					priority_channels.NewChannelWithFreqRatio(
						"Paying Customer - High Priority",
						payingCustomerHighPriorityC,
						5),
					priority_channels.NewChannelWithFreqRatio(
						"Paying Customer - Low Priority",
						payingCustomerLowPriorityC,
						1),
				},
				Priority: 10,
			},
			{
				ChannelsWithFreqRatios: []priority_channels.ChannelFreqRatio[string]{
					priority_channels.NewChannelWithFreqRatio(
						"Free User - High Priority",
						freeUserHighPriorityC,
						5),
					priority_channels.NewChannelWithFreqRatio(
						"Free User - Low Priority",
						freeUserLowPriorityC,
						1),
				},
				Priority: 1,
			},
		}
		return channel_groups.NewByHighestPriorityFirstAmongFreqRatioChannelGroups[string](ctx, channelsWithPriority)

	case NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirstForSameUser:
		channelsWithFreqRatio := []channel_groups.ChannelGroupWithHighestPriorityFirst[string]{
			{
				ChannelsWithPriority: []priority_channels.ChannelWithPriority[string]{
					priority_channels.NewChannelWithPriority(
						"Paying Customer - High Priority",
						payingCustomerHighPriorityC,
						5),
					priority_channels.NewChannelWithPriority(
						"Paying Customer - Low Priority",
						payingCustomerLowPriorityC,
						1),
				},
				FreqRatio: 10,
			},
			{
				ChannelsWithPriority: []priority_channels.ChannelWithPriority[string]{
					priority_channels.NewChannelWithPriority(
						"Free User - High Priority",
						freeUserHighPriorityC,
						5),
					priority_channels.NewChannelWithPriority(
						"Free User - Low Priority",
						freeUserLowPriorityC,
						1),
				},
				FreqRatio: 1,
			},
		}
		return channel_groups.NewByFreqRatioAmongHighestPriorityFirstChannelGroups[string](ctx, channelsWithFreqRatio)

	case FrequencyRatioBetweenUsers_AndFreqRatioBetweenMessagesForSameUser:
		channelsWithFreqRatio := []channel_groups.FreqRatioChannelGroupWithFreqRatio[string]{
			{
				ChannelsWithFreqRatios: []priority_channels.ChannelFreqRatio[string]{
					priority_channels.NewChannelWithFreqRatio(
						"Paying Customer - High Priority",
						payingCustomerHighPriorityC,
						5),
					priority_channels.NewChannelWithFreqRatio(
						"Paying Customer - Low Priority",
						payingCustomerLowPriorityC,
						1),
				},
				FreqRatio: 10,
			},
			{
				ChannelsWithFreqRatios: []priority_channels.ChannelFreqRatio[string]{
					priority_channels.NewChannelWithFreqRatio(
						"Free User - High Priority",
						freeUserHighPriorityC,
						5),
					priority_channels.NewChannelWithFreqRatio(
						"Free User - Low Priority",
						freeUserLowPriorityC,
						1),
				},
				FreqRatio: 1,
			},
		}
		return channel_groups.NewByFreqRatioAmongFreqRatioChannelGroups[string](ctx, channelsWithFreqRatio)

	default:
		return nil
	}
}
```
