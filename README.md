# priority-channels
Process Go channels by priority. 


The following use cases are supported:

- Highest priority always first - when we always want to process messages in order of priority
- Processing by frequency ratio - when we want to prevent starvation of lower priority messages
- Channel groups by highest priority first inside group and choose among groups by frequency ratio
- Channel groups by frequency ratio inside group and choose among groups by highest priority first


## Usage

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
)

func main() {
	ctx := context.Background()
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
		close(payingCustomerHighPriorityC)
		close(payingCustomerLowPriorityC)
		close(freeUserHighPriorityC)
		close(freeUserLowPriorityC)
	}()

	// receiving messages from the priority channel
	for {
		message, channelName, ok := ch.Receive(ctx)
		if !ok {
			break
		}
		fmt.Printf("%s: %s\n", channelName, message)
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
			{
				ChannelName: "Paying Customer - High Priority",
				MsgsC:       payingCustomerHighPriorityC,
				Priority:    10,
			},
			{
				ChannelName: "Paying Customer - Low Priority",
				MsgsC:       payingCustomerLowPriorityC,
				Priority:    6,
			},
			{
				ChannelName: "Free User - High Priority",
				MsgsC:       freeUserHighPriorityC,
				Priority:    5,
			},
			{
				ChannelName: "Free User - Low Priority",
				MsgsC:       freeUserLowPriorityC,
				Priority:    1,
			},
		}
		return priority_channels.NewWithHighestAlwaysFirst[string](channelsWithPriority)
		
	case FrequencyRatioForAll:
		channelsWithFrequencyRatio := []priority_channels.ChannelFreqRatio[string]{
			{
				ChannelName: "Paying Customer - High Priority",
				MsgsC:       payingCustomerHighPriorityC,
				FreqRatio:   50,
			},
			{
				ChannelName: "Paying Customer - Low Priority",
				MsgsC:       payingCustomerLowPriorityC,
				FreqRatio:   10,
			},
			{
				ChannelName: "Free User - High Priority",
				MsgsC:       freeUserHighPriorityC,
				FreqRatio:   5,
			},
			{
				ChannelName: "Free User - Low Priority",
				MsgsC:       freeUserLowPriorityC,
				FreqRatio:   1,
			},
		}
		return priority_channels.NewWithFrequencyRatio[string](channelsWithFrequencyRatio)
		
	case PayingCustomerAlwaysFirst_NoStarvationOfLowMessagesForSameUser:
		channelsWithPriority := []channel_groups.PriorityChannelGroupWithFreqRatio[string]{
			{
				ChannelsWithFreqRatios: []priority_channels.ChannelFreqRatio[string]{
					{
						ChannelName: "Paying Customer - High Priority",
						MsgsC:       payingCustomerHighPriorityC,
						FreqRatio:   5,
					},
					{
						ChannelName: "Paying Customer - Low Priority",
						MsgsC:       payingCustomerLowPriorityC,
						FreqRatio:   1,
					},
				},
				Priority: 10,
			},
			{
				ChannelsWithFreqRatios: []priority_channels.ChannelFreqRatio[string]{
					{
						ChannelName: "Free User - High Priority",
						MsgsC:       freeUserHighPriorityC,
						FreqRatio:   5,
					},
					{
						ChannelName: "Free User - Low Priority",
						MsgsC:       freeUserLowPriorityC,
						FreqRatio:   1,
					},
				},
				Priority: 1,
			},
		}
		return channel_groups.NewByHighestPriorityFirstAmongFreqRatioChannelGroups[string](ctx, channelsWithPriority)
		
	case NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirstForSameUser:
		channelsWithFreqRatio := []channel_groups.ChannelGroupWithHighestPriorityFirst[string]{
			{
				ChannelsWithPriority: []priority_channels.ChannelWithPriority[string]{
					{
						ChannelName: "Paying Customer - High Priority",
						MsgsC:       payingCustomerHighPriorityC,
						Priority:    5,
					},
					{
						ChannelName: "Paying Customer - Low Priority",
						MsgsC:       payingCustomerLowPriorityC,
						Priority:    1,
					},
				},
				FreqRatio: 10,
			},
			{
				ChannelsWithPriority: []priority_channels.ChannelWithPriority[string]{
					{
						ChannelName: "Free User - High Priority",
						MsgsC:       freeUserHighPriorityC,
						Priority:    5,
					},
					{
						ChannelName: "Free User - Low Priority",
						MsgsC:       freeUserLowPriorityC,
						Priority:    1,
					},
				},
				FreqRatio: 1,
			},
		}
		return channel_groups.NewByFreqRatioAmongHighestPriorityFirstChannelGroups[string](ctx, channelsWithFreqRatio)
		
	default:
		return nil
	}
}
```
