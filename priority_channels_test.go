package priority_channels_test

import (
	"context"
	"fmt"
	priority_channels "github.com/dimag-jfrog/priority-channels"
	channel_groups "github.com/dimag-jfrog/priority-channels/channel-groups"
	"testing"
	"time"
)

type UsagePattern int

const (
	HighestPriorityAlwaysFirst UsagePattern = iota
	FrequencyRatioForAll
	PayingCustomerAlwaysFirst_NoStarvationOfLowMessagesForSameUser
	NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirstForSameUser
)

var usagePatternNames = map[UsagePattern]string{
	HighestPriorityAlwaysFirst: "Highest Priority Always First",
	FrequencyRatioForAll:       "Frequency Ratio For All",
	PayingCustomerAlwaysFirst_NoStarvationOfLowMessagesForSameUser:    "Paying Customer Always First, No Starvation Of Low Messages For Same User",
	NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirstForSameUser: "No Starvation Of Free User, High Priority Messages Always First For Same User",
}

func TestAll(t *testing.T) {
	usagePatterns := []UsagePattern{
		HighestPriorityAlwaysFirst,
		FrequencyRatioForAll,
		PayingCustomerAlwaysFirst_NoStarvationOfLowMessagesForSameUser,
		NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirstForSameUser,
	}
	for _, usagePattern := range usagePatterns {
		t.Run(usagePatternNames[usagePattern], func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Recovered from panic: %v", r)
				}
			}()
			testExample(t, usagePattern)
		})
	}
}

func testExample(t *testing.T, pattern UsagePattern) {
	ctx, cancel := context.WithCancel(context.Background())

	payingCustomerHighPriorityC := make(chan string)
	payingCustomerLowPriorityC := make(chan string)
	freeUserHighPriorityC := make(chan string)
	freeUserLowPriorityC := make(chan string)

	ch := getPriorityChannelByUsagePattern(
		ctx,
		pattern,
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
