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
	FrequencyRatioBetweenUsers_AndFreqRatioBetweenMessagesForSameUser
	Combined_CustomersAndUsersOfDifferentProductsWithDifferentMessagePriorities
)

var usagePatternNames = map[UsagePattern]string{
	HighestPriorityAlwaysFirst: "Highest Priority Always First",
	FrequencyRatioForAll:       "Frequency Ratio For All",
	PayingCustomerAlwaysFirst_NoStarvationOfLowMessagesForSameUser:              "Paying Customer Always First, No Starvation Of Low Messages For Same User",
	NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirstForSameUser:           "No Starvation Of Free User, High Priority Messages Always First For Same User",
	FrequencyRatioBetweenUsers_AndFreqRatioBetweenMessagesForSameUser:           "Frequency Ratio Between Users And Frequency Ratio Between Messages For Same User",
	Combined_CustomersAndUsersOfDifferentProductsWithDifferentMessagePriorities: "Combined - Customers And Users Of Different Products With Different Message Priorities",
}

func TestAll(t *testing.T) {
	usagePatterns := []UsagePattern{
		HighestPriorityAlwaysFirst,
		FrequencyRatioForAll,
		PayingCustomerAlwaysFirst_NoStarvationOfLowMessagesForSameUser,
		NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirstForSameUser,
		FrequencyRatioBetweenUsers_AndFreqRatioBetweenMessagesForSameUser,
		Combined_CustomersAndUsersOfDifferentProductsWithDifferentMessagePriorities,
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
		channelsWithPriority := []channel_groups.PriorityChannelWithPriority[string]{
			{
				PriorityChannel: priority_channels.NewWithFrequencyRatio[string]([]priority_channels.ChannelFreqRatio[string]{
					priority_channels.NewChannelWithFreqRatio(
						"Paying Customer - High Priority",
						payingCustomerHighPriorityC,
						5),
					priority_channels.NewChannelWithFreqRatio(
						"Paying Customer - Low Priority",
						payingCustomerLowPriorityC,
						1),
				}),
				Priority: 10,
			},
			{
				PriorityChannel: priority_channels.NewWithFrequencyRatio[string]([]priority_channels.ChannelFreqRatio[string]{
					priority_channels.NewChannelWithFreqRatio(
						"Free User - High Priority",
						freeUserHighPriorityC,
						5),
					priority_channels.NewChannelWithFreqRatio(
						"Free User - Low Priority",
						freeUserLowPriorityC,
						1),
				}),
				Priority: 1,
			},
		}
		return channel_groups.NewByHighestPriorityFirstAmongPriorityChannels[string](ctx, channelsWithPriority)

	case NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirstForSameUser:
		channelsWithFreqRatio := []channel_groups.PriorityChannelWithFreqRatio[string]{
			{
				PriorityChannel: priority_channels.NewWithHighestAlwaysFirst[string]([]priority_channels.ChannelWithPriority[string]{
					priority_channels.NewChannelWithPriority(
						"Paying Customer - High Priority",
						payingCustomerHighPriorityC,
						5),
					priority_channels.NewChannelWithPriority(
						"Paying Customer - Low Priority",
						payingCustomerLowPriorityC,
						1),
				}),
				FreqRatio: 10,
			},
			{
				PriorityChannel: priority_channels.NewWithHighestAlwaysFirst[string]([]priority_channels.ChannelWithPriority[string]{
					priority_channels.NewChannelWithPriority(
						"Free User - High Priority",
						freeUserHighPriorityC,
						5),
					priority_channels.NewChannelWithPriority(
						"Free User - Low Priority",
						freeUserLowPriorityC,
						1),
				}),
				FreqRatio: 1,
			},
		}
		return channel_groups.NewByFrequencyRatioAmongPriorityChannels[string](ctx, channelsWithFreqRatio)

	case FrequencyRatioBetweenUsers_AndFreqRatioBetweenMessagesForSameUser:
		channelsWithFreqRatio := []channel_groups.PriorityChannelWithFreqRatio[string]{
			{
				PriorityChannel: priority_channels.NewWithFrequencyRatio[string]([]priority_channels.ChannelFreqRatio[string]{
					priority_channels.NewChannelWithFreqRatio(
						"Paying Customer - High Priority",
						payingCustomerHighPriorityC,
						5),
					priority_channels.NewChannelWithFreqRatio(
						"Paying Customer - Low Priority",
						payingCustomerLowPriorityC,
						1),
				}),
				FreqRatio: 10,
			},
			{
				PriorityChannel: priority_channels.NewWithFrequencyRatio[string]([]priority_channels.ChannelFreqRatio[string]{
					priority_channels.NewChannelWithFreqRatio(
						"Free User - High Priority",
						freeUserHighPriorityC,
						5),
					priority_channels.NewChannelWithFreqRatio(
						"Free User - Low Priority",
						freeUserLowPriorityC,
						1),
				}),
				FreqRatio: 1,
			},
		}
		return channel_groups.NewByFrequencyRatioAmongPriorityChannels[string](ctx, channelsWithFreqRatio)

	case Combined_CustomersAndUsersOfDifferentProductsWithDifferentMessagePriorities:
		flagshipProductChannelsWithPriority := []channel_groups.PriorityChannelWithPriority[string]{
			{
				PriorityChannel: priority_channels.NewWithFrequencyRatio[string]([]priority_channels.ChannelFreqRatio[string]{
					priority_channels.NewChannelWithFreqRatio(
						"Flagship Product - Paying Customer - High Priority",
						payingCustomerHighPriorityC,
						5),
					priority_channels.NewChannelWithFreqRatio(
						"Flagship Product - Paying Customer - Low Priority",
						payingCustomerLowPriorityC,
						1),
				}),
				Priority: 10,
			},
			{
				PriorityChannel: priority_channels.NewWithFrequencyRatio[string]([]priority_channels.ChannelFreqRatio[string]{
					priority_channels.NewChannelWithFreqRatio(
						"Healthcare - Free User - High Priority",
						freeUserHighPriorityC,
						5),
					priority_channels.NewChannelWithFreqRatio(
						"Healthcare - Free User - Low Priority",
						freeUserLowPriorityC,
						1),
				}),
				Priority: 1,
			},
		}
		flagshipProductChannelGroup := channel_groups.NewByHighestPriorityFirstAmongPriorityChannels[string](ctx, flagshipProductChannelsWithPriority)

		nicheProductChannelsWithPriority := []channel_groups.PriorityChannelWithPriority[string]{
			{
				PriorityChannel: priority_channels.NewWithFrequencyRatio[string]([]priority_channels.ChannelFreqRatio[string]{
					priority_channels.NewChannelWithFreqRatio(
						"Entertainment - Paying Customer - High Priority",
						payingCustomerHighPriorityC,
						5),
					priority_channels.NewChannelWithFreqRatio(
						"Entertainment - Paying Customer - Low Priority",
						payingCustomerLowPriorityC,
						1),
				}),
				Priority: 10,
			},
			{
				PriorityChannel: priority_channels.NewWithFrequencyRatio[string]([]priority_channels.ChannelFreqRatio[string]{
					priority_channels.NewChannelWithFreqRatio(
						"Entertainment - Free User - High Priority",
						freeUserHighPriorityC,
						5),
					priority_channels.NewChannelWithFreqRatio(
						"Entertainment - Free User - Low Priority",
						freeUserLowPriorityC,
						1),
				}),
				Priority: 1,
			},
		}
		nicheProductChannelGroup := channel_groups.NewByHighestPriorityFirstAmongPriorityChannels[string](ctx, nicheProductChannelsWithPriority)

		combinedPriorityChannel := channel_groups.NewByFrequencyRatioAmongPriorityChannels[string](ctx, []channel_groups.PriorityChannelWithFreqRatio[string]{
			{
				PriorityChannel: flagshipProductChannelGroup,
				FreqRatio:       5,
			},
			{
				PriorityChannel: nicheProductChannelGroup,
				FreqRatio:       1,
			},
		})
		return combinedPriorityChannel

	default:
		return nil
	}
}
