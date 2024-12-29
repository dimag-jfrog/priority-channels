package priority_channels_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dimag-jfrog/priority-channels"
	"github.com/dimag-jfrog/priority-channels/channels"
	"github.com/dimag-jfrog/priority-channels/priority-channel-groups"
)

type UsagePattern int

const (
	HighestPriorityAlwaysFirst UsagePattern = iota
	FrequencyRatioForAll
	PayingCustomerAlwaysFirst_NoStarvationOfLowMessagesForSameUser
	NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirstForSameUser
	FrequencyRatioBetweenUsers_AndFreqRatioBetweenMessagesForSameUser
	Combined_FrequencyRatioBetweenUsersAndMessages_PriorityForUrgentMessages
)

var usagePatternNames = map[UsagePattern]string{
	HighestPriorityAlwaysFirst: "Highest Priority Always First",
	FrequencyRatioForAll:       "Frequency Ratio For All",
	PayingCustomerAlwaysFirst_NoStarvationOfLowMessagesForSameUser:           "Paying Customer Always First, No Starvation Of Low Messages For Same User",
	NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirstForSameUser:        "No Starvation Of Free User, High Priority Messages Always First For Same User",
	FrequencyRatioBetweenUsers_AndFreqRatioBetweenMessagesForSameUser:        "Frequency Ratio Between Users And Frequency Ratio Between Message Types For Same User",
	Combined_FrequencyRatioBetweenUsersAndMessages_PriorityForUrgentMessages: "Frequency Ratio Between Users And Between Message Types, Priority For Urgent Messages",
}

func TestAll(t *testing.T) {
	usagePatterns := []UsagePattern{
		HighestPriorityAlwaysFirst,
		FrequencyRatioForAll,
		PayingCustomerAlwaysFirst_NoStarvationOfLowMessagesForSameUser,
		NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirstForSameUser,
		FrequencyRatioBetweenUsers_AndFreqRatioBetweenMessagesForSameUser,
		Combined_FrequencyRatioBetweenUsersAndMessages_PriorityForUrgentMessages,
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
	urgentMessagesC := make(chan string)

	ch := getPriorityChannelByUsagePattern(
		ctx,
		pattern,
		payingCustomerHighPriorityC,
		payingCustomerLowPriorityC,
		freeUserHighPriorityC,
		freeUserLowPriorityC,
		urgentMessagesC)
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
	if pattern == Combined_FrequencyRatioBetweenUsersAndMessages_PriorityForUrgentMessages {
		go func() {
			for i := 1; i <= 5; i++ {
				urgentMessagesC <- fmt.Sprintf("urgent message %d", i)
			}
		}()
	}

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
	urgentMessagesC chan string,
) priority_channels.PriorityChannel[string] {

	switch usage {

	case HighestPriorityAlwaysFirst:
		channelsWithPriority := []channels.ChannelWithPriority[string]{
			channels.NewChannelWithPriority(
				"Paying Customer - High Priority",
				payingCustomerHighPriorityC,
				10),
			channels.NewChannelWithPriority(
				"Paying Customer - Low Priority",
				payingCustomerLowPriorityC,
				6),
			channels.NewChannelWithPriority(
				"Free User - High Priority",
				freeUserHighPriorityC,
				5),
			channels.NewChannelWithPriority(
				"Free User - Low Priority",
				freeUserLowPriorityC,
				1),
		}
		return priority_channels.NewByHighestAlwaysFirst[string](channelsWithPriority)

	case FrequencyRatioForAll:
		channelsWithFrequencyRatio := []channels.ChannelFreqRatio[string]{
			channels.NewChannelWithFreqRatio(
				"Paying Customer - High Priority",
				payingCustomerHighPriorityC,
				50),
			channels.NewChannelWithFreqRatio(
				"Paying Customer - Low Priority",
				payingCustomerLowPriorityC,
				10),
			channels.NewChannelWithFreqRatio(
				"Free User - High Priority",
				freeUserHighPriorityC,
				5),
			channels.NewChannelWithFreqRatio(
				"Free User - Low Priority",
				freeUserLowPriorityC,
				1),
		}
		return priority_channels.NewByFrequencyRatio[string](channelsWithFrequencyRatio)

	case PayingCustomerAlwaysFirst_NoStarvationOfLowMessagesForSameUser:
		channelsWithPriority := []priority_channel_groups.PriorityChannelWithPriority[string]{
			{
				PriorityChannel: priority_channels.NewByFrequencyRatio[string]([]channels.ChannelFreqRatio[string]{
					channels.NewChannelWithFreqRatio(
						"Paying Customer - High Priority",
						payingCustomerHighPriorityC,
						5),
					channels.NewChannelWithFreqRatio(
						"Paying Customer - Low Priority",
						payingCustomerLowPriorityC,
						1),
				}),
				Priority: 10,
			},
			{
				PriorityChannel: priority_channels.NewByFrequencyRatio[string]([]channels.ChannelFreqRatio[string]{
					channels.NewChannelWithFreqRatio(
						"Free User - High Priority",
						freeUserHighPriorityC,
						5),
					channels.NewChannelWithFreqRatio(
						"Free User - Low Priority",
						freeUserLowPriorityC,
						1),
				}),
				Priority: 1,
			},
		}
		return priority_channel_groups.CombineByHighestPriorityFirst[string](ctx, channelsWithPriority)

	case NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirstForSameUser:
		channelsWithFreqRatio := []priority_channel_groups.PriorityChannelWithFreqRatio[string]{
			{
				PriorityChannel: priority_channels.NewByHighestAlwaysFirst[string]([]channels.ChannelWithPriority[string]{
					channels.NewChannelWithPriority(
						"Paying Customer - High Priority",
						payingCustomerHighPriorityC,
						5),
					channels.NewChannelWithPriority(
						"Paying Customer - Low Priority",
						payingCustomerLowPriorityC,
						1),
				}),
				FreqRatio: 10,
			},
			{
				PriorityChannel: priority_channels.NewByHighestAlwaysFirst[string]([]channels.ChannelWithPriority[string]{
					channels.NewChannelWithPriority(
						"Free User - High Priority",
						freeUserHighPriorityC,
						5),
					channels.NewChannelWithPriority(
						"Free User - Low Priority",
						freeUserLowPriorityC,
						1),
				}),
				FreqRatio: 1,
			},
		}
		return priority_channel_groups.CombineByFrequencyRatio[string](ctx, channelsWithFreqRatio)

	case FrequencyRatioBetweenUsers_AndFreqRatioBetweenMessagesForSameUser:
		channelsWithFreqRatio := []priority_channel_groups.PriorityChannelWithFreqRatio[string]{
			{
				PriorityChannel: priority_channels.NewByFrequencyRatio[string]([]channels.ChannelFreqRatio[string]{
					channels.NewChannelWithFreqRatio(
						"Paying Customer - High Priority",
						payingCustomerHighPriorityC,
						5),
					channels.NewChannelWithFreqRatio(
						"Paying Customer - Low Priority",
						payingCustomerLowPriorityC,
						1),
				}),
				FreqRatio: 10,
			},
			{
				PriorityChannel: priority_channels.NewByFrequencyRatio[string]([]channels.ChannelFreqRatio[string]{
					channels.NewChannelWithFreqRatio(
						"Free User - High Priority",
						freeUserHighPriorityC,
						5),
					channels.NewChannelWithFreqRatio(
						"Free User - Low Priority",
						freeUserLowPriorityC,
						1),
				}),
				FreqRatio: 1,
			},
		}
		return priority_channel_groups.CombineByFrequencyRatio[string](ctx, channelsWithFreqRatio)

	case Combined_FrequencyRatioBetweenUsersAndMessages_PriorityForUrgentMessages:
		channelsWithFreqRatio := []priority_channel_groups.PriorityChannelWithFreqRatio[string]{
			{
				PriorityChannel: priority_channels.NewByFrequencyRatio[string]([]channels.ChannelFreqRatio[string]{
					channels.NewChannelWithFreqRatio(
						"Paying Customer - High Priority",
						payingCustomerHighPriorityC,
						5),
					channels.NewChannelWithFreqRatio(
						"Paying Customer - Low Priority",
						payingCustomerLowPriorityC,
						1),
				}),
				FreqRatio: 10,
			},
			{
				PriorityChannel: priority_channels.NewByFrequencyRatio[string]([]channels.ChannelFreqRatio[string]{
					channels.NewChannelWithFreqRatio(
						"Free User - High Priority",
						freeUserHighPriorityC,
						5),
					channels.NewChannelWithFreqRatio(
						"Free User - Low Priority",
						freeUserLowPriorityC,
						1),
				}),
				FreqRatio: 1,
			},
		}
		combinedUsersAndMessageTypesPriorityChannel := priority_channel_groups.CombineByFrequencyRatio[string](ctx, channelsWithFreqRatio)
		return priority_channel_groups.CombineByHighestPriorityFirst[string](ctx, []priority_channel_groups.PriorityChannelWithPriority[string]{
			{
				PriorityChannel: combinedUsersAndMessageTypesPriorityChannel,
				Priority:        1,
			},
			{
				PriorityChannel: priority_channels.NewByHighestAlwaysFirst([]channels.ChannelWithPriority[string]{
					channels.NewChannelWithPriority("Urgent Messages", urgentMessagesC, 1),
				}),
				Priority: 100,
			},
		})

	default:
		return nil
	}
}
