package channel_groups_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dimag-jfrog/priority-channels"
	"github.com/dimag-jfrog/priority-channels/channel-groups"
)

func TestProcessMessagesByFreqRatioAmongHighestFirstChannelGroups(t *testing.T) {
	msgsChannels := make([]chan *Msg, 4)
	msgsChannels[0] = make(chan *Msg, 15)
	msgsChannels[1] = make(chan *Msg, 15)
	msgsChannels[2] = make(chan *Msg, 15)
	msgsChannels[3] = make(chan *Msg, 15)

	channels := []channel_groups.ChannelGroupWithHighestPriorityFirst[*Msg]{
		{
			ChannelsWithPriority: []priority_channels.ChannelWithPriority[*Msg]{
				{
					ChannelName: "Priority-1",
					MsgsC:       msgsChannels[0],
					Priority:    1,
				},
				{
					ChannelName: "Priority-5",
					MsgsC:       msgsChannels[1],
					Priority:    5,
				},
			},
			FreqRatio: 1,
		},
		{
			ChannelsWithPriority: []priority_channels.ChannelWithPriority[*Msg]{
				{
					ChannelName: "Priority-10",
					MsgsC:       msgsChannels[2],
					Priority:    10,
				},
			},
			FreqRatio: 5,
		},
		{
			ChannelsWithPriority: []priority_channels.ChannelWithPriority[*Msg]{
				{
					ChannelName: "Priority-1000",
					MsgsC:       msgsChannels[3],
					Priority:    1000,
				},
			},
			FreqRatio: 10,
		},
	}

	channelNames := []string{"Priority-1", "Priority-5", "Priority-10", "Priority-1000"}

	for i := 0; i <= 2; i++ {
		for j := 1; j <= 15; j++ {
			msgsChannels[i] <- &Msg{Body: fmt.Sprintf("%s Msg-%d", channelNames[i], j)}
		}
	}
	msgsChannels[3] <- &Msg{Body: "Priority-1000 Msg-1"}

	var results []*Msg
	msgProcessor := func(_ context.Context, msg *Msg, ChannelName string) {
		results = append(results, msg)
	}
	ctx, cancel := context.WithCancel(context.Background())

	go channel_groups.ProcessMessagesByFreqRatioAmongHighestFirstChannelGroups(ctx, channels, msgProcessor)

	time.Sleep(3 * time.Second)
	cancel()

	expectedResults := []*Msg{
		{Body: "Priority-1000 Msg-1"},
		{Body: "Priority-10 Msg-1"},
		{Body: "Priority-10 Msg-2"},
		{Body: "Priority-10 Msg-3"},
		{Body: "Priority-10 Msg-4"},
		{Body: "Priority-10 Msg-5"},
		{Body: "Priority-5 Msg-1"},
		{Body: "Priority-10 Msg-6"},
		{Body: "Priority-10 Msg-7"},
		{Body: "Priority-10 Msg-8"},
		{Body: "Priority-10 Msg-9"},
		{Body: "Priority-10 Msg-10"},
		{Body: "Priority-5 Msg-2"},
		{Body: "Priority-10 Msg-11"},
		{Body: "Priority-10 Msg-12"},
		{Body: "Priority-10 Msg-13"},
		{Body: "Priority-10 Msg-14"},
		{Body: "Priority-10 Msg-15"},
		{Body: "Priority-5 Msg-3"},
		{Body: "Priority-5 Msg-4"},
		{Body: "Priority-5 Msg-5"},
		{Body: "Priority-5 Msg-6"},
		{Body: "Priority-5 Msg-7"},
		{Body: "Priority-5 Msg-8"},
		{Body: "Priority-5 Msg-9"},
		{Body: "Priority-5 Msg-10"},
		{Body: "Priority-5 Msg-11"},
		{Body: "Priority-5 Msg-12"},
		{Body: "Priority-5 Msg-13"},
		{Body: "Priority-5 Msg-14"},
		{Body: "Priority-5 Msg-15"},
		{Body: "Priority-1 Msg-1"},
		{Body: "Priority-1 Msg-2"},
		{Body: "Priority-1 Msg-3"},
		{Body: "Priority-1 Msg-4"},
		{Body: "Priority-1 Msg-5"},
		{Body: "Priority-1 Msg-6"},
		{Body: "Priority-1 Msg-7"},
		{Body: "Priority-1 Msg-8"},
		{Body: "Priority-1 Msg-9"},
		{Body: "Priority-1 Msg-10"},
		{Body: "Priority-1 Msg-11"},
		{Body: "Priority-1 Msg-12"},
		{Body: "Priority-1 Msg-13"},
		{Body: "Priority-1 Msg-14"},
		{Body: "Priority-1 Msg-15"},
	}

	if len(results) != len(expectedResults) {
		t.Errorf("Expected %d results, but got %d", len(expectedResults), len(results))
	}
	for i := range results {
		if results[i].Body != expectedResults[i].Body {
			t.Errorf("Result %d: Expected message %s, but got %s",
				i, expectedResults[i].Body, results[i].Body)
		}
	}
}

type UsagePattern int

const (
	HighestPriorityAlwaysFirst UsagePattern = iota
	FrequencyRatioForAll
	PayingCustomerAlwaysFirst_NoStarvationOfLowMessages
	NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirst
)

func TestExample(t *testing.T) {
	ctx := context.Background()
	payingCustomerHighPriorityC := make(chan string, 1)
	payingCustomerLowPriorityC := make(chan string, 1)
	freeUserHighPriorityC := make(chan string, 1)
	freeUserLowPriorityC := make(chan string, 1)

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
		for i := 0; i < 10; i++ {
			payingCustomerHighPriorityC <- fmt.Sprintf("high priority message %d", i)
			payingCustomerLowPriorityC <- fmt.Sprintf("low priority message %d", i)
			freeUserHighPriorityC <- fmt.Sprintf("high priority message %d", i)
			freeUserLowPriorityC <- fmt.Sprintf("low priority message %d", i)
		}
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
	case PayingCustomerAlwaysFirst_NoStarvationOfLowMessages:
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
	case NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirst:
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
