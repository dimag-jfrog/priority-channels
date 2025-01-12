package priority_channel_groups_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dimag-jfrog/priority-channels"
	"github.com/dimag-jfrog/priority-channels/channels"
	"github.com/dimag-jfrog/priority-channels/priority-channel-groups"
)

func TestProcessMessagesByFreqRatioAmongFreqRatioChannelGroups(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	payingCustomerHighPriorityC := make(chan string)
	payingCustomerLowPriorityC := make(chan string)
	freeUserHighPriorityC := make(chan string)
	freeUserLowPriorityC := make(chan string)

	channelsWithFreqRatio := []priority_channel_groups.PriorityChannelWithFreqRatio[string]{
		priority_channel_groups.NewPriorityChannelWithFreqRatio("Paying Customer",
			priority_channels.NewByFrequencyRatio[string](ctx, []channels.ChannelFreqRatio[string]{
				channels.NewChannelWithFreqRatio(
					"Paying Customer - High Priority",
					payingCustomerHighPriorityC,
					5),
				channels.NewChannelWithFreqRatio(
					"Paying Customer - Low Priority",
					payingCustomerLowPriorityC,
					1),
			}),
			10),
		priority_channel_groups.NewPriorityChannelWithFreqRatio("Free User",
			priority_channels.NewByFrequencyRatio[string](ctx, []channels.ChannelFreqRatio[string]{
				channels.NewChannelWithFreqRatio(
					"Free User - High Priority",
					freeUserHighPriorityC,
					5),
				channels.NewChannelWithFreqRatio(
					"Free User - Low Priority",
					freeUserLowPriorityC,
					1),
			}),
			1),
	}

	ch := priority_channel_groups.CombineByFrequencyRatio[string](ctx, channelsWithFreqRatio)

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
		time.Sleep(5 * time.Second)
		cancel()
	}()

	// receiving messages from the priority channel
	results := make([]string, 0, 80)
	for {
		message, channelName, ok := ch.Receive()
		if !ok {
			break
		}
		fmt.Printf("%s: %s\n", channelName, message)
		results = append(results, fmt.Sprintf("%s: %s", channelName, message))
		time.Sleep(50 * time.Millisecond)
	}

	expectedResults := []string{
		"Paying Customer - High Priority: high priority message 1",
		"Paying Customer - High Priority: high priority message 2",
		"Paying Customer - High Priority: high priority message 3",
		"Paying Customer - High Priority: high priority message 4",
		"Paying Customer - High Priority: high priority message 5",
		"Paying Customer - Low Priority: low priority message 1",
		"Paying Customer - High Priority: high priority message 6",
		"Paying Customer - High Priority: high priority message 7",
		"Paying Customer - High Priority: high priority message 8",
		"Paying Customer - High Priority: high priority message 9",
		"Free User - High Priority: high priority message 1",
		"Paying Customer - High Priority: high priority message 10",
		"Paying Customer - Low Priority: low priority message 2",
		"Paying Customer - High Priority: high priority message 11",
		"Paying Customer - High Priority: high priority message 12",
		"Paying Customer - High Priority: high priority message 13",
		"Paying Customer - High Priority: high priority message 14",
		"Paying Customer - High Priority: high priority message 15",
		"Paying Customer - Low Priority: low priority message 3",
		"Paying Customer - High Priority: high priority message 16",
		"Paying Customer - High Priority: high priority message 17",
		"Free User - High Priority: high priority message 2",
		"Paying Customer - High Priority: high priority message 18",
		"Paying Customer - High Priority: high priority message 19",
		"Paying Customer - High Priority: high priority message 20",
		"Paying Customer - Low Priority: low priority message 4",
		"Paying Customer - Low Priority: low priority message 5",
		"Paying Customer - Low Priority: low priority message 6",
		"Paying Customer - Low Priority: low priority message 7",
		"Paying Customer - Low Priority: low priority message 8",
		"Paying Customer - Low Priority: low priority message 9",
		"Paying Customer - Low Priority: low priority message 10",
		"Free User - High Priority: high priority message 3",
		"Paying Customer - Low Priority: low priority message 11",
		"Paying Customer - Low Priority: low priority message 12",
		"Paying Customer - Low Priority: low priority message 13",
		"Paying Customer - Low Priority: low priority message 14",
		"Paying Customer - Low Priority: low priority message 15",
		"Paying Customer - Low Priority: low priority message 16",
		"Paying Customer - Low Priority: low priority message 17",
		"Paying Customer - Low Priority: low priority message 18",
		"Paying Customer - Low Priority: low priority message 19",
		"Paying Customer - Low Priority: low priority message 20",
		"Free User - High Priority: high priority message 4",
		"Free User - High Priority: high priority message 5",
		"Free User - Low Priority: low priority message 1",
		"Free User - High Priority: high priority message 6",
		"Free User - High Priority: high priority message 7",
		"Free User - High Priority: high priority message 8",
		"Free User - High Priority: high priority message 9",
		"Free User - High Priority: high priority message 10",
		"Free User - Low Priority: low priority message 2",
		"Free User - High Priority: high priority message 11",
		"Free User - High Priority: high priority message 12",
		"Free User - High Priority: high priority message 13",
		"Free User - High Priority: high priority message 14",
		"Free User - High Priority: high priority message 15",
		"Free User - Low Priority: low priority message 3",
		"Free User - High Priority: high priority message 16",
		"Free User - High Priority: high priority message 17",
		"Free User - High Priority: high priority message 18",
		"Free User - High Priority: high priority message 19",
		"Free User - High Priority: high priority message 20",
		"Free User - Low Priority: low priority message 4",
		"Free User - Low Priority: low priority message 5",
		"Free User - Low Priority: low priority message 6",
		"Free User - Low Priority: low priority message 7",
		"Free User - Low Priority: low priority message 8",
		"Free User - Low Priority: low priority message 9",
		"Free User - Low Priority: low priority message 10",
		"Free User - Low Priority: low priority message 11",
		"Free User - Low Priority: low priority message 12",
		"Free User - Low Priority: low priority message 13",
		"Free User - Low Priority: low priority message 14",
		"Free User - Low Priority: low priority message 15",
		"Free User - Low Priority: low priority message 16",
		"Free User - Low Priority: low priority message 17",
		"Free User - Low Priority: low priority message 18",
		"Free User - Low Priority: low priority message 19",
		"Free User - Low Priority: low priority message 20",
	}

	if len(results) != len(expectedResults) {
		t.Errorf("Expected %d results, but got %d", len(expectedResults), len(results))
	}
	for i := range results {
		if results[i] != expectedResults[i] {
			t.Errorf("Result %d: Expected message %s, but got %s",
				i, expectedResults[i], results[i])
		}
	}
}

func TestProcessMessagesByFreqRatioAmongFreqRatioChannelGroups_ChannelClosed(t *testing.T) {
	ctx := context.Background()
	payingCustomerHighPriorityC := make(chan string)
	payingCustomerLowPriorityC := make(chan string)
	freeUserHighPriorityC := make(chan string)
	freeUserLowPriorityC := make(chan string)

	channelsWithFreqRatio := []priority_channel_groups.PriorityChannelWithFreqRatio[string]{
		priority_channel_groups.NewPriorityChannelWithFreqRatio("Paying Customer",
			priority_channels.NewByFrequencyRatio[string](ctx, []channels.ChannelFreqRatio[string]{
				channels.NewChannelWithFreqRatio(
					"Paying Customer - High Priority",
					payingCustomerHighPriorityC,
					5),
				channels.NewChannelWithFreqRatio(
					"Paying Customer - Low Priority",
					payingCustomerLowPriorityC,
					1),
			}),
			10),
		priority_channel_groups.NewPriorityChannelWithFreqRatio("Free User",
			priority_channels.NewByFrequencyRatio[string](ctx, []channels.ChannelFreqRatio[string]{
				channels.NewChannelWithFreqRatio(
					"Free User - High Priority",
					freeUserHighPriorityC,
					5),
				channels.NewChannelWithFreqRatio(
					"Free User - Low Priority",
					freeUserLowPriorityC,
					1),
			}),
			1),
	}
	ch := priority_channel_groups.CombineByFrequencyRatio[string](ctx, channelsWithFreqRatio)

	close(freeUserHighPriorityC)

	for i := 0; i < 3; i++ {
		message, channelName, status := ch.ReceiveWithContext(context.Background())
		if status != priority_channels.ReceiveChannelClosed {
			t.Errorf("Expected status ReceiveChannelClosed (%d), but got %d", priority_channels.ReceiveChannelClosed, status)
		}
		if channelName != "Free User - High Priority" {
			t.Errorf("Expected channel name 'Free User - High Priority', but got %s", channelName)
		}
		if message != "" {
			t.Errorf("Expected empty message, but got %s", message)
		}
	}

	message, channelName, status := ch.ReceiveWithDefaultCase()
	if status != priority_channels.ReceiveChannelClosed {
		t.Errorf("Expected status ReceiveChannelClosed (%d), but got %d", priority_channels.ReceiveChannelClosed, status)
	}
	if channelName != "Free User - High Priority" {
		t.Errorf("Expected channel name 'Free User - High Priority', but got %s", channelName)
	}
	if message != "" {
		t.Errorf("Expected empty message, but got %s", message)
	}
}
