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

func TestProcessMessagesByFreqRatioAmongHighestFirstChannelGroups(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	msgsChannels := make([]chan *Msg, 4)
	msgsChannels[0] = make(chan *Msg, 15)
	msgsChannels[1] = make(chan *Msg, 15)
	msgsChannels[2] = make(chan *Msg, 15)
	msgsChannels[3] = make(chan *Msg, 15)

	channels := []priority_channel_groups.PriorityChannelWithFreqRatio[*Msg]{
		{
			PriorityChannel: priority_channels.NewByHighestAlwaysFirst[*Msg](ctx, []channels.ChannelWithPriority[*Msg]{
				channels.NewChannelWithPriority("Priority-1", msgsChannels[0], 1),
				channels.NewChannelWithPriority("Priority-5", msgsChannels[1], 5),
			}),
			FreqRatio: 1,
		},
		{
			PriorityChannel: priority_channels.NewByHighestAlwaysFirst[*Msg](ctx, []channels.ChannelWithPriority[*Msg]{
				channels.NewChannelWithPriority("Priority-10", msgsChannels[2], 10),
			}),
			FreqRatio: 5,
		},
		{
			PriorityChannel: priority_channels.NewByHighestAlwaysFirst[*Msg](ctx, []channels.ChannelWithPriority[*Msg]{
				channels.NewChannelWithPriority("Priority-1000", msgsChannels[3], 1000),
			}),
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
	msgProcessor := func(_ context.Context, msg *Msg, channelName string) {
		results = append(results, msg)
	}

	go priority_channel_groups.ProcessPriorityChannelsByFrequencyRatio(ctx, channels, msgProcessor)

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

func TestProcessMessagesByFreqRatioAmongHighestFirstChannelGroups_ChannelClosed(t *testing.T) {
	ctx := context.Background()
	payingCustomerHighPriorityC := make(chan string)
	payingCustomerLowPriorityC := make(chan string)
	freeUserHighPriorityC := make(chan string)
	freeUserLowPriorityC := make(chan string)

	channelsWithPriority := []priority_channel_groups.PriorityChannelWithPriority[string]{
		{
			PriorityChannel: priority_channels.NewByFrequencyRatio[string](ctx, []channels.ChannelFreqRatio[string]{
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
			PriorityChannel: priority_channels.NewByFrequencyRatio[string](ctx, []channels.ChannelFreqRatio[string]{
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
	ch := priority_channel_groups.CombineByHighestPriorityFirst[string](ctx, channelsWithPriority)

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

func TestProcessMessagesByFreqRatioAmongHighestFirstChannelGroups_ExitOnDefaultCase(t *testing.T) {
	ctx := context.Background()
	payingCustomerHighPriorityC := make(chan string)
	payingCustomerLowPriorityC := make(chan string)
	freeUserHighPriorityC := make(chan string)
	freeUserLowPriorityC := make(chan string)

	channelsWithPriority := []priority_channel_groups.PriorityChannelWithPriority[string]{
		{
			PriorityChannel: priority_channels.NewByFrequencyRatio[string](ctx, []channels.ChannelFreqRatio[string]{
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
			PriorityChannel: priority_channels.NewByFrequencyRatio[string](ctx, []channels.ChannelFreqRatio[string]{
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
	ch := priority_channel_groups.CombineByHighestPriorityFirst[string](ctx, channelsWithPriority)

	message, channelName, status := ch.ReceiveWithDefaultCase()
	if status != priority_channels.ReceiveDefaultCase {
		t.Errorf("Expected status ReceiveDefaultCase (%d), but got %d", priority_channels.ReceiveDefaultCase, status)
	}
	if channelName != "" {
		t.Errorf("Expected empty channel name, but got %s", channelName)
	}
	if message != "" {
		t.Errorf("Expected empty message, but got %s", message)
	}
}

func TestProcessMessagesByFreqRatioAmongHighestFirstChannelGroups_RequestContextCancelled(t *testing.T) {
	ctx := context.Background()
	payingCustomerHighPriorityC := make(chan string)
	payingCustomerLowPriorityC := make(chan string)
	freeUserHighPriorityC := make(chan string)
	freeUserLowPriorityC := make(chan string)

	channelsWithPriority := []priority_channel_groups.PriorityChannelWithPriority[string]{
		{
			PriorityChannel: priority_channels.NewByFrequencyRatio[string](ctx, []channels.ChannelFreqRatio[string]{
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
			PriorityChannel: priority_channels.NewByFrequencyRatio[string](ctx, []channels.ChannelFreqRatio[string]{
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
	ch := priority_channel_groups.CombineByHighestPriorityFirst[string](ctx, channelsWithPriority)

	ctxWithCancel, cancel := context.WithCancel(context.Background())
	cancel()

	message, channelName, status := ch.ReceiveWithContext(ctxWithCancel)
	if status != priority_channels.ReceiveContextCancelled {
		t.Errorf("Expected status ReceiveContextCancelled (%d), but got %d", priority_channels.ReceiveContextCancelled, status)
	}
	if channelName != "" {
		t.Errorf("Expected empty channel name, but got %s", channelName)
	}
	if message != "" {
		t.Errorf("Expected empty message, but got %s", message)
	}
}

func TestProcessMessagesByFreqRatioAmongHighestFirstChannelGroups_PriorityChannelContextCancelled(t *testing.T) {
	ctx := context.Background()
	payingCustomerHighPriorityC := make(chan string)
	payingCustomerLowPriorityC := make(chan string)
	freeUserHighPriorityC := make(chan string)
	freeUserLowPriorityC := make(chan string)

	channelsWithPriority := []priority_channel_groups.PriorityChannelWithPriority[string]{
		{
			PriorityChannel: priority_channels.NewByFrequencyRatio[string](ctx, []channels.ChannelFreqRatio[string]{
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
			PriorityChannel: priority_channels.NewByFrequencyRatio[string](ctx, []channels.ChannelFreqRatio[string]{
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

	ctxWithCancel, cancel := context.WithCancel(context.Background())
	cancel()
	ch := priority_channel_groups.CombineByHighestPriorityFirst[string](ctxWithCancel, channelsWithPriority)

	message, channelName, status := ch.ReceiveWithContext(ctx)
	if status != priority_channels.ReceiveChannelClosed {
		t.Errorf("Expected status ReceiveChannelClosed (%d), but got %d", priority_channels.ReceiveChannelClosed, status)
	}
	if channelName != "" {
		t.Errorf("Expected empty channel name, but got %s", channelName)
	}
	if message != "" {
		t.Errorf("Expected empty message, but got %s", message)
	}
}
