package priority_channel_groups_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dimag-jfrog/priority-channels"
	"github.com/dimag-jfrog/priority-channels/channels"
	"github.com/dimag-jfrog/priority-channels/priority-channel-groups"
)

type Msg struct {
	Body string
}

func TestProcessMessagesByPriorityAmongFreqRatioChannelGroups(t *testing.T) {
	msgsChannels := make([]chan *Msg, 4)
	msgsChannels[0] = make(chan *Msg, 15)
	msgsChannels[1] = make(chan *Msg, 15)
	msgsChannels[2] = make(chan *Msg, 15)
	msgsChannels[3] = make(chan *Msg, 15)

	channels := []priority_channel_groups.PriorityChannelWithPriority[*Msg]{
		{
			PriorityChannel: priority_channels.NewByFrequencyRatio[*Msg]([]channels.ChannelFreqRatio[*Msg]{
				channels.NewChannelWithFreqRatio("Priority-1", msgsChannels[0], 1),
				channels.NewChannelWithFreqRatio("Priority-5", msgsChannels[1], 5),
			}),
			Priority: 1,
		},
		{
			PriorityChannel: priority_channels.NewByFrequencyRatio[*Msg]([]channels.ChannelFreqRatio[*Msg]{
				channels.NewChannelWithFreqRatio("Priority-10", msgsChannels[2], 1),
			}),
			Priority: 10,
		},
		{
			PriorityChannel: priority_channels.NewByFrequencyRatio[*Msg]([]channels.ChannelFreqRatio[*Msg]{
				channels.NewChannelWithFreqRatio("Priority-1000", msgsChannels[3], 1),
			}),
			Priority: 1000,
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
	ctx, cancel := context.WithCancel(context.Background())

	go priority_channel_groups.ProcessPriorityChannelsByPriorityWithHighestAlwaysFirst(ctx, channels, msgProcessor)

	time.Sleep(3 * time.Second)
	cancel()

	expectedResults := []*Msg{
		{Body: "Priority-1000 Msg-1"},
		{Body: "Priority-10 Msg-1"},
		{Body: "Priority-10 Msg-2"},
		{Body: "Priority-10 Msg-3"},
		{Body: "Priority-10 Msg-4"},
		{Body: "Priority-10 Msg-5"},
		{Body: "Priority-10 Msg-6"},
		{Body: "Priority-10 Msg-7"},
		{Body: "Priority-10 Msg-8"},
		{Body: "Priority-10 Msg-9"},
		{Body: "Priority-10 Msg-10"},
		{Body: "Priority-10 Msg-11"},
		{Body: "Priority-10 Msg-12"},
		{Body: "Priority-10 Msg-13"},
		{Body: "Priority-10 Msg-14"},
		{Body: "Priority-10 Msg-15"},
		{Body: "Priority-5 Msg-1"},
		{Body: "Priority-5 Msg-2"},
		{Body: "Priority-5 Msg-3"},
		{Body: "Priority-5 Msg-4"},
		{Body: "Priority-5 Msg-5"},
		{Body: "Priority-1 Msg-1"},
		{Body: "Priority-5 Msg-6"},
		{Body: "Priority-5 Msg-7"},
		{Body: "Priority-5 Msg-8"},
		{Body: "Priority-5 Msg-9"},
		{Body: "Priority-5 Msg-10"},
		{Body: "Priority-1 Msg-2"},
		{Body: "Priority-5 Msg-11"},
		{Body: "Priority-5 Msg-12"},
		{Body: "Priority-5 Msg-13"},
		{Body: "Priority-5 Msg-14"},
		{Body: "Priority-5 Msg-15"},
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

func TestProcessMessagesByPriorityAmongFreqRatioChannelGroups_MessagesInOneOfTheChannelsArriveAfterSomeTime(t *testing.T) {
	msgsChannels := make([]chan *Msg, 3)
	msgsChannels[0] = make(chan *Msg, 7)
	msgsChannels[1] = make(chan *Msg, 7)
	msgsChannels[2] = make(chan *Msg, 7)

	channels := []priority_channel_groups.PriorityChannelWithPriority[*Msg]{
		{
			PriorityChannel: priority_channels.NewByFrequencyRatio[*Msg]([]channels.ChannelFreqRatio[*Msg]{
				channels.NewChannelWithFreqRatio("Priority-1", msgsChannels[0], 1),
				channels.NewChannelWithFreqRatio("Priority-2", msgsChannels[1], 2),
			}),
			Priority: 1,
		},
		{
			PriorityChannel: priority_channels.NewByFrequencyRatio[*Msg]([]channels.ChannelFreqRatio[*Msg]{
				channels.NewChannelWithFreqRatio("Priority-3", msgsChannels[2], 1),
			}),
			Priority: 2,
		},
	}

	simulateLongProcessingMsg := "Simulate long processing"
	for j := 1; j <= 5; j++ {
		msgsChannels[0] <- &Msg{Body: fmt.Sprintf("Priority-1 Msg-%d", j)}
		suffix := ""
		if j == 5 {
			suffix = " - " + simulateLongProcessingMsg
		}
		msgsChannels[2] <- &Msg{Body: fmt.Sprintf("Priority-3 Msg-%d%s", j, suffix)}
	}

	waitForMessagesFromPriority2Chan := make(chan struct{})
	var results []*Msg
	msgProcessor := func(_ context.Context, msg *Msg, channelName string) {
		if strings.HasSuffix(msg.Body, simulateLongProcessingMsg) {
			<-waitForMessagesFromPriority2Chan
		}
		results = append(results, msg)
	}
	ctx, cancel := context.WithCancel(context.Background())

	go priority_channel_groups.ProcessPriorityChannelsByPriorityWithHighestAlwaysFirst(ctx, channels, msgProcessor)

	time.Sleep(1 * time.Second)
	for j := 6; j <= 7; j++ {
		msgsChannels[0] <- &Msg{Body: fmt.Sprintf("Priority-1 Msg-%d", j)}
		msgsChannels[2] <- &Msg{Body: fmt.Sprintf("Priority-3 Msg-%d", j)}
	}
	for j := 1; j <= 7; j++ {
		msgsChannels[1] <- &Msg{Body: fmt.Sprintf("Priority-2 Msg-%d", j)}
	}
	waitForMessagesFromPriority2Chan <- struct{}{}

	time.Sleep(3 * time.Second)
	cancel()

	expectedResults := []*Msg{
		{Body: "Priority-3 Msg-1"},
		{Body: "Priority-3 Msg-2"},
		{Body: "Priority-3 Msg-3"},
		{Body: "Priority-3 Msg-4"},
		{Body: "Priority-3 Msg-5 - Simulate long processing"},
		{Body: "Priority-3 Msg-6"},
		{Body: "Priority-3 Msg-7"},
		{Body: "Priority-1 Msg-1"},
		{Body: "Priority-2 Msg-1"},
		{Body: "Priority-2 Msg-2"},
		{Body: "Priority-2 Msg-3"},
		{Body: "Priority-2 Msg-4"},
		{Body: "Priority-1 Msg-2"},
		{Body: "Priority-2 Msg-5"},
		{Body: "Priority-2 Msg-6"},
		{Body: "Priority-1 Msg-3"},
		{Body: "Priority-2 Msg-7"},
		{Body: "Priority-1 Msg-4"},
		{Body: "Priority-1 Msg-5"},
		{Body: "Priority-1 Msg-6"},
		{Body: "Priority-1 Msg-7"},
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
