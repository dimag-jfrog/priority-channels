package priority_channels_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pc "github.com/dimag-jfrog/priority-channels"
	"github.com/dimag-jfrog/priority-channels/channels"
)

func TestProcessMessagesByPriorityWithHighestAlwaysFirst(t *testing.T) {
	msgsChannels := make([]chan *Msg, 4)
	msgsChannels[0] = make(chan *Msg, 15)
	msgsChannels[1] = make(chan *Msg, 15)
	msgsChannels[2] = make(chan *Msg, 15)
	msgsChannels[3] = make(chan *Msg, 15)

	channels := []channels.ChannelWithPriority[*Msg]{
		channels.NewChannelWithPriority("Priority-1", msgsChannels[0], 1),
		channels.NewChannelWithPriority("Priority-5", msgsChannels[1], 5),
		channels.NewChannelWithPriority("Priority-10", msgsChannels[2], 10),
		channels.NewChannelWithPriority("Priority-1000", msgsChannels[3], 1000),
	}

	for i := 0; i <= 2; i++ {
		for j := 1; j <= 15; j++ {
			msgsChannels[i] <- &Msg{Body: fmt.Sprintf("%s Msg-%d", channels[i].ChannelName(), j)}
		}
	}
	msgsChannels[3] <- &Msg{Body: "Priority-1000 Msg-1"}

	var results []*Msg
	msgProcessor := func(_ context.Context, msg *Msg, channelName string) {
		results = append(results, msg)
	}
	ctx, cancel := context.WithCancel(context.Background())

	go pc.ProcessMessagesByPriorityWithHighestAlwaysFirst(ctx, channels, msgProcessor)

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

func TestProcessMessagesByPriorityWithHighestAlwaysFirst_MessagesInOneOfTheChannelsArriveAfterSomeTime(t *testing.T) {
	msgsChannels := make([]chan *Msg, 3)
	msgsChannels[0] = make(chan *Msg, 7)
	msgsChannels[1] = make(chan *Msg, 7)
	msgsChannels[2] = make(chan *Msg, 7)

	channels := []channels.ChannelWithPriority[*Msg]{
		channels.NewChannelWithPriority("Priority-1", msgsChannels[0], 1),
		channels.NewChannelWithPriority("Priority-2", msgsChannels[1], 2),
		channels.NewChannelWithPriority("Priority-3", msgsChannels[2], 3),
	}

	simulateLongProcessingMsg := "Simulate long processing"
	for j := 1; j <= 5; j++ {
		msgsChannels[0] <- &Msg{Body: fmt.Sprintf("%s Msg-%d", channels[0].ChannelName(), j)}
		suffix := ""
		if j == 5 {
			suffix = " - " + simulateLongProcessingMsg
		}
		msgsChannels[2] <- &Msg{Body: fmt.Sprintf("%s Msg-%d%s", channels[2].ChannelName(), j, suffix)}
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

	go pc.ProcessMessagesByPriorityWithHighestAlwaysFirst(ctx, channels, msgProcessor)

	time.Sleep(1 * time.Second)
	for j := 6; j <= 7; j++ {
		msgsChannels[0] <- &Msg{Body: fmt.Sprintf("%s Msg-%d", channels[0].ChannelName(), j)}
		msgsChannels[2] <- &Msg{Body: fmt.Sprintf("%s Msg-%d", channels[2].ChannelName(), j)}
	}
	msgsChannels[1] <- &Msg{Body: fmt.Sprintf("%s Msg-%d", channels[1].ChannelName(), 1)}
	msgsChannels[1] <- &Msg{Body: fmt.Sprintf("%s Msg-%d", channels[1].ChannelName(), 2)}
	msgsChannels[1] <- &Msg{Body: fmt.Sprintf("%s Msg-%d", channels[1].ChannelName(), 3)}
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
		{Body: "Priority-2 Msg-1"},
		{Body: "Priority-2 Msg-2"},
		{Body: "Priority-2 Msg-3"},
		{Body: "Priority-1 Msg-1"},
		{Body: "Priority-1 Msg-2"},
		{Body: "Priority-1 Msg-3"},
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

func TestProcessMessagesByPriorityWithHighestAlwaysFirst_ChannelClose(t *testing.T) {
	urgentC := make(chan string)
	normalC := make(chan string)
	lowPriorityC := make(chan string)

	channelsWithPriority := []channels.ChannelWithPriority[string]{
		channels.NewChannelWithPriority(
			"Urgent Messages",
			urgentC,
			10),
		channels.NewChannelWithPriority(
			"Normal Messages",
			normalC,
			5),
		channels.NewChannelWithPriority(
			"Low Priority Messages",
			lowPriorityC,
			1),
	}

	close(normalC)

	ch := pc.NewByHighestAlwaysFirst(context.Background(), channelsWithPriority)

	for i := 0; i < 3; i++ {
		message, channelName, status := ch.ReceiveWithContext(context.Background())
		if status != pc.ReceiveChannelClosed {
			t.Errorf("Expected status ReceiveChannelClosed (%d), but got %d", pc.ReceiveChannelClosed, status)
		}
		if channelName != "Normal Messages" {
			t.Errorf("Expected channel name 'Normal Messages', but got %s", channelName)
		}
		if message != "" {
			t.Errorf("Expected empty message, but got %s", message)
		}
	}

	message, channelName, status := ch.ReceiveWithDefaultCase()
	if status != pc.ReceiveChannelClosed {
		t.Errorf("Expected status ReceiveChannelClosed (%d), but got %d", pc.ReceiveChannelClosed, status)
	}
	if channelName != "Normal Messages" {
		t.Errorf("Expected channel name 'Normal Messages', but got %s", channelName)
	}
	if message != "" {
		t.Errorf("Expected empty message, but got %s", message)
	}
}

func TestProcessMessagesByPriorityWithHighestAlwaysFirst_ExitOnDefaultCase(t *testing.T) {
	urgentC := make(chan string)
	normalC := make(chan string)
	lowPriorityC := make(chan string)

	channelsWithPriority := []channels.ChannelWithPriority[string]{
		channels.NewChannelWithPriority(
			"Urgent Messages",
			urgentC,
			10),
		channels.NewChannelWithPriority(
			"Normal Messages",
			normalC,
			5),
		channels.NewChannelWithPriority(
			"Low Priority Messages",
			lowPriorityC,
			1),
	}

	ch := pc.NewByHighestAlwaysFirst(context.Background(), channelsWithPriority)

	message, channelName, status := ch.ReceiveWithDefaultCase()
	if status != pc.ReceiveDefaultCase {
		t.Errorf("Expected status ReceiveDefaultCase (%d), but got %d", pc.ReceiveDefaultCase, status)
	}
	if channelName != "" {
		t.Errorf("Expected empty channel name, but got %s", channelName)
	}
	if message != "" {
		t.Errorf("Expected empty message, but got %s", message)
	}
}

func TestProcessMessagesByPriorityWithHighestAlwaysFirst_RequestContextCancelled(t *testing.T) {
	urgentC := make(chan string)
	normalC := make(chan string)
	lowPriorityC := make(chan string)

	channelsWithPriority := []channels.ChannelWithPriority[string]{
		channels.NewChannelWithPriority(
			"Urgent Messages",
			urgentC,
			10),
		channels.NewChannelWithPriority(
			"Normal Messages",
			normalC,
			5),
		channels.NewChannelWithPriority(
			"Low Priority Messages",
			lowPriorityC,
			1),
	}

	ch := pc.NewByHighestAlwaysFirst(context.Background(), channelsWithPriority)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	message, channelName, status := ch.ReceiveWithContext(ctx)
	if status != pc.ReceiveContextCancelled {
		t.Errorf("Expected status ReceiveContextCancelled (%d), but got %d", pc.ReceiveContextCancelled, status)
	}
	if channelName != "" {
		t.Errorf("Expected empty channel name, but got %s", channelName)
	}
	if message != "" {
		t.Errorf("Expected empty message, but got %s", message)
	}
}

func TestProcessMessagesByPriorityWithHighestAlwaysFirst_PriorityChannelContextCancelled(t *testing.T) {
	urgentC := make(chan string)
	normalC := make(chan string)
	lowPriorityC := make(chan string)

	channelsWithPriority := []channels.ChannelWithPriority[string]{
		channels.NewChannelWithPriority(
			"Urgent Messages",
			urgentC,
			10),
		channels.NewChannelWithPriority(
			"Normal Messages",
			normalC,
			5),
		channels.NewChannelWithPriority(
			"Low Priority Messages",
			lowPriorityC,
			1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ch := pc.NewByHighestAlwaysFirst(ctx, channelsWithPriority)

	message, channelName, status := ch.ReceiveWithContext(context.Background())
	if status != pc.ReceivePriorityChannelCancelled {
		t.Errorf("Expected status ReceivePriorityChannelCancelled (%d), but got %d", pc.ReceivePriorityChannelCancelled, status)
	}
	if channelName != "" {
		t.Errorf("Expected empty channel name, but got %s", channelName)
	}
	if message != "" {
		t.Errorf("Expected empty message, but got %s", message)
	}
}
