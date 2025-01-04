package priority_channels_test

import (
	"context"
	"testing"

	"github.com/dimag-jfrog/priority-channels"
	"github.com/dimag-jfrog/priority-channels/channels"
)

func TestSelect(t *testing.T) {
	urgentC := make(chan string, 1)
	normalC := make(chan string, 1)
	lowPriorityC := make(chan string, 1)

	urgentC <- "Urgent message"
	normalC <- "Normal message"
	lowPriorityC <- "Low priority message"

	channelsWithPriority := []channels.ChannelWithPriority[string]{
		channels.NewChannelWithPriority(
			"Low Priority Messages",
			lowPriorityC,
			1),
		channels.NewChannelWithPriority(
			"Urgent Messages",
			urgentC,
			10),
		channels.NewChannelWithPriority(
			"Normal Messages",
			normalC,
			5),
	}

	ctx := context.Background()
	msg, channelName, status := priority_channels.Select(ctx, channelsWithPriority)
	if channelName != "Urgent Messages" {
		t.Errorf("Expected channel name %s, but got %s", "Urgent Messages", channelName)
	}
	if status != priority_channels.ReceiveSuccess {
		t.Errorf("Expected ok to be true, but got false")
	}
	if msg != "Urgent message" {
		t.Errorf("Expected 'Urgent message' message, but got %s", msg)
	}

	msg, channelName, status = priority_channels.Select(ctx, channelsWithPriority)
	if channelName != "Normal Messages" {
		t.Errorf("Expected channel name %s, but got %s", "Normal Messages", channelName)
	}
	if status != priority_channels.ReceiveSuccess {
		t.Errorf("Expected ok to be true, but got false")
	}
	if msg != "Normal message" {
		t.Errorf("Expected 'Normal message' message, but got %s", msg)
	}

	msg, channelName, status = priority_channels.Select(ctx, channelsWithPriority)
	if channelName != "Low Priority Messages" {
		t.Errorf("Expected channel name %s, but got %s", "Low Priority Messages", channelName)
	}
	if status != priority_channels.ReceiveSuccess {
		t.Errorf("Expected ok to be true, but got false")
	}
	if msg != "Low priority message" {
		t.Errorf("Expected 'Low priority message' message, but got %s", msg)
	}
}

func TestSelectWithDefaultUseCase(t *testing.T) {
	urgentC := make(chan string, 1)
	normalC := make(chan string, 1)
	lowPriorityC := make(chan string, 1)

	channelsWithPriority := []channels.ChannelWithPriority[string]{
		channels.NewChannelWithPriority(
			"Low Priority Messages",
			lowPriorityC,
			1),
		channels.NewChannelWithPriority(
			"Urgent Messages",
			urgentC,
			10),
		channels.NewChannelWithPriority(
			"Normal Messages",
			normalC,
			5),
	}

	msg, channelName, status := priority_channels.SelectWithDefaultCase(channelsWithPriority)
	if status != priority_channels.ReceiveDefaultCase {
		t.Errorf("Expected status default-select-case, but got %d", status)
	}
	if channelName != "" {
		t.Errorf("Expected empty channel name, but got %s", channelName)
	}
	if msg != "" {
		t.Errorf("Expected empty message, but got %s", msg)
	}

	urgentC <- "Urgent message"
	normalC <- "Normal message"
	lowPriorityC <- "Low priority message"

	msg, channelName, status = priority_channels.SelectWithDefaultCase(channelsWithPriority)
	if channelName != "Urgent Messages" {
		t.Errorf("Expected channel name %s, but got %s", "Urgent Messages", channelName)
	}
	if status != priority_channels.ReceiveSuccess {
		t.Errorf("Expected ok to be true, but got false")
	}
	if msg != "Urgent message" {
		t.Errorf("Expected 'Urgent message' message, but got %s", msg)
	}

	msg, channelName, status = priority_channels.SelectWithDefaultCase(channelsWithPriority)
	if channelName != "Normal Messages" {
		t.Errorf("Expected channel name %s, but got %s", "Normal Messages", channelName)
	}
	if status != priority_channels.ReceiveSuccess {
		t.Errorf("Expected ok to be true, but got false")
	}
	if msg != "Normal message" {
		t.Errorf("Expected 'Normal message' message, but got %s", msg)
	}

	msg, channelName, status = priority_channels.SelectWithDefaultCase(channelsWithPriority)
	if channelName != "Low Priority Messages" {
		t.Errorf("Expected channel name %s, but got %s", "Low Priority Messages", channelName)
	}
	if status != priority_channels.ReceiveSuccess {
		t.Errorf("Expected ok to be true, but got false")
	}
	if msg != "Low priority message" {
		t.Errorf("Expected 'Low priority message' message, but got %s", msg)
	}

	msg, channelName, status = priority_channels.SelectWithDefaultCase(channelsWithPriority)
	if status != priority_channels.ReceiveDefaultCase {
		t.Errorf("Expected status default-select-case, but got %d", status)
	}
	if channelName != "" {
		t.Errorf("Expected empty channel name, but got %s", channelName)
	}
	if msg != "" {
		t.Errorf("Expected empty message, but got %s", msg)
	}
}
