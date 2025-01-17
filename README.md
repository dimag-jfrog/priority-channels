# priority-channels
Process Go channels by priority. 


The following use cases are supported:

### Primary use cases
- **Highest priority always first** - when we always want to process messages in order of priority
- **Processing by frequency ratio** - when we want to prevent starvation of lower priority messages

### Advanced use cases - priority channel groups
- Channel groups by highest priority first inside group and choose among groups by frequency ratio
- Channel groups by frequency ratio inside group and choose among groups by highest priority first
- Channel groups by frequency ratio inside group and choose among groups by frequency ratio
- Channel groups by highest priority first inside group and choose among groups also by highest priority first
- Graph of priority channels - any combinations of the above to multiple levels of hierarchy


## Usage

### Priority channel with highest priority always first

```go
// Wrap the Go channels in a slice of channels objects with name and priority properties
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

// Two possible usage modes

// 1. Create a priority channel and receive messages from it explicitly
ch := priority_channels.NewByHighestAlwaysFirst(ctx, channelsWithPriority)
for {
    message, channelName, ok := ch.Receive()
    if !ok {
        break
    }
    fmt.Printf("%s: %s\n", channelName, message)
}

// 2. Use a dedicated function that does it behind the scenes and uses a callback function to process messages
msgProcessor := func(_ context.Context, message string, channelName string) {
    fmt.Printf("%s: %s\n", channelName, message)
}
priority_channels.ProcessMessagesByPriorityWithHighestAlwaysFirst(ctx, channelsWithPriority, msgProcessor)
````

### Priority channel with frequency ratio

```go
// Wrap the Go channels in a slice of channels objects with name and frequency ratio properties
highPriorityC := make(chan string)
normalPriorityC := make(chan string)
lowPriorityC := make(chan string)

channelsWithFrequencyRatio := []channels.ChannelFreqRatio[string]{
    channels.NewChannelWithFreqRatio(
        "High Priority", 
        highPriorityC,
        10),
    channels.NewChannelWithFreqRatio(
        "Normal Priority",
        normalPriorityC,
        5),
    channels.NewChannelWithFreqRatio(
        "Low Priority", 
        lowPriorityC,
        1),
}

// Two possible usage modes

// 1. Create a priority channel and receive messages from it explicitly
ch := priority_channels.NewByFrequencyRatio(ctx, channelsWithFrequencyRatio)
for {
    message, channelName, ok := ch.Receive()
    if !ok {
        break
    }
    fmt.Printf("%s: %s\n", channelName, message)
}

// 2. Use a dedicated function that does it behind the scenes and uses a callback function to process messages
msgProcessor := func(_ context.Context, message string, channelName string) {
    fmt.Printf("%s: %s\n", channelName, message)
}
priority_channels.ProcessMessagesByFrequencyRatio(ctx, channelsWithFrequencyRatio, msgProcessor)
````

### Combination of priority channels by highest priority first

```go
urgentMessagesC := make(chan string)
payingCustomerHighPriorityC := make(chan string)
payingCustomerLowPriorityC := make(chan string)
freeUserHighPriorityC := make(chan string)
freeUserLowPriorityC := make(chan string)

priorityChannelsWithPriority := []priority_channel_groups.PriorityChannelWithPriority[string]{
    priority_channel_groups.NewPriorityChannelWithPriority(
        "Urgent Messages",
        priority_channels.WrapAsPriorityChannel(ctx, "Urgent Messages", urgentMessagesC),
        100,
    ),
    priority_channel_groups.NewPriorityChannelWithPriority(
        "Paying Customer",
        priority_channels.NewByFrequencyRatio(ctx, []channels.ChannelFreqRatio[string]{
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
    priority_channel_groups.NewPriorityChannelWithPriority(
        "Free User",
        priority_channels.NewByFrequencyRatio(ctx, []channels.ChannelFreqRatio[string]{
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

// Two possible usage modes

// 1. Create a priority channel and receive messages from it explicitly
ch := priority_channel_groups.CombineByHighestPriorityFirst(ctx, priorityChannelsWithPriority)
for {
    message, channelName, ok := ch.Receive()
    if !ok {
        break
    }
    fmt.Printf("%s: %s\n", channelName, message)
}

// 2. Use a dedicated function that does it behind the scenes and uses a callback function to process messages
msgProcessor := func(_ context.Context, message string, channelName string) {
    fmt.Printf("%s: %s\n", channelName, message)
}
priority_channel_groups.ProcessPriorityChannelsByPriorityWithHighestAlwaysFirst(
    ctx, 
    priorityChannelsWithPriority, 
    msgProcessor)
```

### Combination of priority channels by frequency ratio

```go
urgentMessagesC := make(chan string)
payingCustomerHighPriorityC := make(chan string)
payingCustomerLowPriorityC := make(chan string)
freeUserHighPriorityC := make(chan string)
freeUserLowPriorityC := make(chan string)

priorityChannelsWithFreqRatio := []priority_channel_groups.PriorityChannelWithFreqRatio[string]{
    priority_channel_groups.NewPriorityChannelWithFreqRatio(
        "Urgent Messages",
        priority_channels.WrapAsPriorityChannel(ctx, "Urgent Messages", urgentMessagesC),
        100,
    ),
    priority_channel_groups.NewPriorityChannelWithFreqRatio(
        "Paying Customer",
        priority_channels.NewByHighestAlwaysFirst(ctx, []channels.ChannelWithPriority[string]{
            channels.NewChannelWithPriority(
                "Paying Customer - High Priority",
                payingCustomerHighPriorityC,
                5),
            channels.NewChannelWithPriority(
                "Paying Customer - Low Priority",
                payingCustomerLowPriorityC,
                1),
        }),
        10),
    priority_channel_groups.NewPriorityChannelWithFreqRatio(
        "Free User",
        priority_channels.NewByHighestAlwaysFirst(ctx, []channels.ChannelWithPriority[string]{
            channels.NewChannelWithPriority(
                "Free User - High Priority",
                freeUserHighPriorityC,
                5),
            channels.NewChannelWithPriority(
                "Free User - Low Priority",
                freeUserLowPriorityC,
                1),
        }),
        1),
}

// Two possible usage modes

// 1. Create a priority channel and receive messages from it explicitly
ch := priority_channel_groups.CombineByFrequencyRatio(ctx, priorityChannelsWithFreqRatio)
for {
    message, channelName, ok := ch.Receive()
    if !ok {
        break
    }
    fmt.Printf("%s: %s\n", channelName, message)
}

// 2. Use a dedicated function that does it behind the scenes and uses a callback function to process messages
msgProcessor := func(_ context.Context, message string, channelName string) {
    fmt.Printf("%s: %s\n", channelName, message)
}
priority_channel_groups.ProcessPriorityChannelsByFrequencyRatio(
    ctx,
    priorityChannelsWithFreqRatio,
    msgProcessor)
````

### Complex example - combination of multiple levels of priority channels

```go
urgentMessagesC := make(chan string)
flagshipProductPayingCustomerHighPriorityC := make(chan string)
flagshipProductPayingCustomerLowPriorityC := make(chan string)
flagshipProductFreeUserHighPriorityC := make(chan string)
flagshipProductFreeUserLowPriorityC := make(chan string)
nicheProductPayingCustomerHighPriorityC := make(chan string)
nicheProductPayingCustomerLowPriorityC := make(chan string)
nicheProductFreeUserHighPriorityC := make(chan string)
nicheProductFreeUserLowPriorityC := make(chan string)

flagshipProductChannelsWithFreqRatio := []priority_channel_groups.PriorityChannelWithFreqRatio[string]{
    priority_channel_groups.NewPriorityChannelWithFreqRatio(
        "Flagship Product - Paying Customer",
        priority_channels.NewByFrequencyRatio(ctx, []channels.ChannelFreqRatio[string]{
            channels.NewChannelWithFreqRatio(
                "Flagship Product - Paying Customer - High Priority",
                flagshipProductPayingCustomerHighPriorityC,
                5),
            channels.NewChannelWithFreqRatio(
                "Flagship Product - Paying Customer - Low Priority",
                flagshipProductPayingCustomerLowPriorityC,
                1),
        }),
        10),
    priority_channel_groups.NewPriorityChannelWithFreqRatio(
        "Flagship Product - Free User",
        priority_channels.NewByFrequencyRatio(ctx, []channels.ChannelFreqRatio[string]{
            channels.NewChannelWithFreqRatio(
                "Flagship Product - Free User - High Priority",
                flagshipProductFreeUserHighPriorityC,
                5),
            channels.NewChannelWithFreqRatio(
                "Flagship Product - Free User - Low Priority",
                flagshipProductFreeUserLowPriorityC,
                1),
        }),
        1),
}
flagshipProductChannelGroup := priority_channel_groups.CombineByFrequencyRatio(ctx, flagshipProductChannelsWithFreqRatio)

nicheProductChannelsWithFreqRatio := []priority_channel_groups.PriorityChannelWithFreqRatio[string]{
    priority_channel_groups.NewPriorityChannelWithFreqRatio(
        "Niche Product - Paying Customer",
        priority_channels.NewByFrequencyRatio(ctx, []channels.ChannelFreqRatio[string]{
            channels.NewChannelWithFreqRatio(
                "Niche Product - Paying Customer - High Priority",
                nicheProductPayingCustomerHighPriorityC,
                5),
            channels.NewChannelWithFreqRatio(
                "Niche Product - Paying Customer - Low Priority",
                nicheProductPayingCustomerLowPriorityC,
                1),
        }),
        10),
    priority_channel_groups.NewPriorityChannelWithFreqRatio(
        "Niche Product - Free User",
        priority_channels.NewByFrequencyRatio[string](ctx, []channels.ChannelFreqRatio[string]{
            channels.NewChannelWithFreqRatio(
                "Niche Product - Free User - High Priority",
                nicheProductFreeUserHighPriorityC,
                5),
            channels.NewChannelWithFreqRatio(
                "Niche Product - Free User - Low Priority",
                nicheProductFreeUserLowPriorityC,
                1),
        }),
        1),
}
nicheProductChannelGroup := priority_channel_groups.CombineByFrequencyRatio(ctx, nicheProductChannelsWithFreqRatio)

combinedProductsPriorityChannel := priority_channel_groups.CombineByFrequencyRatio(ctx, []priority_channel_groups.PriorityChannelWithFreqRatio[string]{
    priority_channel_groups.NewPriorityChannelWithFreqRatio(
        "Flagship Product",
        flagshipProductChannelGroup,
        5),
    priority_channel_groups.NewPriorityChannelWithFreqRatio(
        "Niche Product",
        nicheProductChannelGroup,
        1),
})
urgentMessagesPriorityChannel := priority_channels.WrapAsPriorityChannel(ctx, "Urgent Messages", urgentMessagesC)

combinedTotalPriorityChannel := priority_channel_groups.CombineByHighestPriorityFirst(ctx, []priority_channel_groups.PriorityChannelWithPriority[string]{
    priority_channel_groups.NewPriorityChannelWithPriority(
        "Urgent Messages",
        urgentMessagesPriorityChannel,
        10),
    priority_channel_groups.NewPriorityChannelWithPriority(
        "Combined Products",
        combinedProductsPriorityChannel,
        1),
})

for {
    message, channelName, ok := combinedTotalPriorityChannel.Receive()
    if !ok {
        break
    }
    fmt.Printf("%s: %s\n", channelName, message)
}

````

### Full working example with all use cases

```go

package main

import (
    "context"
    "fmt"
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
    PriorityForUrgentMessages_FrequencyRatioBetweenUsersAndOtherMessagesTypes
)

func main() {
    usagePattern := HighestPriorityAlwaysFirst

    ctx, cancel := context.WithCancel(context.Background())

    payingCustomerHighPriorityC := make(chan string)
    payingCustomerLowPriorityC := make(chan string)
    freeUserHighPriorityC := make(chan string)
    freeUserLowPriorityC := make(chan string)
    urgentMessagesC := make(chan string)

    ch := getPriorityChannelByUsagePattern(
        ctx,
        usagePattern,
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
    if usagePattern == PriorityForUrgentMessages_FrequencyRatioBetweenUsersAndOtherMessagesTypes {
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
        message, channelName, ok := ch.Receive()
        if !ok {
            break
        }
        fmt.Printf("%s: %s\n", channelName, message)
        time.Sleep(10 * time.Millisecond)
    }
}

func getPriorityChannelByUsagePattern(
    ctx context.Context,
    usagePattern UsagePattern,
    payingCustomerHighPriorityC chan string,
    payingCustomerLowPriorityC chan string,
    freeUserHighPriorityC chan string,
    freeUserLowPriorityC chan string,
    urgentMessagesC chan string,
) priority_channels.PriorityChannel[string] {

    switch usagePattern {

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
        return priority_channels.NewByHighestAlwaysFirst(ctx, channelsWithPriority)

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
        return priority_channels.NewByFrequencyRatio(ctx, channelsWithFrequencyRatio)

    case PayingCustomerAlwaysFirst_NoStarvationOfLowMessagesForSameUser:
        priorityChannelsWithPriority := []priority_channel_groups.PriorityChannelWithPriority[string]{
            priority_channel_groups.NewPriorityChannelWithPriority(
                "Paying Customer",
                priority_channels.NewByFrequencyRatio(ctx, []channels.ChannelFreqRatio[string]{
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
            priority_channel_groups.NewPriorityChannelWithPriority(
                "Free User",
                priority_channels.NewByFrequencyRatio(ctx, []channels.ChannelFreqRatio[string]{
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
        return priority_channel_groups.CombineByHighestPriorityFirst(ctx, priorityChannelsWithPriority)

    case NoStarvationOfFreeUser_HighPriorityMessagesAlwaysFirstForSameUser:
        priorityChannelsWithFreqRatio := []priority_channel_groups.PriorityChannelWithFreqRatio[string]{
            priority_channel_groups.NewPriorityChannelWithFreqRatio(
                "Paying Customer",
                priority_channels.NewByHighestAlwaysFirst(ctx, []channels.ChannelWithPriority[string]{
                    channels.NewChannelWithPriority(
                        "Paying Customer - High Priority", 
                        payingCustomerHighPriorityC, 
                        5),
                    channels.NewChannelWithPriority(
                        "Paying Customer - Low Priority",
                        payingCustomerLowPriorityC,
                        1),
                }),
                10),
            priority_channel_groups.NewPriorityChannelWithFreqRatio(
                "Free User",
                priority_channels.NewByHighestAlwaysFirst(ctx, []channels.ChannelWithPriority[string]{
                    channels.NewChannelWithPriority(
                        "Free User - High Priority", 
                        freeUserHighPriorityC,
                        5), 
                    channels.NewChannelWithPriority(
                        "Free User - Low Priority", 
                        freeUserLowPriorityC,
                        1),
                }),
                1),
        }
        return priority_channel_groups.CombineByFrequencyRatio(ctx, priorityChannelsWithFreqRatio)

    case FrequencyRatioBetweenUsers_AndFreqRatioBetweenMessageTypesForSameUser:
        priorityChannelsWithFreqRatio := []priority_channel_groups.PriorityChannelWithFreqRatio[string]{
            priority_channel_groups.NewPriorityChannelWithFreqRatio(
                "Paying Customer",
                priority_channels.NewByFrequencyRatio(ctx, []channels.ChannelFreqRatio[string]{
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
            priority_channel_groups.NewPriorityChannelWithFreqRatio(
                "Free User",
                 priority_channels.NewByFrequencyRatio(ctx, []channels.ChannelFreqRatio[string]{
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
        return priority_channel_groups.CombineByFrequencyRatio(ctx, priorityChannelsWithFreqRatio)

    case PriorityForUrgentMessages_FrequencyRatioBetweenUsersAndOtherMessagesTypes:
        priorityChannelsWithFreqRatio := []priority_channel_groups.PriorityChannelWithFreqRatio[string]{
            priority_channel_groups.NewPriorityChannelWithFreqRatio(
                "Paying Customer",
                priority_channels.NewByFrequencyRatio(ctx, []channels.ChannelFreqRatio[string]{
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
            priority_channel_groups.NewPriorityChannelWithFreqRatio(
                "Free User",
                priority_channels.NewByFrequencyRatio(ctx, []channels.ChannelFreqRatio[string]{
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
        combinedUsersAndMessageTypesPriorityChannel := priority_channel_groups.CombineByFrequencyRatio(ctx, priorityChannelsWithFreqRatio)

        return priority_channel_groups.CombineByHighestPriorityFirst(ctx, []priority_channel_groups.PriorityChannelWithPriority[string]{
            priority_channel_groups.NewPriorityChannelWithPriority(
                "Combined Users and Message Types",
                combinedUsersAndMessageTypesPriorityChannel,
                1),
            priority_channel_groups.NewPriorityChannelWithPriority(
                "Urgent Messages",
                priority_channels.WrapAsPriorityChannel(ctx, "Urgent Messages", urgentMessagesC),
                100),
        })

    default:
        return nil
    }
}
```
