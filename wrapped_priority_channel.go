package priority_channels

import (
	"context"

	"github.com/dimag-jfrog/priority-channels/internal/selectable"
)

func asSelectableChannelWithWeight[T any, W any](pc *PriorityChannel[T], name string, weight W) selectable.ChannelWithWeight[T, W] {
	return &wrapCompositeChannelWithNameAndWeight[T, W]{
		overrideCompositeChannelName: overrideCompositeChannelName[T]{
			ctx:     pc.ctx,
			name:    name,
			channel: pc.compositeChannel,
		},
		weight: weight,
	}
}

type wrapCompositeChannelWithNameAndWeight[T any, W any] struct {
	overrideCompositeChannelName[T]
	weight W
}

func (w *wrapCompositeChannelWithNameAndWeight[T, W]) Weight() W {
	return w.weight
}

type overrideCompositeChannelName[T any] struct {
	ctx     context.Context
	name    string
	channel selectable.Channel[T]
}

func (oc *overrideCompositeChannelName[T]) ChannelName() string {
	return oc.name
}

func (oc *overrideCompositeChannelName[T]) NextSelectCases(upto int) ([]selectable.SelectCase[T], bool, *selectable.ClosedChannelDetails) {
	select {
	case <-oc.ctx.Done():
		return nil, true, &selectable.ClosedChannelDetails{
			ChannelName: oc.ChannelName(),
			PathInTree:  nil,
		}
	default:
		res, allSelected, closedChannel := oc.channel.NextSelectCases(upto)
		if closedChannel != nil {
			closedChannel.PathInTree[len(closedChannel.PathInTree)-1].ChannelName = oc.name
			return res, allSelected, closedChannel
		}
		for i, sc := range res {
			if len(sc.PathInTree) > 0 {
				res[i].PathInTree[len(res[i].PathInTree)-1].ChannelName = oc.name
			}
		}
		return res, allSelected, nil
	}
}

func (oc *overrideCompositeChannelName[T]) UpdateOnCaseSelected(pathInTree []selectable.ChannelNode) {
	oc.channel.UpdateOnCaseSelected(pathInTree)
}
