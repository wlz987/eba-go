package bus

import (
	"github.com/wlz987/eba-go/src/envelope"
	"github.com/wlz987/eba-go/src/inbox"
	"github.com/wlz987/eba-go/src/pattern"
)

type Bus struct{ subs *Subscriptions }

func NewBus() *Bus { return &Bus{subs: NewSubscriptions()} }

func (b *Bus) Subscribe(patternText string, box *inbox.Inbox) error {
	pat, err := pattern.ParsePattern(patternText)
	if err != nil {
		return err
	}
	b.subs.Subscribe(pat, box)
	return nil
}

func (b *Bus) Unsubscribe(patternText string, box *inbox.Inbox) (bool, error) {
	pat, err := pattern.ParsePattern(patternText)
	if err != nil {
		return false, err
	}
	return b.subs.Unsubscribe(pat, box), nil
}

func (b *Bus) Publish(e *envelope.Envelope) error {
	parts, err := envelope.SplitTopic(e.Header.Topic)
	if err != nil {
		return err
	}
	var targets []*inbox.Inbox
	for _, box := range b.subs.SnapshotMatch(parts) {
		if !box.IsClosed() {
			targets = append(targets, box)
		}
	}
	if len(targets) == 0 {
		return nil
	}
	for _, box := range targets {
		if !box.HasRoom() {
			return &MailboxFull{BusError{Msg: "target inbox full"}}
		}
	}
	var enqueued []*inbox.Inbox
	for _, box := range targets {
		if !box.TryEnqueue(e) {
			for _, done := range enqueued {
				done.TryDropLast(e)
			}
			return &MailboxFull{BusError{Msg: "inbox reservation violated during enqueue"}}
		}
		enqueued = append(enqueued, box)
	}
	return nil
}
