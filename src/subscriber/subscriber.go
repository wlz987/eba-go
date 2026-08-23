package subscriber

import (
	"github.com/wlz987/eba-go/src/bus"
	"github.com/wlz987/eba-go/src/envelope"
	"github.com/wlz987/eba-go/src/inbox"
)

type Subscriber struct {
	ActorID  envelope.ActorId
	Inbox    *inbox.Inbox
	Patterns []string
}

func New(actor envelope.ActorId, box *inbox.Inbox, patterns []string) *Subscriber {
	box.NoteReader(actor)
	return &Subscriber{ActorID: actor, Inbox: box, Patterns: patterns}
}

func (s *Subscriber) Start(bus *bus.Bus) error {
	for _, p := range s.Patterns {
		if err := bus.Subscribe(p, s.Inbox); err != nil {
			return err
		}
	}
	return nil
}

func (s *Subscriber) TryRecv() *envelope.Envelope { return s.Inbox.TryRecv() }
