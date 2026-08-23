package publisher

import (
	"github.com/wlz987/eba-go/src/bus"
	"github.com/wlz987/eba-go/src/envelope"
)

type Publisher struct{ ActorID envelope.ActorId }

func New(actor envelope.ActorId) *Publisher { return &Publisher{ActorID: actor} }

func (p *Publisher) Publish(bus *bus.Bus, e *envelope.Envelope) error {
	return bus.Publish(e)
}
