package reply

import (
	"github.com/wlz987/eba-go/src/bus"
	"github.com/wlz987/eba-go/src/envelope"
	"github.com/wlz987/eba-go/src/result"
)

// Matchmaker 扣住请求信封；Reply 仍走会合，不另开认答路线。
type Matchmaker struct {
	Request *envelope.Envelope
}

func NewMatchmaker(request *envelope.Envelope) *Matchmaker {
	return &Matchmaker{Request: request}
}

func (m *Matchmaker) Reply(
	bus *bus.Bus,
	body result.Body,
	resultPrefix string,
	from envelope.ActorId,
	gen envelope.IdGen,
) error {
	return Reply(bus, m.Request, body, resultPrefix, from, gen)
}
