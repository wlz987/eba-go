package reply

import (
	"github.com/wlz987/eba-go/src/bus"
	"github.com/wlz987/eba-go/src/envelope"
	"github.com/wlz987/eba-go/src/registry"
	"github.com/wlz987/eba-go/src/result"
)

func Reply(
	bus *bus.Bus,
	request *envelope.Envelope,
	body result.Body,
	resultPrefix string,
	from envelope.ActorId,
	gen envelope.IdGen,
) error {
	topic, err := registry.ResultTopicOf(request.Header.ID, resultPrefix, gen)
	if err != nil {
		return err
	}
	e, err := envelope.MakeEnvelope(topic,
		map[string]any{"request_id": string(request.Header.ID), "result": map[string]any(body)},
		from, &envelope.Options{Cause: request.Header.Cause, IDGen: gen})
	if err != nil {
		return err
	}
	return bus.Publish(e)
}
