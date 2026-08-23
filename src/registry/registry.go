package registry

import (
	"github.com/wlz987/eba-go/src/bus"
	"github.com/wlz987/eba-go/src/envelope"
	"github.com/wlz987/eba-go/src/inbox"
	"github.com/wlz987/eba-go/src/result"
)

type Registry struct {
	entries map[envelope.EnvelopeId]*Entry
	idGen   envelope.IdGen
}

func NewRegistry() *Registry {
	return &Registry{entries: map[envelope.EnvelopeId]*Entry{}}
}

func (r *Registry) BindIDGen(gen envelope.IdGen) { r.idGen = gen }

type StartParams struct {
	RequestPrefix string
	ResultPrefix  string
	Payload       any
	From          envelope.ActorId
	Cause         envelope.EnvelopeId
	RequestID     envelope.EnvelopeId
}

func (r *Registry) StartRequest(
	bus *bus.Bus, box *inbox.Inbox, gen envelope.IdGen, p StartParams,
) (*envelope.Envelope, error) {
	r.BindIDGen(gen)
	eid := p.RequestID
	if eid == "" {
		eid = gen.NextEnvelopeID()
	}
	seg, err := gen.TopicSegment(eid)
	if err != nil {
		return nil, err
	}
	request, err := envelope.MakeEnvelope(p.RequestPrefix+"."+seg, p.Payload, p.From,
		&envelope.Options{Cause: p.Cause, ID: eid})
	if err != nil {
		return nil, err
	}
	resultTopic := p.ResultPrefix + "." + seg
	if err := r.expect(request, resultTopic, gen); err != nil {
		return nil, err
	}
	if err := bus.Subscribe(resultTopic, box); err != nil {
		r.drop(eid)
		return nil, err
	}
	if err := bus.Publish(request); err != nil {
		r.setTerminal(eid, StateFailed)
		r.FinishSafe(bus, box, eid)
		return nil, err
	}
	return request, nil
}

func (r *Registry) ResolveOnly(e *envelope.Envelope) ResolveOutcome {
	if r.idGen == nil {
		panic("registry: IdGen bound by start_request or JobHost.handle")
	}
	requestID, ok := result.ResultRequestID(e)
	var prior *State
	if ok {
		if st, has := r.State(requestID); has {
			prior = &st
		}
	}
	state := r.applyQuad(e, requestID, ok)
	fresh := prior != nil && *prior == StatePending && state != nil && *state == StateResolved
	return ResolveOutcome{State: state, RequestID: requestIDOrEmpty(requestID, ok), Fresh: fresh}
}

func (r *Registry) FinishSafe(bus *bus.Bus, box *inbox.Inbox, requestID envelope.EnvelopeId) {
	entry := r.entries[requestID]
	if entry != nil {
		_, _ = bus.Unsubscribe(entry.ExpectedTopic, box)
	}
	r.drop(requestID)
}

func (r *Registry) State(requestID envelope.EnvelopeId) (State, bool) {
	entry, ok := r.entries[requestID]
	if !ok {
		return 0, false
	}
	return entry.State, true
}

func (r *Registry) Timeout(requestID envelope.EnvelopeId) {
	r.setTerminal(requestID, StateTimedOut)
}

func (r *Registry) expect(request *envelope.Envelope, expectedTopic string, gen envelope.IdGen) error {
	requestID := request.Header.ID
	segment, err := gen.TopicSegment(requestID)
	if err != nil {
		return err
	}
	suffix, err := envelope.TopicSuffix(expectedTopic)
	if err != nil {
		return err
	}
	if suffix != segment {
		return &envelope.EnvelopeBuildError{Msg: "expected_topic " + expectedTopic +
			" does not end with topic_segment(request_id) " + segment}
	}
	r.drop(requestID)
	r.entries[requestID] = &Entry{ExpectedTopic: expectedTopic, Cause: request.Header.Cause}
	return nil
}

func (r *Registry) applyQuad(e *envelope.Envelope, requestID envelope.EnvelopeId, has bool) *State {
	if !has {
		return nil
	}
	entry, ok := r.entries[requestID]
	if !ok || !quadOK(requestID, entry, e, r.idGen) || result.ResultBody(e) == nil {
		return nil
	}
	if entry.State != StatePending {
		st := entry.State
		return &st
	}
	entry.State = StateResolved
	st := StateResolved
	return &st
}

func (r *Registry) drop(requestID envelope.EnvelopeId) {
	delete(r.entries, requestID)
}

func (r *Registry) setTerminal(requestID envelope.EnvelopeId, state State) {
	entry, ok := r.entries[requestID]
	if !ok || entry.State != StatePending {
		return
	}
	entry.State = state
}

func requestIDOrEmpty(id envelope.EnvelopeId, ok bool) envelope.EnvelopeId {
	if ok {
		return id
	}
	return ""
}
