package registry

import (
	"github.com/wlz987/eba-go/src/envelope"
	"github.com/wlz987/eba-go/src/result"
)

type State int

const (
	StatePending State = iota
	StateResolved
	StateTimedOut
	StateFailed
)

type ResolveOutcome struct {
	State     *State
	RequestID envelope.EnvelopeId
	Fresh     bool
}

type Entry struct {
	ExpectedTopic string
	Cause         envelope.EnvelopeId
	State         State
}

func quadOK(requestID envelope.EnvelopeId, entry *Entry, e *envelope.Envelope, gen envelope.IdGen) bool {
	if id, ok := result.ResultRequestID(e); !ok || id != requestID {
		return false
	}
	seg, err := gen.TopicSegment(requestID)
	if err != nil {
		return false
	}
	suffix, err := envelope.TopicSuffix(e.Header.Topic)
	if err != nil {
		return false
	}
	return suffix == seg &&
		e.Header.Topic == entry.ExpectedTopic &&
		e.Header.Cause == entry.Cause
}
