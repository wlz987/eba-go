package jobhost

import (
	"github.com/wlz987/eba-go/src/envelope"
	"github.com/wlz987/eba-go/src/job"
)

type SlotBook struct {
	byID    map[envelope.EnvelopeId]*job.Job
	byCause map[envelope.EnvelopeId]*job.Job
}

func NewSlotBook() *SlotBook {
	return &SlotBook{
		byID:    map[envelope.EnvelopeId]*job.Job{},
		byCause: map[envelope.EnvelopeId]*job.Job{},
	}
}

func (s *SlotBook) Adopt(j *job.Job) {
	key := j.Root.Header.ID
	if _, dup := s.byID[key]; dup {
		panic(&job.StateError{Msg: "duplicate job key: " + string(key)})
	}
	hdr := j.Root.Header
	if hdr.ID == hdr.Cause {
		if _, taken := s.rootCauses()[hdr.Cause]; taken {
			panic(&job.StateError{Msg: "duplicate job cause: " + string(hdr.Cause)})
		}
		s.byCause[hdr.Cause] = j
	}
	s.byID[key] = j
}

func (s *SlotBook) Remove(j *job.Job) {
	key := j.Root.Header.ID
	if s.byID[key] == j {
		delete(s.byID, key)
	}
	cause := j.Root.Header.Cause
	if s.byCause[cause] == j {
		delete(s.byCause, cause)
	}
}

func (s *SlotBook) Job(cause envelope.EnvelopeId) *job.Job {
	if found := s.byCause[cause]; found != nil {
		return found
	}
	return s.byID[cause]
}

func (s *SlotBook) RouteParent(cause envelope.EnvelopeId) *job.Job {
	return s.byCause[cause]
}

func (s *SlotBook) Actives() []*job.Job {
	out := make([]*job.Job, 0, len(s.byID))
	for _, j := range s.byID {
		out = append(out, j)
	}
	return out
}

func (s *SlotBook) rootCauses() map[envelope.EnvelopeId]struct{} {
	occupied := map[envelope.EnvelopeId]struct{}{}
	for _, j := range s.byID {
		hdr := j.Root.Header
		if hdr.ID == hdr.Cause {
			occupied[hdr.Cause] = struct{}{}
		}
	}
	return occupied
}
