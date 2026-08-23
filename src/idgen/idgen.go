package idgen

import (
	"fmt"

	"github.com/wlz987/eba-go/src/envelope"
)

type UuidIdGen struct{}

func (UuidIdGen) NextEnvelopeID() envelope.EnvelopeId {
	return envelope.NewEnvelopeID()
}

func (UuidIdGen) TopicSegment(id envelope.EnvelopeId) (string, error) {
	return envelope.TopicSegment(id)
}

type SeqIdGen struct{ next uint64 }

func NewSeqIDGen(start uint64) *SeqIdGen {
	if start < 1 {
		start = 1
	}
	return &SeqIdGen{next: start}
}

func (g *SeqIdGen) NextEnvelopeID() envelope.EnvelopeId {
	n := g.next
	g.next++
	return envelope.EnvelopeId(fmt.Sprintf("%032x", n))
}

func (SeqIdGen) TopicSegment(id envelope.EnvelopeId) (string, error) {
	return envelope.TopicSegment(id)
}
