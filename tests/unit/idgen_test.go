package unit

import (
	"testing"

	eba "github.com/wlz987/eba-go/src"
)

func TestUuidIDsDiffer(t *testing.T) {
	gen := eba.UuidIdGen{}
	a, b := gen.NextEnvelopeID(), gen.NextEnvelopeID()
	if a == b {
		t.Fatal("uuid ids must differ")
	}
	seg, _ := eba.TopicSegment(a)
	if mapped, _ := gen.TopicSegment(a); mapped != seg {
		t.Fatal("segment must follow standard mapping")
	}
}

func TestBadIDSegment(t *testing.T) {
	if _, err := eba.TopicSegment(eba.EnvelopeId("not-hex")); err == nil {
		t.Fatal("non-hex id must fail")
	}
}

func TestSeqIncrements(t *testing.T) {
	gen := eba.NewSeqIDGen(3)
	if gen.NextEnvelopeID() == gen.NextEnvelopeID() {
		t.Fatal("seq ids must increment")
	}
}
