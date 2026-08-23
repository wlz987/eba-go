package unit

import (
	"strings"
	"testing"

	eba "github.com/wlz987/eba-go/src"
)

func TestRootCauseEqualsID(t *testing.T) {
	ids := eba.NewSeqIDGen(1)
	env, err := eba.MakeEnvelope("read.x", 1, "a", &eba.Options{IDGen: ids})
	if err != nil {
		t.Fatal(err)
	}
	if env.Header.Cause != env.Header.ID {
		t.Fatalf("cause %v != id %v", env.Header.Cause, env.Header.ID)
	}
	if env.Header.From != eba.ActorId("a") {
		t.Fatal("from mismatch")
	}
}

func TestInheritedCause(t *testing.T) {
	ids := eba.NewSeqIDGen(1)
	root, _ := eba.MakeEnvelope("job.x", nil, "a", &eba.Options{IDGen: ids})
	child, _ := eba.MakeEnvelope("acl.x", nil, "a", &eba.Options{
		Cause: root.Header.Cause, IDGen: ids,
	})
	if child.Header.Cause != root.Header.ID || child.Header.ID == root.Header.ID {
		t.Fatalf("bad inheritance: child=%v root=%v", child.Header.ID, root.Header.ID)
	}
}

func TestRequestTopicMustEndWithIDSegment(t *testing.T) {
	eid := eba.EnvelopeId(strings.Repeat("a", 32))
	seg, _ := eba.TopicSegment(eid)
	env, err := eba.MakeEnvelope("echo."+seg, nil, "a", &eba.Options{ID: eid})
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(env.Header.Topic, ".")
	wantSeg, _ := eba.TopicSegment(env.Header.ID)
	if parts[len(parts)-1] != wantSeg {
		t.Fatalf("topic suffix %q != topic_segment(id) %q", parts[len(parts)-1], wantSeg)
	}
}

func TestMakeEnvelopeNeedsIDOrIDGen(t *testing.T) {
	if _, err := eba.MakeEnvelope("read.x", 1, eba.ActorId("a"), nil); err == nil {
		t.Fatal("expected error for missing id/id_gen")
	}
}

func TestSeqIdgenStable(t *testing.T) {
	ids := eba.NewSeqIDGen(1)
	a := ids.NextEnvelopeID()
	b := ids.NextEnvelopeID()
	if a == b {
		t.Fatal("ids must differ")
	}
	seg, _ := eba.TopicSegment(a)
	if !strings.HasPrefix(seg, "e") {
		t.Fatalf("segment %q lacks prefix", seg)
	}
}

func TestEmptyAndIllegalTopic(t *testing.T) {
	if _, err := eba.SplitTopic(""); err == nil {
		t.Fatal("empty topic must fail")
	}
	if _, err := eba.MakeEnvelope("Bad", 1, "a", &eba.Options{ID: eba.EnvelopeId(aaaa32)}); err == nil {
		t.Fatal("illegal topic segment must fail")
	}
}
