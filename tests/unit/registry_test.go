package unit

import (
	"testing"

	eba "github.com/wlz987/eba-go/src"
)

type FixedId struct{ eid eba.EnvelopeId }

func (f FixedId) NextEnvelopeID() eba.EnvelopeId { return f.eid }

func (f FixedId) TopicSegment(id eba.EnvelopeId) (string, error) {
	return eba.TopicSegment(id)
}

func startFixed(t *testing.T) (*eba.Bus, *eba.Inbox, *eba.Registry, eba.EnvelopeId) {
	t.Helper()
	bus, box, reg := eba.NewBus(), eba.NewInbox(4), eba.NewRegistry()
	rid := eba.EnvelopeId(aaaa32)
	_, err := reg.StartRequest(bus, box, FixedId{rid}, eba.StartParams{
		RequestPrefix: "acl", ResultPrefix: "acl.result",
		Payload: nil, From: "h", Cause: rid,
	})
	if err != nil {
		t.Fatal(err)
	}
	return bus, box, reg, rid
}

func echoResult(rid eba.EnvelopeId, cause eba.EnvelopeId, value any) *eba.Envelope {
	e, err := eba.MakeEnvelope(
		mustTopic(eba.ResultTopicOf(rid, "acl.result", FixedId{rid})),
		map[string]any{"request_id": string(rid), "result": eba.OkPayload(value)},
		"l", &eba.Options{Cause: cause, IDGen: eba.NewSeqIDGen(1)},
	)
	if err != nil {
		panic(err)
	}
	return e
}

func mustTopic(topic string, err error) string {
	if err != nil {
		panic(err)
	}
	return topic
}

func TestQuadMissKeepsPending(t *testing.T) {
	bus, _, reg, rid := startFixed(t)
	echo := echoResult(rid, eba.EnvelopeId(bbbb32), 1)
	out := reg.ResolveOnly(echo)
	if out.Fresh {
		t.Fatal("mismatched cause must not resolve")
	}
	if st, ok := reg.State(rid); !ok || st != eba.StatePending {
		t.Fatal("entry must stay pending")
	}
	_ = bus
}

func TestQuadHitFresh(t *testing.T) {
	_, _, reg, rid := startFixed(t)
	reply := echoResult(rid, rid, "ok")
	out := reg.ResolveOnly(reply)
	if !out.Fresh || out.State == nil || *out.State != eba.StateResolved {
		t.Fatalf("expected fresh resolution, got %+v", out)
	}
}

func TestFinishSafeUnsubscribes(t *testing.T) {
	bus, box, reg, rid := startFixed(t)
	_ = mustTopic(eba.ResultTopicOf(rid, "acl.result", FixedId{rid}))
	reg.FinishSafe(bus, box, rid)
	if err := bus.Publish(echoResult(rid, rid, 1)); err != nil {
		t.Fatal(err)
	}
	if box.TryRecv() != nil {
		t.Fatal("finish_safe must unsubscribe the result topic")
	}
}

func TestLateEchoNotFreshThenFinish(t *testing.T) {
	bus, box, reg, rid := startFixed(t)
	reply := echoResult(rid, rid, "ok")
	if !reg.ResolveOnly(reply).Fresh {
		t.Fatal("first echo must resolve fresh")
	}
	again := reg.ResolveOnly(reply)
	if again.Fresh || again.State == nil || *again.State != eba.StateResolved {
		t.Fatalf("second echo must be stale: %+v", again)
	}
	reg.FinishSafe(bus, box, rid)
	if _, ok := reg.State(rid); ok {
		t.Fatal("finish_safe must drop the entry")
	}
}

func TestResolveWithoutBindRaises(t *testing.T) {
	defer func() {
		r, ok := recover().(string)
		if !ok || !contains(r, "IdGen") {
			t.Fatalf("expected IdGen bind panic, got %v", r)
		}
	}()
	plain, _ := eba.MakeEnvelope("x", 1, eba.ActorId("a"),
		&eba.Options{ID: eba.EnvelopeId(aaaa32)})
	eba.NewRegistry().ResolveOnly(plain)
}

func TestStartRequestPublishFullCleansUp(t *testing.T) {
	bus := eba.NewBus()
	dest := eba.NewInbox(1)
	pad, _ := eba.MakeEnvelope("pad", 0, eba.ActorId("a"),
		&eba.Options{ID: eba.EnvelopeId(cccc32)})
	_ = dest.TryEnqueue(pad)
	_ = bus.Subscribe("acl.**", dest)
	box := eba.NewInbox(4)
	reg := eba.NewRegistry()
	rid := eba.EnvelopeId(aaaa32)
	_, err := reg.StartRequest(bus, box, FixedId{rid}, eba.StartParams{
		RequestPrefix: "acl", ResultPrefix: "acl.result",
		Payload: nil, From: "h", Cause: rid,
	})
	if _, full := err.(*eba.MailboxFull); !full {
		t.Fatalf("expected MailboxFull, got %v", err)
	}
	if _, ok := reg.State(rid); ok {
		t.Fatal("failed start_request must clean up its entry")
	}
}

func TestTopicSuffixMiss(t *testing.T) {
	bus, _, reg, rid := startFixed(t)
	echo, _ := eba.MakeEnvelope("acl.result.other",
		map[string]any{"request_id": string(rid), "result": eba.OkPayload(1)},
		"l", &eba.Options{Cause: rid, IDGen: eba.NewSeqIDGen(1)})
	out := reg.ResolveOnly(echo)
	if out.Fresh {
		t.Fatal("suffix miss must not resolve")
	}
	if st, ok := reg.State(rid); !ok || st != eba.StatePending {
		t.Fatal("entry must stay pending after suffix miss")
	}
	_ = bus
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

const bbbb32 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
const cccc32 = "cccccccccccccccccccccccccccccccc"
