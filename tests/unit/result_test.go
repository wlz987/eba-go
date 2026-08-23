package unit

import (
	"testing"

	eba "github.com/wlz987/eba-go/src"
)

type remapGen struct{ eid eba.EnvelopeId }

func (g remapGen) NextEnvelopeID() eba.EnvelopeId { return g.eid }

func (remapGen) TopicSegment(id eba.EnvelopeId) (string, error) {
	return "z" + string(id[:1]), nil
}

func envOpt(topic string, payload any, opts *eba.Options) *eba.Envelope {
	e, err := eba.MakeEnvelope(topic, payload, eba.ActorId("a"), opts)
	if err != nil {
		panic(err)
	}
	return e
}

func TestOkAndErrShape(t *testing.T) {
	ok := eba.OkPayload(7)
	err := eba.ErrPayload("nope", map[string]any{"extra": 1})
	if !eba.IsResultOK(ok) {
		t.Fatal("ok shape must hold")
	}
	if v, _ := eba.ResultValue(ok); v != 7 {
		t.Fatalf("value %v", v)
	}
	if !eba.IsResultErr(err) {
		t.Fatal("err shape must hold")
	}
	if msg, _ := eba.ResultError(err); msg != "nope" {
		t.Fatalf("error %q", msg)
	}
	if _, ok := eba.ResultValue(err); ok {
		t.Fatal("err body must not yield value")
	}
	if _, ok := eba.ResultError(ok); ok {
		t.Fatal("ok body must not yield error")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("reserved key must panic")
		}
	}()
	_ = eba.ErrPayload("x", map[string]any{"ok": false})
}

func TestTimeoutBodyIsErr(t *testing.T) {
	if !eba.IsResultErr(eba.TimeoutBody) {
		t.Fatal("timeout body must be Err")
	}
	if msg, _ := eba.ResultError(eba.TimeoutBody); msg != "request_timeout" {
		t.Fatalf("error %q", msg)
	}
}

func TestLooksLikeNeedsIDSegment(t *testing.T) {
	rid := eba.EnvelopeId(aaaa32)
	gen := FixedId{rid}
	good := envOpt(mustTopic(eba.ResultTopicOf(rid, "acl.result", gen)),
		map[string]any{"request_id": string(rid), "result": eba.OkPayload(1)},
		&eba.Options{Cause: rid, IDGen: eba.NewSeqIDGen(1)})
	if !eba.LooksLikeResultEnvelope(good, gen) {
		t.Fatal("good echo must look like result")
	}
	if id, ok := eba.ResultRequestID(good); !ok || id != rid {
		t.Fatalf("request_id mismatch: %v", id)
	}
	if eba.ResultBodyOf(good) == nil {
		t.Fatal("body must extract")
	}
	bad := envOpt("acl.result.gone",
		map[string]any{"request_id": string(rid), "result": eba.OkPayload(1)},
		&eba.Options{Cause: rid, IDGen: eba.NewSeqIDGen(1)})
	if eba.LooksLikeResultEnvelope(bad, gen) {
		t.Fatal("wrong suffix must fail segment check")
	}
	plain := envOpt("x", 1, &eba.Options{IDGen: eba.NewSeqIDGen(1)})
	if eba.ResultBodyOf(plain) != nil {
		t.Fatal("plain payload has no result body")
	}
	mapped := envOpt("acl.result.za",
		map[string]any{"request_id": string(rid), "result": eba.OkPayload(1)},
		&eba.Options{Cause: rid, IDGen: eba.NewSeqIDGen(1)})
	if !eba.LooksLikeResultEnvelope(mapped, remapGen{rid}) {
		t.Fatal("segment must follow the borrowed IdGen mapping")
	}
	if eba.LooksLikeResultEnvelope(good, remapGen{rid}) {
		t.Fatal("standard segment must miss under remapped gen")
	}
}
