package integration

import (
	"strings"
	"testing"

	eba "github.com/wlz987/eba-go/src"
)

func TestSyncBeginReplyFinish(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID:  actorH,
		Inbox:    eba.NewInbox(8),
		Patterns: []string{"read"},
		MakeJob:  func(root *eba.Envelope) *eba.Job { return newReadJob(host, root) },
	})
	if err := host.Start(bus); err != nil {
		t.Fatal(err)
	}
	dest := eba.NewInbox(8)
	if err := bus.Subscribe("read.result.**", dest); err != nil {
		t.Fatal(err)
	}
	if err := host.Handle(bus, envGen("read", "buf.a", gen), clk, gen); err != nil {
		t.Fatal(err)
	}
	got := dest.TryRecv()
	if got == nil {
		t.Fatal("reply not delivered")
	}
	v, ok := eba.ResultValue(got.Payload.(map[string]any)["result"])
	if !ok || v != "buf.a" {
		t.Fatalf("unexpected reply value %v", got.Payload)
	}
	if host.Job(got.Header.Cause) != nil {
		t.Fatal("job must be released after finish")
	}
}

func TestRequestThenStage(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var states []*stepState
	parent := newStepHost(t, eba.NewInbox(8), 0, 0, &states)
	var leaf *eba.JobHost
	leaf = eba.NewJobHost(eba.JobHostParams{
		ActorID:  actorL,
		Inbox:    eba.NewInbox(8),
		Patterns: []string{"acl.*"},
		MakeJob:  func(root *eba.Envelope) *eba.Job { return newEchoJob(leaf, root) },
	})
	if err := parent.Start(bus); err != nil {
		t.Fatal(err)
	}
	if err := leaf.Start(bus); err != nil {
		t.Fatal(err)
	}
	dest := eba.NewInbox(8)
	_ = bus.Subscribe("job.result.**", dest)

	root := envGen("job", map[string]any{}, gen)
	if err := parent.Handle(bus, root, clk, gen); err != nil {
		t.Fatal(err)
	}
	child := leaf.Inbox().TryRecv()
	if child == nil {
		t.Fatal("leaf must receive the request")
	}
	if child.Header.Cause != root.Header.Cause {
		t.Fatalf("child cause %v != root %v", child.Header.Cause, root.Header.Cause)
	}
	if err := leaf.Handle(bus, child, clk, gen); err != nil {
		t.Fatal(err)
	}
	result := parent.Inbox().TryRecv()
	if result == nil {
		t.Fatal("parent must receive the stage result")
	}
	if err := parent.Handle(bus, result, clk, gen); err != nil {
		t.Fatal(err)
	}
	done := dest.TryRecv()
	if done == nil {
		t.Fatal("root reply not delivered")
	}
	if !eba.IsResultOK(done.Payload.(map[string]any)["result"]) {
		t.Fatal("root reply must be Ok")
	}
}

func TestSameInboxRequestDrains(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID:  actorH,
		Inbox:    eba.NewInbox(8),
		Patterns: []string{"job", "acl.*"},
		MakeJob: func(root *eba.Envelope) *eba.Job {
			if strings.HasPrefix(root.Header.Topic, "acl") {
				return newEchoJob(host, root)
			}
			j, _ := newStepJob(host, root)
			return j
		},
	})
	if err := host.Start(bus); err != nil {
		t.Fatal(err)
	}
	dest := eba.NewInbox(8)
	_ = bus.Subscribe("job.result.**", dest)
	if err := host.Handle(bus, envGen("job", map[string]any{}, gen), clk, gen); err != nil {
		t.Fatal(err)
	}
	done := dest.TryRecv()
	if done == nil {
		t.Fatal("same-inbox drain must finish the job in one handle")
	}
	if !eba.IsResultOK(done.Payload.(map[string]any)["result"]) {
		t.Fatal("root reply must be Ok")
	}
}

func TestReentrantHandleRejected(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID:  actorH,
		Inbox:    eba.NewInbox(8),
		Patterns: []string{"job"},
		MakeJob: func(root *eba.Envelope) *eba.Job {
			j := eba.NewJob(host, root)
			j.OnBegin = func(*eba.Job) error {
				_, err := host.Poll(bus, clk, gen)
				return err
			}
			return j
		},
	})
	if err := host.Start(bus); err != nil {
		t.Fatal(err)
	}
	defer func() {
		r := recover()
		se, ok := r.(*eba.StateError)
		if !ok || !strings.Contains(se.Error(), "reentrant") {
			t.Fatalf("expected reentrant StateError, got %v", r)
		}
	}()
	_ = host.Handle(bus, envGen("job", map[string]any{}, gen), clk, gen)
	t.Fatal("reentrant handle must panic")
}

func TestClockWatchdog(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var states []*stepState
	parent := newStepHost(t, eba.NewInbox(8), 10, 0, &states)
	if err := parent.Start(bus); err != nil {
		t.Fatal(err)
	}
	root := envGen("job", map[string]any{}, gen)
	if err := parent.Handle(bus, root, clk, gen); err != nil {
		t.Fatal(err)
	}
	job := parent.Job(root.Header.Cause)
	if job == nil {
		t.Fatal("job must be on stage")
	}
	clk.Advance(10)
	ok, err := parent.Poll(bus, clk, gen)
	if err != nil || ok {
		t.Fatalf("poll: %v %v", ok, err)
	}
	if len(states[0].seen) != 1 || states[0].seen[0] != "acl" || !job.IsFinished() {
		t.Fatalf("watchdog must time out with TIMEOUT stage; seen=%v finished=%v",
			states[0].seen, job.IsFinished())
	}
}

func TestDeferredReplyRoundtrip(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	begins := 0
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID:  actorH,
		Inbox:    eba.NewInbox(8),
		Patterns: []string{"wait"},
		MakeJob: func(root *eba.Envelope) *eba.Job {
			return newWaitJob(host, root, &begins)
		},
	})
	if err := host.Start(bus); err != nil {
		t.Fatal(err)
	}
	dest := eba.NewInbox(8)
	held := eba.NewInbox(8)
	_ = bus.Subscribe("wait.result.**", dest)
	_ = bus.Subscribe("ext.wait.**", held)
	root := envGen("wait", map[string]any{}, gen)
	if err := host.Handle(bus, root, clk, gen); err != nil {
		t.Fatal(err)
	}
	if host.Job(root.Header.Cause) == nil || begins != 1 {
		t.Fatal("wait job must stay active after request")
	}
	req := held.TryRecv()
	if req == nil {
		t.Fatal("delayed replier must see the held request")
	}
	if err := eba.NewMatchmaker(req).Reply(bus, eba.OkPayload("ready"), "ext.wait.result", actorL, gen); err != nil {
		t.Fatal(err)
	}
	result := host.Inbox().TryRecv()
	if result == nil {
		t.Fatal("result must land in host inbox")
	}
	if err := host.Handle(bus, result, clk, gen); err != nil {
		t.Fatal(err)
	}
	if dest.TryRecv() == nil {
		t.Fatal("root reply must fire")
	}
	if begins != 1 {
		t.Fatal("must not re-enter begin")
	}
	if host.Job(root.Header.Cause) != nil {
		t.Fatal("job must finish")
	}
}

func TestDeferredReplyConjunction(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID:  actorH,
		Inbox:    eba.NewInbox(8),
		Patterns: []string{"job"},
		MakeJob: func(root *eba.Envelope) *eba.Job {
			return newTwoWaitJob(host, root)
		},
	})
	if err := host.Start(bus); err != nil {
		t.Fatal(err)
	}
	aBox, bBox := eba.NewInbox(8), eba.NewInbox(8)
	_ = bus.Subscribe("ext.a.**", aBox)
	_ = bus.Subscribe("ext.b.**", bBox)
	root := envGen("job", map[string]any{}, gen)
	if err := host.Handle(bus, root, clk, gen); err != nil {
		t.Fatal(err)
	}
	ra, rb := aBox.TryRecv(), bBox.TryRecv()
	if ra == nil || rb == nil {
		t.Fatal("both held requests")
	}
	if err := eba.NewMatchmaker(ra).Reply(bus, eba.OkPayload(1), "ext.a.result", actorL, gen); err != nil {
		t.Fatal(err)
	}
	if err := eba.NewMatchmaker(rb).Reply(bus, eba.OkPayload(2), "ext.b.result", actorL, gen); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		res := host.Inbox().TryRecv()
		if res == nil {
			t.Fatal("stage result")
		}
		if err := host.Handle(bus, res, clk, gen); err != nil {
			t.Fatal(err)
		}
	}
	if host.Job(root.Header.Cause) != nil {
		t.Fatal("conjunction must finish")
	}
}

func TestDeferredReplyLateAfterTimeout(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var states []*stepState
	parent := newStepHost(t, eba.NewInbox(8), 5, 0, &states)
	if err := parent.Start(bus); err != nil {
		t.Fatal(err)
	}
	held := eba.NewInbox(8)
	_ = bus.Subscribe("acl.**", held)
	root := envGen("job", map[string]any{}, gen)
	if err := parent.Handle(bus, root, clk, gen); err != nil {
		t.Fatal(err)
	}
	req := held.TryRecv()
	if req == nil {
		t.Fatal("held request")
	}
	clk.Advance(5)
	if _, err := parent.Poll(bus, clk, gen); err != nil {
		t.Fatal(err)
	}
	if err := eba.NewMatchmaker(req).Reply(bus, eba.OkPayload("late"), "acl.result", actorL, gen); err != nil {
		t.Fatal(err)
	}
	if late := parent.Inbox().TryRecv(); late != nil {
		_ = parent.Handle(bus, late, clk, gen)
	}
	job := parent.Job(root.Header.Cause)
	if job != nil && !job.IsFinished() {
		t.Fatal("late reply must not revive the job")
	}
}

func TestRootCauseUnique(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	begins := 0
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID:  actorH,
		Inbox:    eba.NewInbox(8),
		Patterns: []string{"wait"},
		MakeJob: func(root *eba.Envelope) *eba.Job {
			return newWaitJob(host, root, &begins)
		},
	})
	root := envGen("wait", map[string]any{}, gen)
	if err := host.Handle(bus, root, clk, gen); err != nil {
		t.Fatal(err)
	}
	defer func() {
		r := recover()
		se, ok := r.(*eba.StateError)
		if !ok || !containsStr(se.Error(), "duplicate") {
			t.Fatalf("expected duplicate StateError, got %v", r)
		}
	}()
	dup := env("wait", map[string]any{}, &eba.Options{ID: root.Header.ID, Cause: root.Header.Cause, IDGen: gen})
	_ = host.Handle(bus, dup, clk, gen)
	t.Fatal("duplicate cause must panic")
}

func TestLeafIDsShareCause(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var leaf *eba.JobHost
	leaf = eba.NewJobHost(eba.JobHostParams{
		ActorID:  actorL,
		Inbox:    eba.NewInbox(8),
		Patterns: []string{"acl.*"},
		MakeJob:  func(root *eba.Envelope) *eba.Job { return newEchoJob(leaf, root) },
	})
	cause := gen.NextEnvelopeID()
	a, b := gen.NextEnvelopeID(), gen.NextEnvelopeID()
	segA, _ := eba.TopicSegment(a)
	segB, _ := eba.TopicSegment(b)
	if err := leaf.Handle(bus, env("acl."+segA, 1, &eba.Options{ID: a, Cause: cause}), clk, gen); err != nil {
		t.Fatal(err)
	}
	if err := leaf.Handle(bus, env("acl."+segB, 2, &eba.Options{ID: b, Cause: cause}), clk, gen); err != nil {
		t.Fatal(err)
	}
	if leaf.Job(a) != nil || leaf.Job(b) != nil {
		t.Fatal("leaf jobs finish immediately and leave no slot")
	}
}

func TestUnmatchedAndTwoActive(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var states []*stepState
	host := newStepHost(t, eba.NewInbox(8), 0, 1, &states)
	if err := host.Start(bus); err != nil {
		t.Fatal(err)
	}
	if err := host.Handle(bus, envGen("other", 1, gen), clk, gen); err != nil {
		t.Fatal(err)
	}
	first := envGen("job", map[string]any{}, gen)
	second := envGen("job", map[string]any{"pad": 1}, gen)
	if err := host.Handle(bus, first, clk, gen); err != nil {
		t.Fatal(err)
	}
	if err := host.Handle(bus, second, clk, gen); err != nil {
		t.Fatal(err)
	}
	if host.Job(first.Header.Cause) == nil || host.Job(second.Header.Cause) == nil {
		t.Fatal("two active jobs must coexist on stage")
	}
}

func TestResultWhenQueueFull(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var states []*stepState
	host := newStepHost(t, eba.NewInbox(8), 0, 1, &states)
	if err := host.Start(bus); err != nil {
		t.Fatal(err)
	}
	root := envGen("job", map[string]any{}, gen)
	if err := host.Handle(bus, root, clk, gen); err != nil {
		t.Fatal(err)
	}
	job := host.Job(root.Header.Cause)
	childID := states[0].childID
	if childID == "" {
		t.Fatal("child request must be issued")
	}
	if err := host.Handle(bus, envGen("job", map[string]any{"pad": 1}, gen), clk, gen); err != nil {
		t.Fatal(err)
	}
	resultTopic := mustTopic(eba.ResultTopicOf(childID, "acl.result", gen))
	echo := env(resultTopic,
		map[string]any{"request_id": string(childID), "result": eba.OkPayload("allow")},
		&eba.Options{Cause: root.Header.Cause, IDGen: gen})
	if err := host.Handle(bus, echo, clk, gen); err != nil {
		t.Fatal(err)
	}
	if !job.IsFinished() {
		t.Fatal("result routed at queue limit must finish the job")
	}
	if id, ok := eba.ResultRequestID(echo); !ok || id != childID {
		t.Fatalf("request_id roundtrip mismatch: %v %v", id, ok)
	}
}
