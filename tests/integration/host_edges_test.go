package integration

import (
	"testing"

	eba "github.com/wlz987/eba-go/src"
)

func TestAcceptNarrowerThanSubscribe(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var states []*stepState
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID: actorH, Inbox: eba.NewInbox(8),
		Patterns: []string{"job.**"}, Accept: []string{"job"},
		MakeJob: func(root *eba.Envelope) *eba.Job {
			j, st := newStepJob(host, root)
			states = append(states, st)
			return j
		},
	})
	if err := host.Start(bus); err != nil {
		t.Fatal(err)
	}
	extra := envGen("job.extra", map[string]any{}, gen)
	if err := host.Handle(bus, extra, clk, gen); err != nil {
		t.Fatal(err)
	}
	if host.Job(extra.Header.Cause) != nil {
		t.Fatal("narrower accept must skip job.extra")
	}
	root := envGen("job", map[string]any{}, gen)
	if err := host.Handle(bus, root, clk, gen); err != nil {
		t.Fatal(err)
	}
	if host.Job(root.Header.Cause) == nil {
		t.Fatal("accepted root must be adopted")
	}
}

func TestFinishIdempotent(t *testing.T) {
	var ends []bool
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID: actorH, Inbox: eba.NewInbox(8), Patterns: []string{"job"},
		MakeJob: func(root *eba.Envelope) *eba.Job {
			j := eba.NewJob(host, root)
			j.OnBegin = func(j *eba.Job) error {
				if err := j.Finish(true, "", 1); err != nil {
					return err
				}
				return j.Finish(false, "no", nil)
			}
			j.OnFinished = func(*eba.Job, bool, string, any) error {
				ends = append(ends, true)
				return nil
			}
			return j
		},
	})
	root := envGen("job", map[string]any{}, gen)
	if err := host.Handle(bus, root, clk, gen); err != nil {
		t.Fatal(err)
	}
	if len(ends) != 1 {
		t.Fatalf("on_finished must fire once, got %v", ends)
	}
	if host.Job(root.Header.Cause) != nil {
		t.Fatal("finished job must leave the slot")
	}
}

func TestBeginErrorDropsSlot(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID: actorH, Inbox: eba.NewInbox(8), Patterns: []string{"job"},
		MakeJob: func(root *eba.Envelope) *eba.Job {
			j := eba.NewJob(host, root)
			j.OnBegin = func(*eba.Job) error { return &boomError{} }
			return j
		},
	})
	root := envGen("job", map[string]any{}, gen)
	if _, isBoom := host.Handle(bus, root, clk, gen).(*boomError); !isBoom {
		t.Fatal("begin error must surface from handle")
	}
	if host.Job(root.Header.Cause) != nil {
		t.Fatal("failed begin must drop the slot")
	}
}

type boomError struct{}

func (*boomError) Error() string { return "boom" }

func TestNoTimeoutDoesNotExpire(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var states []*stepState
	parent := newStepHost(t, eba.NewInbox(8), 0, 0, &states)
	if err := parent.Start(bus); err != nil {
		t.Fatal(err)
	}
	root := envGen("job", map[string]any{}, gen)
	if err := parent.Handle(bus, root, clk, gen); err != nil {
		t.Fatal(err)
	}
	job := parent.Job(root.Header.Cause)
	if job == nil {
		t.Fatal("job must be active")
	}
	clk.Advance(10_000)
	if _, err := parent.Poll(bus, clk, gen); err != nil {
		t.Fatal(err)
	}
	if job.IsFinished() || parent.Job(root.Header.Cause) != job {
		t.Fatal("without timeout the watchdog must leave the job alone")
	}
}

func TestLateResultAfterTimeout(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var states []*stepState
	parent := newStepHost(t, eba.NewInbox(8), 5, 0, &states)
	if err := parent.Start(bus); err != nil {
		t.Fatal(err)
	}
	root := envGen("job", map[string]any{}, gen)
	if err := parent.Handle(bus, root, clk, gen); err != nil {
		t.Fatal(err)
	}
	job := parent.Job(root.Header.Cause)
	childID := states[0].childID
	if job == nil || childID == "" {
		t.Fatal("child request must be issued")
	}
	clk.Advance(5)
	if _, err := parent.Poll(bus, clk, gen); err != nil {
		t.Fatal(err)
	}
	if !job.IsFinished() || len(states[0].seen) != 1 || states[0].seen[0] != "acl" {
		t.Fatalf("watchdog must deliver TIMEOUT_BODY: seen=%v", states[0].seen)
	}
	if msg, _ := eba.ResultError(eba.TimeoutBody); msg != "request_timeout" ||
		!eba.IsResultErr(eba.TimeoutBody) {
		t.Fatal("timeout body must be Err(request_timeout)")
	}
	resultTopic := mustTopic(eba.ResultTopicOf(childID, "acl.result", gen))
	echo := env(resultTopic,
		map[string]any{"request_id": string(childID), "result": eba.OkPayload("late")},
		&eba.Options{Cause: root.Header.Cause, IDGen: gen})
	if err := parent.Handle(bus, echo, clk, gen); err != nil {
		t.Fatal(err)
	}
	if parent.Job(root.Header.Cause) != nil {
		t.Fatal("late result must find no live job")
	}
}

func TestMaxInflight(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID: actorH, Inbox: eba.NewInbox(8), Patterns: []string{"job"},
		MakeJob: func(root *eba.Envelope) *eba.Job {
			j := eba.NewJob(host, root)
			j.SetMaxInflight(1)
			j.OnBegin = func(j *eba.Job) error {
				spec := eba.RequestSpec{Stage: "a", RequestPrefix: "acl",
					ResultPrefix: "acl.result"}
				if _, err := j.Request(withPayload(spec, 1)); err != nil {
					return err
				}
				_, err := j.Request(withPayload(spec, 2))
				return err
			}
			return j
		},
	})
	if err := host.Start(bus); err != nil {
		t.Fatal(err)
	}
	err := host.Handle(bus, envGen("job", map[string]any{}, gen), clk, gen)
	if bp, ok := err.(*eba.BackpressureError); !ok {
		t.Fatalf("expected BackpressureError, got %v", err)
	} else if bp.Reason != "max_inflight" {
		t.Fatalf("reason %q", bp.Reason)
	}
}

func withPayload(spec eba.RequestSpec, payload any) eba.RequestSpec {
	spec.Payload = payload
	return spec
}

func TestContinuationDefersSecondRequest(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var stages []string
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID: actorH, Inbox: eba.NewInbox(8), Patterns: []string{"job"},
		MakeJob: func(root *eba.Envelope) *eba.Job {
			j := eba.NewJob(host, root)
			j.OnBegin = func(j *eba.Job) error {
				_, err := j.Request(eba.RequestSpec{Stage: "one",
					RequestPrefix: "acl", ResultPrefix: "acl.result", Payload: 1})
				return err
			}
			j.OnStageResult = func(j *eba.Job, stage string, body eba.ResultBody) error {
				stages = append(stages, stage)
				if stage == "one" {
					_, err := j.Request(eba.RequestSpec{Stage: "two",
						RequestPrefix: "acl", ResultPrefix: "acl.result", Payload: 2})
					return err
				}
				return j.Finish(true, "", stage)
			}
			return j
		},
	})
	var leaf *eba.JobHost
	leaf = eba.NewJobHost(eba.JobHostParams{
		ActorID: actorL, Inbox: eba.NewInbox(8), Patterns: []string{"acl.*"},
		MakeJob: func(root *eba.Envelope) *eba.Job { return newEchoJob(leaf, root) },
	})
	if err := host.Start(bus); err != nil {
		t.Fatal(err)
	}
	if err := leaf.Start(bus); err != nil {
		t.Fatal(err)
	}
	root := envGen("job", map[string]any{}, gen)
	if err := host.Handle(bus, root, clk, gen); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		req := leaf.Inbox().TryRecv()
		if req == nil {
			t.Fatalf("round %d: leaf must receive the request", i)
		}
		if err := leaf.Handle(bus, req, clk, gen); err != nil {
			t.Fatal(err)
		}
		r := host.Inbox().TryRecv()
		if r == nil {
			t.Fatalf("round %d: parent must receive the stage result", i)
		}
		if err := host.Handle(bus, r, clk, gen); err != nil {
			t.Fatal(err)
		}
	}
	if len(stages) != 2 || stages[0] != "one" || stages[1] != "two" {
		t.Fatalf("deferred chain must run one→two, got %v", stages)
	}
	job := host.Job(root.Header.Cause)
	if job != nil && !job.IsFinished() {
		t.Fatal("chained job must be finished")
	}
}

func TestSyncReplyCauseAndNewID(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID: actorH, Inbox: eba.NewInbox(8), Patterns: []string{"read"},
		MakeJob: func(root *eba.Envelope) *eba.Job { return newReadJob(host, root) },
	})
	if err := host.Start(bus); err != nil {
		t.Fatal(err)
	}
	dest := eba.NewInbox(8)
	_ = bus.Subscribe("read.result.**", dest)
	root := envGen("read", "z", gen)
	if err := host.Handle(bus, root, clk, gen); err != nil {
		t.Fatal(err)
	}
	got := dest.TryRecv()
	if got == nil {
		t.Fatal("reply must arrive")
	}
	if got.Header.Cause != root.Header.Cause || got.Header.ID == root.Header.ID {
		t.Fatalf("cause must inherit, id must renew: %+v", got.Header)
	}
	if rid, _ := got.Payload.(map[string]any)["request_id"]; rid != string(root.Header.ID) {
		t.Fatalf("request_id mismatch: %v", rid)
	}
}

func TestRequestAfterFinishRejected(t *testing.T) {
	defer func() {
		se, ok := recover().(*eba.StateError)
		if !ok || !containsStr(se.Error(), "not requestable") {
			t.Fatalf("expected not-requestable StateError, got %v", recover())
		}
	}()
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID: actorH, Inbox: eba.NewInbox(8), Patterns: []string{"job"},
		MakeJob: func(root *eba.Envelope) *eba.Job {
			j := eba.NewJob(host, root)
			j.OnBegin = func(j *eba.Job) error {
				if err := j.Finish(true, "", nil); err != nil {
					return err
				}
				_, err := j.Request(eba.RequestSpec{Stage: "a",
					RequestPrefix: "acl", ResultPrefix: "acl.result", Payload: 1})
				return err
			}
			return j
		},
	})
	if err := host.Start(bus); err != nil {
		t.Fatal(err)
	}
	if err := host.Handle(bus, envGen("job", map[string]any{}, gen), clk, gen); err != nil {
		t.Fatal(err)
	}
	t.Fatal("request after finish must panic")
}

func TestResultErrFinishesStep(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var states []*stepState
	parent := newStepHost(t, eba.NewInbox(8), 0, 0, &states)
	if err := parent.Start(bus); err != nil {
		t.Fatal(err)
	}
	root := envGen("job", map[string]any{}, gen)
	if err := parent.Handle(bus, root, clk, gen); err != nil {
		t.Fatal(err)
	}
	job := parent.Job(root.Header.Cause)
	childID := states[0].childID
	if job == nil || childID == "" {
		t.Fatal("child request must be issued")
	}
	resultTopic := mustTopic(eba.ResultTopicOf(childID, "acl.result", gen))
	echo := env(resultTopic,
		map[string]any{"request_id": string(childID), "result": eba.ErrPayload("deny", nil)},
		&eba.Options{Cause: root.Header.Cause, IDGen: gen})
	if err := parent.Handle(bus, echo, clk, gen); err != nil {
		t.Fatal(err)
	}
	if !job.IsFinished() || len(states[0].seen) != 1 || states[0].seen[0] != "acl" {
		t.Fatalf("err result must finish the step: seen=%v", states[0].seen)
	}
}
