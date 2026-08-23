package integration

import (
	"testing"

	eba "github.com/wlz987/eba-go/src"
)

func TestLoanOnlyDuringHandle(t *testing.T) {
	defer func() {
		r, ok := recover().(string)
		if !ok || !containsStr(r, "borrowed") {
			t.Fatalf("expected loan error, got %v", r)
		}
	}()
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID:  eba.ActorId("h"),
		Inbox:    eba.NewInbox(4),
		Patterns: []string{"read"},
		MakeJob: func(root *eba.Envelope) *eba.Job {
			j := eba.NewJob(host, root)
			j.OnBegin = func(j *eba.Job) error { return j.Finish(true, "", nil) }
			return j
		},
	})
	host.LoanBus()
	t.Fatal("loan outside handle must panic")
}

func TestCoverPollEmptyStillWatchdog(t *testing.T) {
	bus := eba.NewBus()
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID:  eba.ActorId("h"),
		Inbox:    eba.NewInbox(4),
		Patterns: []string{"read"},
		MakeJob: func(root *eba.Envelope) *eba.Job {
			j := eba.NewJob(host, root)
			j.OnBegin = func(j *eba.Job) error { return j.Finish(true, "", nil) }
			return j
		},
	})
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	if ok, err := host.Poll(bus, clk, gen); ok || err != nil {
		t.Fatalf("empty poll: %v %v", ok, err)
	}
	e := envGen("read", 1, gen)
	if err := bus.Subscribe("read", host.Inbox()); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(e); err != nil {
		t.Fatal(err)
	}
	ok, err := host.Poll(bus, clk, gen)
	if !ok || err != nil {
		t.Fatalf("poll after publish: %v %v", ok, err)
	}
}

func TestShutdownSkipsBusinessRoutesResult(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var states []*stepState
	host := newStepHost(t, eba.NewInbox(8), 0, 0, &states)
	if err := host.Start(bus); err != nil {
		t.Fatal(err)
	}
	root := envGen("job", map[string]any{}, gen)
	if err := host.Handle(bus, root, clk, gen); err != nil {
		t.Fatal(err)
	}
	childID := states[0].childID
	host.Shutdown()
	extra := envGen("job", map[string]any{"later": 1}, gen)
	if err := host.Handle(bus, extra, clk, gen); err != nil {
		t.Fatal(err)
	}
	if host.Job(extra.Header.Cause) != nil {
		t.Fatal("shutdown must not adopt new jobs")
	}
	job := host.Job(root.Header.Cause)
	resultTopic := mustTopic(eba.ResultTopicOf(childID, "acl.result", gen))
	echo := env(resultTopic,
		map[string]any{"request_id": string(childID), "result": eba.OkPayload("allow")},
		&eba.Options{Cause: root.Header.Cause, IDGen: gen})
	if err := host.Handle(bus, echo, clk, gen); err != nil {
		t.Fatal(err)
	}
	if job == nil || !job.IsFinished() {
		t.Fatal("in-flight result must still resolve after shutdown")
	}
}

func TestOrphanResultFinishesRegistry(t *testing.T) {
	bus := eba.NewBus()
	clk := eba.NewManualClock(0)
	gen := eba.NewSeqIDGen(1)
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID:  eba.ActorId("h"),
		Inbox:    eba.NewInbox(8),
		Patterns: []string{"read"},
		MakeJob: func(root *eba.Envelope) *eba.Job {
			j := eba.NewJob(host, root)
			j.OnBegin = func(j *eba.Job) error { return j.Finish(true, "", nil) }
			return j
		},
	})
	if err := host.Start(bus); err != nil {
		t.Fatal(err)
	}
	cause := gen.NextEnvelopeID()
	req, err := host.Registry().StartRequest(bus, host.Inbox(), gen, eba.StartParams{
		RequestPrefix: "acl",
		ResultPrefix:  "acl.result",
		Payload:       map[string]any{},
		From:          eba.ActorId("h"),
		Cause:         cause,
	})
	if err != nil {
		t.Fatal(err)
	}
	rid := req.Header.ID
	if _, ok := host.Registry().State(rid); !ok {
		t.Fatal("request must be pending")
	}
	topic := mustTopic(eba.ResultTopicOf(rid, "acl.result", gen))
	echo := env(topic,
		map[string]any{"request_id": string(rid), "result": eba.OkPayload("allow")},
		&eba.Options{Cause: cause, IDGen: gen})
	if err := host.Handle(bus, echo, clk, gen); err != nil {
		t.Fatal(err)
	}
	if _, ok := host.Registry().State(rid); ok {
		t.Fatal("orphan result must finish the registry entry")
	}
}

func containsStr(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
