package integration

import (
	"testing"

	eba "github.com/wlz987/eba-go/src"
)

const (
	actorA = eba.ActorId("a")
	actorH = eba.ActorId("host")
	actorL = eba.ActorId("leaf")

	aaaa32 = eba.EnvelopeId("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	bbbb32 = eba.EnvelopeId("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	cccc32 = eba.EnvelopeId("cccccccccccccccccccccccccccccccc")
	dddd32 = eba.EnvelopeId("dddddddddddddddddddddddddddddddd")

	defaultQueueLimitForTest = 512
)

func env(topic string, payload any, opts *eba.Options) *eba.Envelope {
	e, err := eba.MakeEnvelope(topic, payload, actorA, opts)
	if err != nil {
		panic(err)
	}
	return e
}

func envGen(topic string, payload any, gen eba.IdGen) *eba.Envelope {
	return env(topic, payload, &eba.Options{IDGen: gen})
}

func newReadJob(host eba.HostAPI, root *eba.Envelope) *eba.Job {
	j := eba.NewJob(host, root)
	j.OnBegin = func(*eba.Job) error {
		if err := j.Reply("read.result", eba.OkPayload(root.Payload)); err != nil {
			return err
		}
		return j.Finish(true, "", root.Payload)
	}
	return j
}

func newEchoJob(host eba.HostAPI, root *eba.Envelope) *eba.Job {
	j := eba.NewJob(host, root)
	j.OnBegin = func(*eba.Job) error {
		if err := j.Reply("acl.result", eba.OkPayload("allow")); err != nil {
			return err
		}
		return j.Finish(true, "", "allow")
	}
	return j
}

type stepState struct {
	seen    []string
	childID eba.EnvelopeId
}

func newStepJob(host eba.HostAPI, root *eba.Envelope) (*eba.Job, *stepState) {
	j := eba.NewJob(host, root)
	st := &stepState{}
	j.OnBegin = func(*eba.Job) error {
		id, err := j.Request(eba.RequestSpec{
			Stage: "acl", RequestPrefix: "acl",
			ResultPrefix: "acl.result", Payload: map[string]any{"who": "x"},
		})
		if err != nil {
			return err
		}
		st.childID = id
		return nil
	}
	j.OnStageResult = func(j *eba.Job, stage string, body eba.ResultBody) error {
		st.seen = append(st.seen, stage)
		if eba.IsResultErr(body) {
			msg, _ := eba.ResultError(body)
			return j.Finish(false, msg, nil)
		}
		v, _ := eba.ResultValue(body)
		if err := j.Reply("job.result", eba.OkPayload(v)); err != nil {
			return err
		}
		return j.Finish(true, "", v)
	}
	return j, st
}

func newWaitJob(host eba.HostAPI, root *eba.Envelope, begins *int) *eba.Job {
	j := eba.NewJob(host, root)
	j.OnBegin = func(*eba.Job) error {
		*begins++
		_, err := j.Request(eba.RequestSpec{
			Stage: "ext", RequestPrefix: "ext.wait",
			ResultPrefix: "ext.wait.result", Payload: map[string]any{},
		})
		return err
	}
	j.OnStageResult = func(j *eba.Job, stage string, body eba.ResultBody) error {
		if eba.IsResultErr(body) {
			msg, _ := eba.ResultError(body)
			if err := j.Reply("wait.result", body); err != nil {
				return err
			}
			return j.Finish(false, msg, nil)
		}
		v, _ := eba.ResultValue(body)
		if err := j.Reply("wait.result", eba.OkPayload(v)); err != nil {
			return err
		}
		return j.Finish(true, "", v)
	}
	return j
}

func newTwoWaitJob(host eba.HostAPI, root *eba.Envelope) *eba.Job {
	j := eba.NewJob(host, root)
	left := 2
	j.OnBegin = func(*eba.Job) error {
		if _, err := j.Request(eba.RequestSpec{
			Stage: "a", RequestPrefix: "ext.a",
			ResultPrefix: "ext.a.result", Payload: 1,
		}); err != nil {
			return err
		}
		_, err := j.Request(eba.RequestSpec{
			Stage: "b", RequestPrefix: "ext.b",
			ResultPrefix: "ext.b.result", Payload: 2,
		})
		return err
	}
	j.OnStageResult = func(j *eba.Job, stage string, body eba.ResultBody) error {
		left--
		if left > 0 {
			return nil
		}
		return j.Finish(true, "", nil)
	}
	return j
}

func newStepHost(t *testing.T, box *eba.Inbox, timeoutMs int64, queueLimit int,
	states *[]*stepState) *eba.JobHost {
	t.Helper()
	var host *eba.JobHost
	host = eba.NewJobHost(eba.JobHostParams{
		ActorID:          actorH,
		Inbox:            box,
		Patterns:         []string{"job"},
		RequestTimeoutMs: timeoutMs,
		QueueLimit:       queueLimit,
		MakeJob: func(root *eba.Envelope) *eba.Job {
			j, st := newStepJob(host, root)
			*states = append(*states, st)
			return j
		},
	})
	return host
}

func mustTopic(topic string, err error) string {
	if err != nil {
		panic(err)
	}
	return topic
}
