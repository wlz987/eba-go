package job

import (
	"github.com/wlz987/eba-go/src/envelope"
	"github.com/wlz987/eba-go/src/registry"
	"github.com/wlz987/eba-go/src/reply"
	"github.com/wlz987/eba-go/src/result"
)

type Job struct {
	Host HostAPI
	Root *envelope.Envelope

	OnBegin       func(j *Job) error
	OnStageResult func(j *Job, stage string, body result.Body) error
	OnFinished    func(j *Job, ok bool, errMsg string, answer any) error

	finished    bool
	inflight    map[envelope.EnvelopeId]*Inflight
	deferred    []Deferred
	maxInflight int
}

func NewJob(host HostAPI, root *envelope.Envelope) *Job {
	return &Job{
		Host:        host,
		Root:        root,
		inflight:    map[envelope.EnvelopeId]*Inflight{},
		maxInflight: DefaultMaxInflight,
	}
}

func (j *Job) SetMaxInflight(n int) {
	if n < 1 {
		panic("job: max_inflight must be >= 1")
	}
	j.maxInflight = n
}

func (j *Job) IsFinished() bool { return j.finished }

func (j *Job) Begin() error {
	if j.OnBegin == nil {
		return nil
	}
	return j.OnBegin(j)
}

type RequestSpec struct {
	Stage         string
	RequestPrefix string
	ResultPrefix  string
	Payload       any
}

func (j *Job) Request(spec RequestSpec) (envelope.EnvelopeId, error) {
	if j.Host.ContinuationDepth() > 0 {
		return j.deferRequest(spec)
	}
	return j.issue(spec, "")
}

func (j *Job) Reply(resultPrefix string, body result.Body) error {
	return reply.Reply(j.Host.LoanBus(), j.Root, body, resultPrefix,
		j.Host.ActorID(), j.Host.LoanIDGen())
}

func (j *Job) Finish(ok bool, errMsg string, answer any) error {
	if j.finished {
		return nil
	}
	j.finished = true
	j.deferred = nil
	j.dropInflight()
	j.Host.RemoveJob(j)
	if j.OnFinished != nil {
		return j.OnFinished(j, ok, errMsg, answer)
	}
	return nil
}

func (j *Job) OnChildResult(e *envelope.Envelope) error {
	childID, ok := result.ResultRequestID(e)
	if !ok {
		return nil
	}
	body := result.ResultBody(e)
	if body == nil {
		return nil
	}
	entry := j.inflight[childID]
	delete(j.inflight, childID)
	j.Host.Registry().FinishSafe(j.Host.LoanBus(), j.Host.HostInbox(), childID)
	if entry == nil || j.finished {
		return nil
	}
	return j.emit(entry.Stage, body)
}

func (j *Job) ExpireDue(nowMs int64) error {
	var due []envelope.EnvelopeId
	for id, ent := range j.inflight {
		if ent.DeadlineMs != 0 && nowMs >= ent.DeadlineMs {
			due = append(due, id)
		}
	}
	reg := j.Host.Registry()
	for _, id := range due {
		ent := j.inflight[id]
		delete(j.inflight, id)
		if st, ok := reg.State(id); ok && st == registry.StatePending {
			reg.Timeout(id)
		}
		reg.FinishSafe(j.Host.LoanBus(), j.Host.HostInbox(), id)
		if !j.finished {
			if err := j.emit(ent.Stage, TimeoutBody); err != nil {
				return err
			}
		}
	}
	return nil
}

func (j *Job) ensureRequestable() {
	if j.finished {
		panic(&StateError{Msg: "job not requestable"})
	}
}

func (j *Job) deferRequest(spec RequestSpec) (envelope.EnvelopeId, error) {
	j.ensureRequestable()
	if len(j.inflight)+len(j.deferred) >= j.maxInflight {
		return "", &BackpressureError{Reason: "max_inflight"}
	}
	eid := j.Host.LoanIDGen().NextEnvelopeID()
	j.deferred = append(j.deferred, Deferred{
		Stage: spec.Stage, RequestPrefix: spec.RequestPrefix,
		ResultPrefix: spec.ResultPrefix, Payload: spec.Payload, RequestID: eid,
	})
	return eid, nil
}

func (j *Job) issue(spec RequestSpec, requestID envelope.EnvelopeId) (envelope.EnvelopeId, error) {
	j.ensureRequestable()
	if len(j.inflight) >= j.maxInflight {
		return "", &BackpressureError{Reason: "max_inflight"}
	}
	child, err := j.Host.Registry().StartRequest(
		j.Host.LoanBus(), j.Host.HostInbox(), j.Host.LoanIDGen(), registry.StartParams{
			RequestPrefix: spec.RequestPrefix,
			ResultPrefix:  spec.ResultPrefix,
			Payload:       spec.Payload,
			From:          j.Host.ActorID(),
			Cause:         j.Root.Header.Cause,
			RequestID:     requestID,
		})
	if err != nil {
		return "", err
	}
	var deadline int64
	if t := j.Host.TimeoutMs(); t != 0 {
		deadline = j.Host.LoanClock().NowMs() + t
	}
	j.inflight[child.Header.ID] = &Inflight{
		Stage: spec.Stage, ResultPrefix: spec.ResultPrefix, DeadlineMs: deadline,
	}
	return child.Header.ID, nil
}

func (j *Job) flushDeferred() error {
	for len(j.deferred) > 0 && !j.finished {
		nxt := j.deferred[0]
		j.deferred = j.deferred[1:]
		spec := RequestSpec{Stage: nxt.Stage, RequestPrefix: nxt.RequestPrefix,
			ResultPrefix: nxt.ResultPrefix, Payload: nxt.Payload}
		if _, err := j.issue(spec, nxt.RequestID); err != nil {
			return err
		}
	}
	return nil
}

func (j *Job) emit(stage string, body result.Body) error {
	j.Host.EnterContinuation()
	defer j.Host.LeaveContinuation()
	if j.OnStageResult != nil {
		if err := j.OnStageResult(j, stage, body); err != nil {
			return err
		}
	}
	return j.flushDeferred()
}

func (j *Job) dropInflight() {
	b := j.Host.LoanBus()
	reg := j.Host.Registry()
	for id := range j.inflight {
		delete(j.inflight, id)
		if st, ok := reg.State(id); ok && st == registry.StatePending {
			reg.Timeout(id)
		}
		reg.FinishSafe(b, j.Host.HostInbox(), id)
	}
}
