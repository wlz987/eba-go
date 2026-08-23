package jobhost

import (
	"github.com/wlz987/eba-go/src/bus"
	"github.com/wlz987/eba-go/src/clock"
	"github.com/wlz987/eba-go/src/envelope"
	"github.com/wlz987/eba-go/src/inbox"
	"github.com/wlz987/eba-go/src/job"
	"github.com/wlz987/eba-go/src/pattern"
	"github.com/wlz987/eba-go/src/registry"
	"github.com/wlz987/eba-go/src/subscriber"
)

const DefaultQueueLimit = 512

type loan struct {
	bus *bus.Bus
	clk clock.Clock
	gen envelope.IdGen
}

type Params struct {
	ActorID          envelope.ActorId
	Inbox            *inbox.Inbox
	Patterns         []string
	Accept           []string
	RequestTimeoutMs int64
	QueueLimit       int
	MakeJob          func(root *envelope.Envelope) *job.Job
}

type JobHost struct {
	actorID      envelope.ActorId
	inbox        *inbox.Inbox
	timeoutMs    int64
	registry     *registry.Registry
	sub          *subscriber.Subscriber
	accept       []pattern.Pattern
	slots        *SlotBook
	queue        *envelopeQueue
	contDepth    int
	current      *loan
	busy         bool
	shuttingDown bool
	makeJob      func(*envelope.Envelope) *job.Job
}

func NewJobHost(p Params) *JobHost {
	limit := p.QueueLimit
	if limit == 0 {
		limit = DefaultQueueLimit
	}
	src := p.Patterns
	if p.Accept != nil {
		src = p.Accept
	}
	accept := make([]pattern.Pattern, 0, len(src))
	for _, text := range src {
		pat, err := pattern.ParsePattern(text)
		if err != nil {
			panic(err)
		}
		accept = append(accept, pat)
	}
	return &JobHost{
		actorID:   p.ActorID,
		inbox:     p.Inbox,
		timeoutMs: p.RequestTimeoutMs,
		registry:  registry.NewRegistry(),
		sub:       subscriber.New(p.ActorID, p.Inbox, p.Patterns),
		accept:    accept,
		slots:     NewSlotBook(),
		queue:     newEnvelopeQueue(limit),
		makeJob:   p.MakeJob,
	}
}

func (h *JobHost) Inbox() *inbox.Inbox { return h.inbox }

func (h *JobHost) Start(b *bus.Bus) error { return h.sub.Start(b) }

func (h *JobHost) Job(cause envelope.EnvelopeId) *job.Job { return h.slots.Job(cause) }

func (h *JobHost) Shutdown() { h.shuttingDown = true }

func (h *JobHost) Handle(b *bus.Bus, e *envelope.Envelope, clk clock.Clock, gen envelope.IdGen) error {
	if h.busy {
		panic(&job.StateError{Msg: "reentrant handle"})
	}
	h.busy = true
	defer func() { h.busy = false }()
	h.borrow(b, clk, gen)
	defer h.loanEnd()
	empty := h.inbox.IsEmpty()
	if err := h.step(e); err != nil {
		return err
	}
	if empty {
		return h.drainInbox()
	}
	return nil
}

func (h *JobHost) step(e *envelope.Envelope) error {
	if err := dispatch(h, e); err != nil {
		return err
	}
	if err := watchdog(h); err != nil {
		return err
	}
	return flushQueue(h)
}

func (h *JobHost) drainInbox() error {
	for {
		e := h.inbox.TryRecv()
		if e == nil {
			return nil
		}
		if err := h.step(e); err != nil {
			return err
		}
	}
}

func (h *JobHost) Poll(b *bus.Bus, clk clock.Clock, gen envelope.IdGen) (bool, error) {
	e := h.inbox.TryRecv()
	if e == nil {
		if h.busy {
			panic(&job.StateError{Msg: "reentrant handle"})
		}
		h.busy = true
		defer func() { h.busy = false }()
		h.borrow(b, clk, gen)
		defer h.loanEnd()
		return false, watchdog(h)
	}
	return true, h.Handle(b, e, clk, gen)
}

func (h *JobHost) ActorID() envelope.ActorId    { return h.actorID }
func (h *JobHost) HostInbox() *inbox.Inbox      { return h.inbox }
func (h *JobHost) TimeoutMs() int64             { return h.timeoutMs }
func (h *JobHost) Registry() *registry.Registry { return h.registry }
func (h *JobHost) ContinuationDepth() int       { return h.contDepth }

func (h *JobHost) EnterContinuation() { h.contDepth++ }

func (h *JobHost) LeaveContinuation() {
	if h.contDepth > 0 {
		h.contDepth--
	}
}

func (h *JobHost) RemoveJob(j *job.Job) { h.slots.Remove(j) }

func (h *JobHost) LoanBus() *bus.Bus         { return h.requireLoan().bus }
func (h *JobHost) LoanClock() clock.Clock    { return h.requireLoan().clk }
func (h *JobHost) LoanIDGen() envelope.IdGen { return h.requireLoan().gen }

func (h *JobHost) requireLoan() *loan {
	if h.current == nil {
		panic("jobhost: Bus/Clock/IdGen borrowed only during handle/poll")
	}
	return h.current
}

func (h *JobHost) borrow(b *bus.Bus, clk clock.Clock, gen envelope.IdGen) {
	h.current = &loan{bus: b, clk: clk, gen: gen}
	h.registry.BindIDGen(gen)
}

func (h *JobHost) loanEnd() { h.current = nil }

func (h *JobHost) adoptAndBegin(root *envelope.Envelope) (*job.Job, error) {
	j := h.makeJob(root)
	h.slots.Adopt(j)
	if err := j.Begin(); err != nil {
		h.slots.Remove(j)
		return nil, err
	}
	return j, nil
}
