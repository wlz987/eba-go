package jobhost

import (
	"github.com/wlz987/eba-go/src/envelope"
	"github.com/wlz987/eba-go/src/pattern"
	"github.com/wlz987/eba-go/src/result"
)

func dispatch(h *JobHost, e *envelope.Envelope) error {
	if result.LooksLikeResultEnvelope(e, h.LoanIDGen()) {
		return routeResult(h, e)
	}
	if h.shuttingDown {
		return nil
	}
	if !matchesBusiness(h.accept, e) {
		return nil
	}
	return h.queue.offer(e)
}

func routeResult(h *JobHost, e *envelope.Envelope) error {
	outcome := h.registry.ResolveOnly(e)
	if !outcome.Fresh {
		if outcome.State != nil && outcome.RequestID != "" {
			h.registry.FinishSafe(h.LoanBus(), h.inbox, outcome.RequestID)
		}
		return nil
	}
	parent := h.slots.RouteParent(e.Header.Cause)
	if parent == nil {
		if outcome.RequestID != "" {
			h.registry.FinishSafe(h.LoanBus(), h.inbox, outcome.RequestID)
		}
		return nil
	}
	return parent.OnChildResult(e)
}

func matchesBusiness(accept []pattern.Pattern, e *envelope.Envelope) bool {
	parts, err := envelope.SplitTopic(e.Header.Topic)
	if err != nil {
		return false
	}
	for _, p := range accept {
		if pattern.Matches(p, parts) {
			return true
		}
	}
	return false
}

func watchdog(h *JobHost) error {
	now := h.LoanClock().NowMs()
	for _, j := range h.slots.Actives() {
		if err := j.ExpireDue(now); err != nil {
			return err
		}
	}
	return nil
}

func flushQueue(h *JobHost) error {
	for h.queue.len() > 0 {
		e := h.queue.popleft()
		if h.shuttingDown {
			continue
		}
		if _, err := h.adoptAndBegin(e); err != nil {
			return err
		}
	}
	return nil
}
