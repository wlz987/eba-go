package jobhost

import "github.com/wlz987/eba-go/src/envelope"

type QueueFull struct{ Msg string }

func (e *QueueFull) Error() string { return "jobhost: " + e.Msg }

type envelopeQueue struct {
	limit int
	items []*envelope.Envelope
}

func newEnvelopeQueue(limit int) *envelopeQueue {
	if limit < 1 {
		panic("jobhost: queue_limit must be >= 1")
	}
	return &envelopeQueue{limit: limit}
}

func (q *envelopeQueue) len() int { return len(q.items) }

func (q *envelopeQueue) atLimit() bool { return len(q.items) >= q.limit }

func (q *envelopeQueue) offer(e *envelope.Envelope) error {
	if q.atLimit() {
		return &QueueFull{Msg: "queue_full"}
	}
	q.items = append(q.items, e)
	return nil
}

func (q *envelopeQueue) popleft() *envelope.Envelope {
	e := q.items[0]
	q.items = q.items[1:]
	return e
}
