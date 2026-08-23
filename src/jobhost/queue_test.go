package jobhost

import (
	"testing"

	"github.com/wlz987/eba-go/src/envelope"
)

const qAaaa32 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const qBbbb32 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func TestQueueOfferHitsLimit(t *testing.T) {
	q := newEnvelopeQueue(1)
	e, err := envelope.MakeEnvelope("job", 1, "a", &envelope.Options{ID: envelope.EnvelopeId(qAaaa32)})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.offer(e); err != nil {
		t.Fatal(err)
	}
	if _, ok := q.offer(e).(*QueueFull); !ok {
		t.Fatal("second offer must report QueueFull")
	}
}

func TestQueueDrainOrder(t *testing.T) {
	q := newEnvelopeQueue(2)
	a, _ := envelope.MakeEnvelope("job", 1, "a", &envelope.Options{ID: envelope.EnvelopeId(qAaaa32)})
	b, _ := envelope.MakeEnvelope("job", 2, "a", &envelope.Options{ID: envelope.EnvelopeId(qBbbb32)})
	_ = q.offer(a)
	_ = q.offer(b)
	if q.len() != 2 || !q.atLimit() {
		t.Fatal("queue must be at limit")
	}
	if q.popleft() != a || q.popleft() != b || q.len() != 0 {
		t.Fatal("queue must drain in order")
	}
}

func TestQueueLimitRejectsZero(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("limit < 1 must panic")
		}
	}()
	newEnvelopeQueue(0)
}
