package inbox

import (
	"sync/atomic"

	"github.com/wlz987/eba-go/src/envelope"
)

var inboxSeq atomic.Uint64

type Inbox struct {
	id        uint64
	capacity  int
	items     []*envelope.Envelope
	head      int
	closed    bool
	reader    envelope.ActorId
	hasReader bool
}

func New(capacity int) *Inbox {
	if capacity < 1 {
		panic("inbox: capacity must be >= 1")
	}
	return &Inbox{id: inboxSeq.Add(1), capacity: capacity}
}

func (b *Inbox) ID() uint64     { return b.id }
func (b *Inbox) IsClosed() bool { return b.closed }

func (b *Inbox) NoteReader(actor envelope.ActorId) {
	if !b.hasReader {
		b.reader = actor
		b.hasReader = true
	}
}

func (b *Inbox) HasRoom() bool {
	return b.remaining() >= 1
}

func (b *Inbox) remaining() int {
	if b.closed {
		return 0
	}
	return b.capacity - b.Len()
}

func (b *Inbox) Len() int { return len(b.items) - b.head }

func (b *Inbox) IsEmpty() bool { return b.Len() == 0 }

func (b *Inbox) TryEnqueue(e *envelope.Envelope) bool {
	if b.closed || b.Len() >= b.capacity {
		return false
	}
	b.items = append(b.items, e)
	return true
}

func (b *Inbox) TryRecv() *envelope.Envelope {
	if b.IsEmpty() {
		return nil
	}
	e := b.items[b.head]
	b.items[b.head] = nil
	b.head++
	if b.head == len(b.items) {
		b.items = b.items[:0]
		b.head = 0
	}
	return e
}

func (b *Inbox) TryDropLast(e *envelope.Envelope) bool {
	if b.IsEmpty() || b.items[len(b.items)-1] != e {
		return false
	}
	b.items = b.items[:len(b.items)-1]
	return true
}

func (b *Inbox) Close() {
	if b.closed {
		return
	}
	b.closed = true
	b.items = nil
	b.head = 0
}
