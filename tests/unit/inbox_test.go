package unit

import (
	"testing"

	eba "github.com/wlz987/eba-go/src"
)

func inboxEnv(n uint64) *eba.Envelope {
	return envN("echo.x", n)
}

func TestCapacityRejectsZero(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("capacity < 1 must panic")
		}
	}()
	eba.NewInbox(0)
}

func TestCloseClearsAndStaysEmpty(t *testing.T) {
	box := eba.NewInbox(2)
	if !box.TryEnqueue(inboxEnv(1)) {
		t.Fatal("enqueue must succeed")
	}
	box.Close()
	if !box.IsClosed() || !box.IsEmpty() || box.TryRecv() != nil {
		t.Fatal("close must clear the box")
	}
	if box.TryEnqueue(inboxEnv(2)) {
		t.Fatal("closed box must refuse enqueue")
	}
	box.Close()
	if !box.IsClosed() {
		t.Fatal("close must be idempotent")
	}
}

func TestEnqueueFullThenRecv(t *testing.T) {
	box := eba.NewInbox(1)
	if !box.TryEnqueue(inboxEnv(1)) {
		t.Fatal("first enqueue must succeed")
	}
	if box.TryEnqueue(inboxEnv(2)) {
		t.Fatal("full box must refuse")
	}
	if box.TryRecv() == nil || box.TryRecv() != nil {
		t.Fatal("drain must yield exactly one letter")
	}
}

func TestDropLastIdentity(t *testing.T) {
	box := eba.NewInbox(2)
	a, b := inboxEnv(1), inboxEnv(2)
	_ = box.TryEnqueue(a)
	if box.TryDropLast(b) {
		t.Fatal("drop must match identity")
	}
	if !box.TryDropLast(a) || !box.IsEmpty() || box.TryDropLast(a) {
		t.Fatal("drop-last semantics broken")
	}
}

func TestNoteReaderSoftSecondActor(t *testing.T) {
	box := eba.NewInbox(2)
	_ = eba.NewSubscriber(eba.ActorId("one"), box, nil)
	_ = eba.NewSubscriber(eba.ActorId("two"), box, nil)
	if !box.TryEnqueue(inboxEnv(1)) {
		t.Fatal("soft second reader must not disturb delivery")
	}
}
