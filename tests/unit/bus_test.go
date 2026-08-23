package unit

import (
	"fmt"
	"testing"

	eba "github.com/wlz987/eba-go/src"
)

func envN(topic string, n uint64) *eba.Envelope {
	e, err := eba.MakeEnvelope(topic, n, eba.ActorId("a"),
		&eba.Options{ID: eba.EnvelopeId(fmt.Sprintf("%032x", n))})
	if err != nil {
		panic(err)
	}
	return e
}

func TestUnmatchedPublishSilent(t *testing.T) {
	if err := eba.NewBus().Publish(envN("echo.gone", 1)); err != nil {
		t.Fatal(err)
	}
}

func TestFanoutAndFullRollback(t *testing.T) {
	bus := eba.NewBus()
	a := eba.NewInbox(1)
	b := eba.NewInbox(1)
	_ = bus.Subscribe("echo.**", a)
	_ = bus.Subscribe("echo.**", b)
	if err := bus.Publish(envN("echo.aa", 1)); err != nil {
		t.Fatal(err)
	}
	err := bus.Publish(envN("echo.bb", 2))
	if _, ok := err.(*eba.MailboxFull); !ok {
		t.Fatalf("expected MailboxFull, got %v", err)
	}
	if a.TryRecv() == nil || b.TryRecv() == nil {
		t.Fatal("first letters must survive rollback")
	}
	if a.TryRecv() != nil {
		t.Fatal("second letter must be rolled back")
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	bus := eba.NewBus()
	box := eba.NewInbox(4)
	_ = bus.Subscribe("echo.**", box)
	ok, err := bus.Unsubscribe("echo.**", box)
	if err != nil || !ok {
		t.Fatalf("unsubscribe: %v %v", ok, err)
	}
	_ = bus.Publish(envN("echo.aa", 1))
	if box.TryRecv() != nil {
		t.Fatal("delivery must stop after unsubscribe")
	}
}

func TestTryRecvEmpty(t *testing.T) {
	if eba.NewInbox(1).TryRecv() != nil {
		t.Fatal("empty inbox must yield nil")
	}
}

func TestClosedInboxSkipped(t *testing.T) {
	bus := eba.NewBus()
	box := eba.NewInbox(2)
	_ = bus.Subscribe("echo.**", box)
	box.Close()
	_ = bus.Publish(envN("echo.aa", 1))
	if box.TryRecv() != nil {
		t.Fatal("closed inbox must be skipped")
	}
}

func TestUnsubscribeUnknownIsFalse(t *testing.T) {
	ok, err := eba.NewBus().Unsubscribe("echo.**", eba.NewInbox(1))
	if err != nil || ok {
		t.Fatalf("unknown unsubscribe must be false: %v %v", ok, err)
	}
}

func TestDoubleSubscribeOneDelivery(t *testing.T) {
	bus := eba.NewBus()
	box := eba.NewInbox(4)
	_ = bus.Subscribe("echo.**", box)
	_ = bus.Subscribe("echo.**", box)
	_ = bus.Publish(envN("echo.aa", 1))
	if box.TryRecv() == nil || box.TryRecv() != nil {
		t.Fatal("double subscribe must deliver one copy")
	}
}

func TestSameInboxTwoPatternsOneCopy(t *testing.T) {
	bus := eba.NewBus()
	box := eba.NewInbox(4)
	_ = bus.Subscribe("echo.**", box)
	_ = bus.Subscribe("**", box)
	_ = bus.Publish(envN("echo.aa", 1))
	if box.TryRecv() == nil || box.TryRecv() != nil {
		t.Fatal("overlapping patterns must deliver one copy")
	}
}

func TestOneFullRollsBackAll(t *testing.T) {
	bus := eba.NewBus()
	room := eba.NewInbox(2)
	tight := eba.NewInbox(1)
	_ = bus.Subscribe("echo.**", room)
	_ = bus.Subscribe("echo.**", tight)
	_ = bus.Publish(envN("echo.aa", 1))
	if _, ok := bus.Publish(envN("echo.bb", 2)).(*eba.MailboxFull); !ok {
		t.Fatal("one full inbox must roll back the whole publish")
	}
	if room.TryRecv() == nil || room.TryRecv() != nil {
		t.Fatal("roomy inbox must keep only the first letter")
	}
	if tight.TryRecv() == nil {
		t.Fatal("tight inbox must keep the first letter")
	}
}
