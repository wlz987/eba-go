package unit

import (
	"testing"

	eba "github.com/wlz987/eba-go/src"
)

func TestPublisherHasNoInbox(t *testing.T) {
	pub := eba.NewPublisher(eba.ActorId("p"))
	bus := eba.NewBus()
	dest := eba.NewInbox(2)
	if err := bus.Subscribe("echo.**", dest); err != nil {
		t.Fatal(err)
	}
	e, _ := eba.MakeEnvelope("echo.x", 1, eba.ActorId("p"),
		&eba.Options{ID: eba.EnvelopeId(aaaa32)})
	if err := pub.Publish(bus, e); err != nil {
		t.Fatal(err)
	}
	got := dest.TryRecv()
	if got == nil || got.Payload != 1 {
		t.Fatal("publisher letter not delivered")
	}
}

const aaaa32 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
