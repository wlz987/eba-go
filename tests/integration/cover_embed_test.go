package integration

import (
	"testing"

	eba "github.com/wlz987/eba-go/src"
)

func TestSubscriberStartAndRecv(t *testing.T) {
	bus := eba.NewBus()
	box := eba.NewInbox(2)
	sub := eba.NewSubscriber(eba.ActorId("s"), box, []string{"echo.**"})
	if err := sub.Start(bus); err != nil {
		t.Fatal(err)
	}
	bus.Publish(env("echo.x", 1, &eba.Options{ID: aaaa32}))
	got := sub.TryRecv()
	if got == nil || got.Payload != 1 {
		t.Fatal("subscriber must receive the letter")
	}
}

func TestPublisherPublishOnly(t *testing.T) {
	bus := eba.NewBus()
	dest := eba.NewInbox(2)
	if err := bus.Subscribe("echo.**", dest); err != nil {
		t.Fatal(err)
	}
	pub := eba.NewPublisher(eba.ActorId("p"))
	if err := pub.Publish(bus, env("echo.x", "hi", &eba.Options{IDGen: eba.NewSeqIDGen(1)})); err != nil {
		t.Fatal(err)
	}
	got := dest.TryRecv()
	if got == nil || got.Payload != "hi" {
		t.Fatal("publisher letter not delivered")
	}
}
