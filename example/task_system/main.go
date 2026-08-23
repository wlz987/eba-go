package main

import (
	"fmt"

	eba "github.com/wlz987/eba-go/src"
)

func main() {
	bus := eba.NewBus()
	clk := eba.NewMonotonicClock()
	gen := eba.NewSeqIDGen(1)

	var reader *eba.JobHost
	reader = eba.NewJobHost(eba.JobHostParams{
		ActorID:  "reader",
		Inbox:    eba.NewInbox(8),
		Patterns: []string{"read"},
		MakeJob: func(root *eba.Envelope) *eba.Job {
			j := eba.NewJob(reader, root)
			j.OnBegin = func(*eba.Job) error {
				if err := j.Reply("read.result", eba.OkPayload(root.Payload)); err != nil {
					return err
				}
				return j.Finish(true, "", root.Payload)
			}
			return j
		},
	})
	if err := reader.Start(bus); err != nil {
		panic(err)
	}

	answers := eba.NewInbox(8)
	if err := bus.Subscribe("read.result.**", answers); err != nil {
		panic(err)
	}

	req, _ := eba.MakeEnvelope("read", "buf.a", "client", &eba.Options{IDGen: gen})
	if err := bus.Publish(req); err != nil {
		panic(err)
	}
	for {
		more, err := reader.Poll(bus, clk, gen)
		if err != nil {
			panic(err)
		}
		if !more {
			break
		}
	}

	got := answers.TryRecv()
	body := got.Payload.(map[string]any)["result"]
	v, _ := eba.ResultValue(body)
	fmt.Println("answer:", v)
}
