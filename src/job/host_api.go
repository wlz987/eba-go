package job

import (
	"github.com/wlz987/eba-go/src/bus"
	"github.com/wlz987/eba-go/src/clock"
	"github.com/wlz987/eba-go/src/envelope"
	"github.com/wlz987/eba-go/src/inbox"
	"github.com/wlz987/eba-go/src/registry"
)

type HostAPI interface {
	ActorID() envelope.ActorId
	HostInbox() *inbox.Inbox
	TimeoutMs() int64
	LoanBus() *bus.Bus
	LoanClock() clock.Clock
	LoanIDGen() envelope.IdGen
	Registry() *registry.Registry
	ContinuationDepth() int
	EnterContinuation()
	LeaveContinuation()
	RemoveJob(j *Job)
}
