package eba

import (
	"github.com/wlz987/eba-go/src/bus"
	"github.com/wlz987/eba-go/src/clock"
	"github.com/wlz987/eba-go/src/envelope"
	"github.com/wlz987/eba-go/src/idgen"
	"github.com/wlz987/eba-go/src/inbox"
	"github.com/wlz987/eba-go/src/job"
	"github.com/wlz987/eba-go/src/jobhost"
	"github.com/wlz987/eba-go/src/pattern"
	"github.com/wlz987/eba-go/src/publisher"
	"github.com/wlz987/eba-go/src/registry"
	"github.com/wlz987/eba-go/src/reply"
	"github.com/wlz987/eba-go/src/result"
	"github.com/wlz987/eba-go/src/subscriber"
)

type (
	ActorId            = envelope.ActorId
	EnvelopeId         = envelope.EnvelopeId
	Header             = envelope.Header
	Envelope           = envelope.Envelope
	Options            = envelope.Options
	IdGen              = envelope.IdGen
	EnvelopeBuildError = envelope.EnvelopeBuildError
	Inbox              = inbox.Inbox
	Pattern            = pattern.Pattern
	Wildcard           = pattern.Wildcard
	InvalidTopic       = pattern.InvalidTopic
	Bus                = bus.Bus
	BusError           = bus.BusError
	MailboxFull        = bus.MailboxFull
	Subscriber         = subscriber.Subscriber
	Publisher          = publisher.Publisher
	State              = registry.State
	ResolveOutcome     = registry.ResolveOutcome
	Registry           = registry.Registry
	StartParams        = registry.StartParams
	ResultBody         = result.Body
	UuidIdGen          = idgen.UuidIdGen
	SeqIdGen           = idgen.SeqIdGen
	Clock              = clock.Clock
	MonotonicClock     = clock.MonotonicClock
	ManualClock        = clock.ManualClock
	HostAPI            = job.HostAPI
	Job                = job.Job
	RequestSpec        = job.RequestSpec
	BackpressureError  = job.BackpressureError
	StateError         = job.StateError
	JobHost            = jobhost.JobHost
	JobHostParams      = jobhost.Params
	QueueFull          = jobhost.QueueFull
	Matchmaker         = reply.Matchmaker
)

const (
	WildcardNone     = pattern.WildcardNone
	WildcardStar     = pattern.WildcardStar
	WildcardGlobStar = pattern.WildcardGlobStar

	StatePending  = registry.StatePending
	StateResolved = registry.StateResolved
	StateTimedOut = registry.StateTimedOut
	StateFailed   = registry.StateFailed

	DefaultMaxInflight = job.DefaultMaxInflight
	DefaultQueueLimit  = jobhost.DefaultQueueLimit
)

var (
	MakeEnvelope            = envelope.MakeEnvelope
	NewEnvelopeID           = envelope.NewEnvelopeID
	TopicSegment            = envelope.TopicSegment
	SplitTopic              = envelope.SplitTopic
	TopicSuffix             = envelope.TopicSuffix
	IsName                  = envelope.IsName
	ParsePattern            = pattern.ParsePattern
	Matches                 = pattern.Matches
	NewBus                  = bus.NewBus
	NewInbox                = inbox.New
	NewSubscriber           = subscriber.New
	NewPublisher            = publisher.New
	NewRegistry             = registry.NewRegistry
	ResultTopicOf           = registry.ResultTopicOf
	OkPayload               = result.OkPayload
	ErrPayload              = result.ErrPayload
	IsResultOK              = result.IsResultOK
	IsResultErr             = result.IsResultErr
	ResultValue             = result.ResultValue
	ResultError             = result.ResultError
	ResultBodyOf            = result.ResultBody
	ResultRequestID         = result.ResultRequestID
	LooksLikeResultEnvelope = result.LooksLikeResultEnvelope
	Reply                   = reply.Reply
	NewMatchmaker           = reply.NewMatchmaker
	NewSeqIDGen             = idgen.NewSeqIDGen
	NewManualClock          = clock.NewManual
	NewMonotonicClock       = clock.NewMonotonic
	NewJob                  = job.NewJob
	TimeoutBody             = job.TimeoutBody
	NewJobHost              = jobhost.NewJobHost
)
