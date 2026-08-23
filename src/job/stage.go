package job

import (
	"github.com/wlz987/eba-go/src/envelope"
	"github.com/wlz987/eba-go/src/result"
)

const (
	DefaultMaxInflight = 100
)

var TimeoutBody = result.ErrPayload("request_timeout", nil)

type Inflight struct {
	Stage        string
	ResultPrefix string
	DeadlineMs   int64
}

type Deferred struct {
	Stage         string
	RequestPrefix string
	ResultPrefix  string
	Payload       any
	RequestID     envelope.EnvelopeId
}
