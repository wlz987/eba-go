package result

import (
	"github.com/wlz987/eba-go/src/envelope"
)

func LooksLikeResultEnvelope(e *envelope.Envelope, gen envelope.IdGen) bool {
	requestID, ok := ResultRequestID(e)
	if !ok || ResultBody(e) == nil || gen == nil {
		return false
	}
	seg, err := gen.TopicSegment(requestID)
	if err != nil {
		return false
	}
	suffix, err := envelope.TopicSuffix(e.Header.Topic)
	if err != nil {
		return false
	}
	return suffix == seg
}
