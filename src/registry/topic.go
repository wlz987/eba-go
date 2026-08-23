package registry

import (
	"github.com/wlz987/eba-go/src/envelope"
)

func ResultTopicOf(requestID envelope.EnvelopeId, resultPrefix string, gen envelope.IdGen) (string, error) {
	seg, err := gen.TopicSegment(requestID)
	if err != nil {
		return "", err
	}
	return resultPrefix + "." + seg, nil
}
