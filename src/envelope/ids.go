package envelope

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

const (
	maxSeg    = 64
	segPrefix = "e"
)

type EnvelopeId string
type ActorId string

func NewEnvelopeID() EnvelopeId {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return EnvelopeId(hex.EncodeToString(b[:]))
}

func TopicSegment(id EnvelopeId) (string, error) {
	cleaned := strings.ReplaceAll(strings.ToLower(string(id)), "-", "")
	if cleaned == "" {
		return "", &EnvelopeBuildError{Msg: "invalid envelope id: " + string(id)}
	}
	for i := 0; i < len(cleaned); i++ {
		c := cleaned[i]
		if c >= '0' && c <= '9' || c >= 'a' && c <= 'f' {
			continue
		}
		return "", &EnvelopeBuildError{Msg: "invalid envelope id: " + string(id)}
	}
	segment := segPrefix + cleaned
	if len(segment) > maxSeg || !IsName(segment) {
		return "", &EnvelopeBuildError{Msg: "invalid topic_segment for id: " + string(id)}
	}
	return segment, nil
}

type IdGen interface {
	NextEnvelopeID() EnvelopeId
	TopicSegment(EnvelopeId) (string, error)
}
