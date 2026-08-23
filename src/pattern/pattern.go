package pattern

import (
	"strings"

	"github.com/wlz987/eba-go/src/envelope"
)

type Wildcard int

const (
	WildcardNone Wildcard = iota
	WildcardStar
	WildcardGlobStar
)

type Pattern struct {
	Literal  []string
	Wildcard Wildcard
}

func ParsePattern(text string) (Pattern, error) {
	if text == "" {
		return Pattern{}, &InvalidTopic{Msg: "empty pattern"}
	}
	parts := strings.Split(text, ".")
	for _, part := range parts {
		if part == "" {
			return Pattern{}, &InvalidTopic{Msg: "illegal pattern: " + text}
		}
	}
	for index, part := range parts {
		if part == "*" || part == "**" {
			if index != len(parts)-1 {
				return Pattern{}, &InvalidTopic{Msg: "wildcard not terminal: " + text}
			}
			continue
		}
		if !envelope.IsName(part) {
			return Pattern{}, &InvalidTopic{Msg: "illegal pattern segment: " + part}
		}
	}
	wildcard := WildcardNone
	literal := parts
	switch parts[len(parts)-1] {
	case "*", "**":
		if parts[len(parts)-1] == "*" {
			wildcard = WildcardStar
		} else {
			wildcard = WildcardGlobStar
		}
		literal = parts[:len(parts)-1]
	}
	return Pattern{Literal: literal, Wildcard: wildcard}, nil
}

func Matches(p Pattern, topicParts []string) bool {
	n := len(p.Literal)
	switch p.Wildcard {
	case WildcardNone:
		return equalParts(topicParts, p.Literal)
	case WildcardStar:
		return len(topicParts) == n+1 && equalParts(topicParts[:n], p.Literal)
	default:
		return len(topicParts) >= n && equalParts(topicParts[:n], p.Literal)
	}
}

func (p Pattern) Key() string {
	parts := append([]string{}, p.Literal...)
	switch p.Wildcard {
	case WildcardStar:
		parts = append(parts, "*")
	case WildcardGlobStar:
		parts = append(parts, "**")
	}
	return strings.Join(parts, ".")
}

func equalParts(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
