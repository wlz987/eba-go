package envelope

import "strings"

func IsName(segment string) bool {
	if segment == "" || segment[0] < 'a' || segment[0] > 'z' {
		return false
	}
	for i := 1; i < len(segment); i++ {
		c := segment[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' {
			continue
		}
		return false
	}
	return true
}

func SplitTopic(topic string) ([]string, error) {
	if topic == "" {
		return nil, &EnvelopeBuildError{Msg: "empty topic"}
	}
	parts := strings.Split(topic, ".")
	for _, part := range parts {
		if !IsName(part) {
			return nil, &EnvelopeBuildError{Msg: "illegal topic segment: " + part}
		}
	}
	return parts, nil
}

func TopicSuffix(topic string) (string, error) {
	if topic == "" {
		return "", &EnvelopeBuildError{Msg: "empty topic"}
	}
	if i := strings.LastIndex(topic, "."); i >= 0 {
		return topic[i+1:], nil
	}
	return topic, nil
}
