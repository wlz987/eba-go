package result

import (
	"github.com/wlz987/eba-go/src/envelope"
)

type Body map[string]any

func OkPayload(value any) Body {
	return Body{"ok": true, "value": value}
}

func ErrPayload(errMsg string, detail map[string]any) Body {
	body := Body{"ok": false, "error": errMsg}
	for k, v := range detail {
		switch k {
		case "ok", "error", "value":
			panic("result: reserved result key: " + k)
		}
		body[k] = v
	}
	return body
}

func IsResultOK(body any) bool {
	m, ok := asBody(body)
	return ok && m["ok"] == true && hasKey(m, "value")
}

func IsResultErr(body any) bool {
	m, ok := asBody(body)
	if !ok || m["ok"] != false {
		return false
	}
	_, isStr := m["error"].(string)
	return isStr
}

func hasResultShape(body any) bool { return IsResultOK(body) || IsResultErr(body) }

func ResultValue(body any) (any, bool) {
	if !IsResultOK(body) {
		return nil, false
	}
	m, _ := asBody(body)
	return m["value"], true
}

func ResultError(body any) (string, bool) {
	if !IsResultErr(body) {
		return "", false
	}
	m, _ := asBody(body)
	msg, _ := m["error"].(string)
	return msg, true
}

func ResultBody(e *envelope.Envelope) Body {
	m, ok := asBody(e.Payload)
	if !ok {
		return nil
	}
	raw, present := m["result"]
	if !present || !hasResultShape(raw) {
		return nil
	}
	out, _ := asBody(raw)
	return out
}

func ResultRequestID(e *envelope.Envelope) (envelope.EnvelopeId, bool) {
	m, ok := e.Payload.(map[string]any)
	if !ok {
		return "", false
	}
	raw, present := m["request_id"]
	if !present || raw == nil {
		return "", false
	}
	s, isStr := raw.(string)
	if !isStr {
		return "", false
	}
	return envelope.EnvelopeId(s), true
}

func asBody(v any) (map[string]any, bool) {
	switch m := v.(type) {
	case map[string]any:
		return m, true
	case Body:
		return m, true
	}
	return nil, false
}

func hasKey(m map[string]any, key string) bool {
	_, ok := m[key]
	return ok
}
