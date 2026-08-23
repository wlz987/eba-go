package pattern

type InvalidTopic struct{ Msg string }

func (e *InvalidTopic) Error() string { return "pattern: " + e.Msg }
