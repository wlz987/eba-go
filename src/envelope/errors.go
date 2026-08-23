package envelope

type EnvelopeBuildError struct{ Msg string }

func (e *EnvelopeBuildError) Error() string { return "envelope: " + e.Msg }
