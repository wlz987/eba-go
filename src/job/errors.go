package job

type BackpressureError struct{ Reason string }

func (e *BackpressureError) Error() string { return "job: " + e.Reason }

type StateError struct{ Msg string }

func (e *StateError) Error() string { return "job: " + e.Msg }
