package bus

type BusError struct{ Msg string }

func (e *BusError) Error() string { return "bus: " + e.Msg }

type MailboxFull struct{ BusError }
