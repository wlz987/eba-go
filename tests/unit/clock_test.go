package unit

import (
	"testing"

	eba "github.com/wlz987/eba-go/src"
)

func TestManualClockAdvance(t *testing.T) {
	c := eba.NewManualClock(10)
	if c.NowMs() != 10 {
		t.Fatal("initial now mismatch")
	}
	c.Advance(5)
	if c.NowMs() != 15 {
		t.Fatal("advance mismatch")
	}
}

func TestMonotonicClockMoves(t *testing.T) {
	if eba.NewMonotonicClock().NowMs() < 0 {
		t.Fatal("monotonic clock must be non-negative")
	}
}

func TestManualDefaultZero(t *testing.T) {
	if eba.NewManualClock(0).NowMs() != 0 {
		t.Fatal("manual clock must start where told")
	}
}
