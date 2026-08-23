package unit

import (
	"testing"

	eba "github.com/wlz987/eba-go/src"
)

func parts(t *testing.T, topic string) []string {
	t.Helper()
	p, err := eba.SplitTopic(topic)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func mustPattern(t *testing.T, text string) eba.Pattern {
	t.Helper()
	p, err := eba.ParsePattern(text)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestExactStarGlob(t *testing.T) {
	if !eba.Matches(mustPattern(t, "echo"), parts(t, "echo")) {
		t.Fatal("echo should match echo")
	}
	if !eba.Matches(mustPattern(t, "echo.*"), parts(t, "echo.e01")) {
		t.Fatal("echo.* should match echo.e01")
	}
	if eba.Matches(mustPattern(t, "echo.*"), parts(t, "echo.a.b")) {
		t.Fatal("echo.* should not match echo.a.b")
	}
	if !eba.Matches(mustPattern(t, "echo.**"), parts(t, "echo.a.b")) {
		t.Fatal("echo.** should match echo.a.b")
	}
	if eba.Matches(mustPattern(t, "echo.**"), parts(t, "result.echo.e01")) {
		t.Fatal("echo.** should not match result.echo.e01")
	}
}

func TestWildcardMustBeTerminal(t *testing.T) {
	if _, err := eba.ParsePattern("echo.*.x"); err == nil {
		t.Fatal("expected InvalidTopic")
	} else if _, ok := err.(*eba.InvalidTopic); !ok {
		t.Fatalf("wrong error type %T", err)
	}
}

func TestEmptyIllegal(t *testing.T) {
	if _, err := eba.ParsePattern(""); err == nil {
		t.Fatal("expected InvalidTopic for empty pattern")
	}
}

func TestEmptySegmentAndIllegalName(t *testing.T) {
	for _, text := range []string{"echo..x", "Echo", "echo.**.x"} {
		if _, err := eba.ParsePattern(text); err == nil {
			t.Fatalf("expected InvalidTopic for %q", text)
		}
	}
}

func TestBareWildcards(t *testing.T) {
	if !eba.Matches(mustPattern(t, "*"), parts(t, "echo")) {
		t.Fatal("* must match single segment")
	}
	if eba.Matches(mustPattern(t, "*"), parts(t, "echo.x")) {
		t.Fatal("* must not match two segments")
	}
	if !eba.Matches(mustPattern(t, "**"), parts(t, "echo")) ||
		!eba.Matches(mustPattern(t, "**"), parts(t, "a.b.c")) {
		t.Fatal("** must match any depth")
	}
}

func TestExactMiss(t *testing.T) {
	if eba.Matches(mustPattern(t, "echo"), parts(t, "echo.x")) {
		t.Fatal("exact must miss deeper topic")
	}
	if eba.Matches(mustPattern(t, "echo.*"), parts(t, "echo")) {
		t.Fatal("star must miss shallower topic")
	}
}
