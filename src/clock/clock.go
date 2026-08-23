package clock

import "time"

type Clock interface{ NowMs() int64 }

type MonotonicClock struct{ start time.Time }

func NewMonotonic() *MonotonicClock { return &MonotonicClock{start: time.Now()} }

func (m *MonotonicClock) NowMs() int64 { return time.Since(m.start).Milliseconds() }

type ManualClock struct{ now int64 }

func NewManual(now int64) *ManualClock { return &ManualClock{now: now} }

func (m *ManualClock) NowMs() int64 { return m.now }

func (m *ManualClock) Advance(ms int64) { m.now += ms }
