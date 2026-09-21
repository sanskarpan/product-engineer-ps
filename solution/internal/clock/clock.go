package clock

import (
	"sync/atomic"
	"time"
)

// Clock is the injectable time source. Production uses SystemClock;
// demos, tests, and the benchmark use ManualClock so time-dependent
// behaviour is deterministic and never waits real minutes.
type Clock interface {
	Now() time.Time
	NowMs() int64
}

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }
func (SystemClock) NowMs() int64   { return time.Now().UnixMilli() }

// ManualClock is a thread-safe fake clock. Zero value = Unix epoch;
// always construct with NewManual.
type ManualClock struct {
	ms atomic.Int64
}

func NewManual(t time.Time) *ManualClock {
	c := &ManualClock{}
	c.ms.Store(t.UnixMilli())
	return c
}

func (c *ManualClock) Now() time.Time { return time.UnixMilli(c.ms.Load()).UTC() }
func (c *ManualClock) NowMs() int64   { return c.ms.Load() }

// Set jumps to an absolute instant.
func (c *ManualClock) Set(t time.Time) { c.ms.Store(t.UnixMilli()) }

// Advance moves forward by d (never backwards).
func (c *ManualClock) Advance(d time.Duration) time.Time {
	for {
		old := c.ms.Load()
		next := time.UnixMilli(old).Add(d).UnixMilli()
		if next < old {
			next = old
		}
		if c.ms.CompareAndSwap(old, next) {
			return time.UnixMilli(next).UTC()
		}
	}
}
