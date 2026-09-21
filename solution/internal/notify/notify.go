package notify

import (
	"context"
	"fmt"
	"sync"
)

// Outcome of one notification attempt.
const (
	// Temporary failures (destination down, timeout) are retried bounded.
	Temporary = "temporary"
	// Permanent failures (bad content the destination rejects) never retry.
	Permanent = "permanent"
	// Uncertain means the notification may have landed but the
	// acknowledgement was lost. It retries like temporary, but the attempt
	// is recorded distinctly and exhaustion reports possible delivery
	// instead of a clean failure — the one case where at-least-once and
	// exactly-once genuinely conflict must stay visible, not lie.
	Uncertain = "uncertain"
)

// Notifier is the delivery-boundary seam: the scheduler programs against
// this, so a real push/email provider can replace the fake untouched.
type Notifier interface {
	Send(ctx context.Context, deliveryKey, content string) (ok bool, kind, errStr string)
	// LogicalCount reports how many distinct delivery keys were delivered.
	// Used by the benchmark to prove exactly-once per occurrence.
	LogicalCount() int
	DeliveriesFor(key string) int
	Reset()
}

// Fake modes for tests, demos, and the benchmark.
const (
	ModeOK           = "ok"          // always deliver
	ModeFailFirst    = "fail-first"  // temporary failures, then deliver
	ModeAlwaysTemp   = "always-temp" // temporary failure forever
	ModeAlwaysPerm   = "always-perm" // permanent rejection forever
	ModeLostAckFirst = "lost-ack"    // deliver logically, report temporary (lost acknowledgement)
)

type Fake struct {
	mu       sync.Mutex
	mode     string
	failLeft int
	// delivered tracks keys already delivered: redelivery of the same key
	// is acknowledged without a second logical notification (AC4).
	delivered map[string]bool
	calls     map[string]int
	logical   int
}

func NewFake(mode string, failFirst int) *Fake {
	return &Fake{mode: mode, failLeft: failFirst,
		delivered: map[string]bool{}, calls: map[string]int{}}
}

func (f *Fake) SetMode(mode string, failFirst int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode = mode
	if failFirst > 0 {
		f.failLeft = failFirst
	} else if mode == ModeFailFirst && f.failLeft == 0 {
		f.failLeft = 2
	}
}

func (f *Fake) Send(_ context.Context, key, content string) (bool, string, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[key]++
	if f.delivered[key] {
		return true, "", "" // duplicate execution: already counted, no second notification
	}
	switch f.mode {
	case ModeAlwaysTemp:
		return false, Temporary, "destination temporarily unavailable"
	case ModeAlwaysPerm:
		return false, Permanent, fmt.Sprintf("destination rejected content: %q", content)
	case ModeFailFirst:
		if f.failLeft > 0 {
			f.failLeft--
			return false, Temporary, "destination temporarily unavailable"
		}
	case ModeLostAckFirst:
		if f.failLeft > 0 {
			f.failLeft--
			// The critical uncertain-acknowledgement case: the notification
			// logically landed, but the caller hears failure and will retry.
			// The retry must not double-notify (same key), and the attempt
			// must be recorded as uncertain, not plain retryable.
			f.delivered[key] = true
			f.logical++
			return false, Uncertain, "acknowledgement lost in transit (possibly delivered)"
		}
	}
	if content == "" {
		return false, Permanent, "empty content rejected"
	}
	f.delivered[key] = true
	f.logical++
	return true, "", ""
}

func (f *Fake) LogicalCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logical
}

func (f *Fake) DeliveriesFor(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[key]
}

func (f *Fake) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delivered = map[string]bool{}
	f.calls = map[string]int{}
	f.logical = 0
	f.failLeft = 0
}
