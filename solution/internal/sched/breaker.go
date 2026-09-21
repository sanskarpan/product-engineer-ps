package sched

import (
	"sync"
	"time"
)

// Breaker states.
const (
	BreakerClosed   = "closed"    // traffic flows; failures counted
	BreakerOpen     = "open"      // claiming paused; destination gets a break
	BreakerHalfOpen = "half-open" // one trial delivery admitted
)

// BreakerConfig tunes the circuit breaker. Threshold <= 0 disables it.
type BreakerConfig struct {
	// Threshold is consecutive failing sends after which the circuit opens.
	// Successes and permanent rejections (proof the destination is alive)
	// reset the count. Keep it above MaxAttempts so one poison reminder
	// cannot trip it alone.
	Threshold int
	// Cooldown is how long claiming pauses before a half-open trial.
	Cooldown time.Duration
}

func DefaultBreakerConfig() BreakerConfig {
	return BreakerConfig{Threshold: 10, Cooldown: 5 * time.Second}
}

func BreakerDisabled() BreakerConfig { return BreakerConfig{Threshold: 0} }

// Breaker stops N workers from hammering a down destination in a tight
// claim→fail→backoff loop. While open, workers pause claiming instead of
// burning attempts and log lines against an endpoint that cannot succeed.
type Breaker struct {
	mu            sync.Mutex
	cfg           BreakerConfig
	consec        int
	state         string
	openedAt      time.Time
	trialInFlight bool
	opens         int64
}

func NewBreaker(cfg BreakerConfig) *Breaker {
	if cfg.Threshold < 0 {
		cfg.Threshold = 0
	}
	return &Breaker{cfg: cfg, state: BreakerClosed}
}

func (b *Breaker) disabled() bool { return b.cfg.Threshold <= 0 }

// Allow reports whether a delivery may proceed. After the cooldown it
// admits exactly one half-open trial.
func (b *Breaker) Allow() bool {
	if b.disabled() {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case BreakerClosed:
		return true
	case BreakerHalfOpen:
		return false // trial already in flight
	default: // open
		if time.Since(b.openedAt) >= b.cfg.Cooldown {
			b.state = BreakerHalfOpen
			b.trialInFlight = true
			return true
		}
		return false
	}
}

// Remaining sleeps off the cooldown instead of busy-spinning while open.
func (b *Breaker) Remaining() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != BreakerOpen {
		return 0
	}
	left := b.cfg.Cooldown - time.Since(b.openedAt)
	if left < 0 {
		return 0
	}
	return left
}

// RecordSuccess resets the count; a successful half-open trial closes.
func (b *Breaker) RecordSuccess() {
	if b.disabled() {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consec = 0
	if b.state == BreakerHalfOpen {
		b.state = BreakerClosed
		b.trialInFlight = false
	}
}

// RecordFailure counts one failed send; a failed trial re-opens.
func (b *Breaker) RecordFailure() {
	if b.disabled() {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BreakerHalfOpen {
		b.state = BreakerOpen
		b.openedAt = time.Now()
		b.trialInFlight = false
		b.opens++
		return
	}
	b.consec++
	if b.state == BreakerClosed && b.consec >= b.cfg.Threshold {
		b.state = BreakerOpen
		b.openedAt = time.Now()
		b.opens++
	}
}

// ReleaseTrial abandons an admitted trial that found no work, so the
// circuit cannot wedge in half-open when admission races an empty queue.
func (b *Breaker) ReleaseTrial() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BreakerHalfOpen && b.trialInFlight {
		b.state = BreakerOpen
		b.openedAt = time.Now()
		b.trialInFlight = false
	}
}

func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) Opens() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.opens
}
