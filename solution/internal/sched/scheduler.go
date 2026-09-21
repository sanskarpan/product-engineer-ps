package sched

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/product-engineer-ps/solution/internal/clock"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/notify"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/store"
)

// Policy bounds retries. Temporary failures retry with exponential backoff
// + jitter; permanent rejections go straight to failed.
type Policy struct {
	MaxAttempts int
	BaseDelayMs int64
	MaxDelayMs  int64
}

func DefaultPolicy() Policy { return Policy{MaxAttempts: 5, BaseDelayMs: 500, MaxDelayMs: 30000} }

func (p Policy) Validate() error {
	if p.MaxAttempts < 1 || p.MaxAttempts > 20 {
		return fmt.Errorf("max attempts must be 1..20, got %d", p.MaxAttempts)
	}
	if p.BaseDelayMs < 0 || p.MaxDelayMs <= 0 || p.BaseDelayMs > p.MaxDelayMs {
		return fmt.Errorf("bad backoff bounds base=%d max=%d", p.BaseDelayMs, p.MaxDelayMs)
	}
	return nil
}

func (p Policy) backoff(n int, rnd *rand.Rand) time.Duration {
	if n < 1 {
		n = 1
	}
	d := p.BaseDelayMs << (n - 1)
	if d > p.MaxDelayMs || d < 0 {
		d = p.MaxDelayMs
	}
	d += int64(float64(d) * 0.2 * rnd.Float64())
	if d > p.MaxDelayMs {
		d = p.MaxDelayMs
	}
	return time.Duration(d) * time.Millisecond
}

// Scheduler owns due-work discovery + delivery. Storage owns durability
// (store.Provider), the fake destination owns idempotency (notify.Notifier),
// the clock owns time (clock.Clock). It owns none of those.
type Scheduler struct {
	store       store.Provider
	notifier    notify.Notifier
	clock       clock.Clock
	policy      Policy
	breaker     *Breaker
	maxLateness time.Duration // 0 = disabled: overdue always fires ASAP
	log         *slog.Logger

	notifyCh chan struct{}
	stop     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	mu       sync.Mutex
	rndMu    sync.Mutex
	running  bool
	stopped  bool
	rnd      *rand.Rand

	delivered   atomic.Int64
	failed      atomic.Int64
	attempts    atomic.Int64
	stale       atomic.Int64 // in-flight results discarded after edit/cancel
	droppedLate atomic.Int64 // overdue beyond max-lateness, dropped not delivered
}

type Snapshot struct {
	Delivered    int64  `json:"delivered"`
	Failed       int64  `json:"failed"`
	Attempts     int64  `json:"attempts"`
	Stale        int64  `json:"staleDiscarded"`
	DroppedLate  int64  `json:"droppedOverdue"`
	BreakerState string `json:"breakerState"`
	BreakerOpens int64  `json:"breakerOpens"`
}

func New(s store.Provider, n notify.Notifier, c clock.Clock, p Policy, log *slog.Logger) *Scheduler {
	return NewFull(s, n, c, p, BreakerDisabled(), 0, log)
}

// NewFull wires the production options: breaker + max-lateness. Tests use
// New and opt in explicitly.
func NewFull(s store.Provider, n notify.Notifier, c clock.Clock, p Policy, bc BreakerConfig, maxLateness time.Duration, log *slog.Logger) *Scheduler {
	if log == nil {
		log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Scheduler{store: s, notifier: n, clock: c, policy: p,
		breaker: NewBreaker(bc), maxLateness: maxLateness, log: log,
		notifyCh: make(chan struct{}, 1), stop: make(chan struct{}),
		ctx: ctx, cancel: cancel, rnd: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

func (d *Scheduler) Snapshot() Snapshot {
	return Snapshot{Delivered: d.delivered.Load(), Failed: d.failed.Load(),
		Attempts: d.attempts.Load(), Stale: d.stale.Load(), DroppedLate: d.droppedLate.Load(),
		BreakerState: d.breaker.State(), BreakerOpens: d.breaker.Opens()}
}

func (d *Scheduler) Kick() {
	select {
	case d.notifyCh <- struct{}{}:
	default:
	}
}

func (d *Scheduler) Start(workers int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.running || d.stopped {
		return
	}
	d.running = true
	if workers < 1 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		d.wg.Add(1)
		go d.loop()
	}
}

func (d *Scheduler) Stop() {
	d.mu.Lock()
	if !d.running {
		d.mu.Unlock()
		return
	}
	d.running = false
	d.stopped = true
	d.cancel()
	close(d.stop)
	d.mu.Unlock()
	d.wg.Wait()
}

func (d *Scheduler) loop() {
	defer d.wg.Done()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		d.drain()
		select {
		case <-d.stop:
			return
		case <-d.notifyCh:
		case <-tick.C:
		}
	}
}

func (d *Scheduler) drain() {
	for {
		select {
		case <-d.stop:
			return
		default:
		}
		if !d.breaker.Allow() {
			// Circuit open: pause claiming instead of hammering a down
			// destination. The notify arm keeps recovery responsive.
			wait := d.breaker.Remaining()
			if wait <= 0 {
				wait = 50 * time.Millisecond
			}
			select {
			case <-d.stop:
				return
			case <-d.notifyCh:
			case <-time.After(wait):
			}
			return
		}
		r, ok, contended, err := d.store.ClaimDue(d.clock.NowMs())
		if err != nil {
			d.log.Error("claim due failed", "err", err)
			d.breaker.ReleaseTrial()
			time.Sleep(200 * time.Millisecond)
			return
		}
		if ok {
			d.deliver(r)
			continue
		}
		if contended {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		d.breaker.ReleaseTrial() // admitted trial, empty queue: don't wedge half-open
		return
	}
}

func (d *Scheduler) backoff(n int) time.Duration {
	d.rndMu.Lock()
	defer d.rndMu.Unlock()
	return d.policy.backoff(n, d.rnd)
}

// noteBreaker feeds the circuit breaker and logs transitions. A half-open
// trial is judged strictly: only a successful send proves destination
// health and closes the circuit. Outside trials, permanent rejections also
// prove liveness (the destination answered decisively) and reset the count.
func (d *Scheduler) noteBreaker(outcome string) {
	before := d.breaker.State()
	switch {
	case before == BreakerHalfOpen && outcome == store.OutcomeSuccess:
		d.breaker.RecordSuccess()
	case before == BreakerHalfOpen:
		d.breaker.RecordFailure()
	case outcome == store.OutcomeSuccess || outcome == store.OutcomePermanent:
		d.breaker.RecordSuccess()
	default:
		d.breaker.RecordFailure()
	}
	if after := d.breaker.State(); after != before {
		d.log.Info("circuit breaker transition", "from", before, "to", after)
	}
}

func (d *Scheduler) deliver(r store.Reminder) {
	attemptNo := r.Attempts + 1
	started := d.clock.NowMs()
	t0 := time.Now() // wall clock: latency stays meaningful under time travel

	// Max-lateness gate (opt-in production policy): an item overdue beyond
	// the limit is dropped, not delivered — a 3am pill reminder firing at
	// noon is wrong, and "late beats never" must have a bound. No attempt
	// is recorded (nothing was attempted); the error explains the drop.
	// Disabled (0) by default, preserving fire-always ASAP semantics.
	if d.maxLateness > 0 && started-r.FireAtMs > d.maxLateness.Milliseconds() {
		d.droppedLate.Add(1)
		d.failed.Add(1)
		msg := fmt.Sprintf("dropped: overdue %s exceeds max lateness %s",
			time.Duration(started-r.FireAtMs)*time.Millisecond, d.maxLateness)
		// No attempt row exists for a drop (nothing was attempted), so the
		// count stays put — history length and count never skew.
		applied, err := d.store.FinishDelivery(r.ID, r.Version, store.StatusFailed, r.Attempts, nil, msg)
		if err != nil {
			d.log.Error("state update failed", "id", r.ID, "err", err)
		} else if !applied {
			d.stale.Add(1)
		}
		d.log.Info("overdue dropped", "id", r.ID, "overdue_ms", started-r.FireAtMs)
		return
	}

	// Durable dedupe: if this key already notified (e.g. a lost-ack retry or
	// a redelivery after a crash), suppress the re-send and reconcile the
	// row as delivered. The destination's memory alone cannot survive restarts.
	if done, err := d.store.IsDelivered(r.DeliveryKey); err != nil {
		d.log.Error("dedupe check failed", "id", r.ID, "err", err)
	} else if done {
		d.attempts.Add(1)
		d.delivered.Add(1)
		applied, err := d.store.FinishAttempt(store.Attempt{
			ReminderID: r.ID, Version: r.Version, AttemptNo: attemptNo,
			StartedAt: started, FinishedAt: d.clock.NowMs(),
			Outcome: store.OutcomeSuccess, Error: "duplicate suppressed (already delivered)", LatencyMs: 0,
		}, store.StatusDelivered, attemptNo, nil, "")
		if err != nil {
			d.log.Error("state update failed", "id", r.ID, "err", err)
		} else if !applied {
			d.stale.Add(1)
		}
		d.log.Info("duplicate suppressed", "id", r.ID, "key", r.DeliveryKey)
		return
	}

	ok, kind, errStr := d.notifier.Send(d.ctx, r.DeliveryKey, r.Content)

	latency := time.Since(t0).Milliseconds()
	outcome := store.OutcomeSuccess
	if !ok {
		switch kind {
		case notify.Permanent:
			outcome = store.OutcomePermanent
		case notify.Uncertain:
			outcome = store.OutcomeUncertain
		default:
			outcome = store.OutcomeRetryable
		}
	}
	d.noteBreaker(outcome)
	d.attempts.Add(1)
	d.log.Info("attempt", "id", r.ID, "version", r.Version, "attempt", attemptNo,
		"outcome", outcome, "latency_ms", latency, "err", errStr)

	// finish applies only if the row is still running at this version;
	// false = edited or cancelled meanwhile → discard, never overwrite.
	// Attempt and state persist atomically: no history/count skew on crash.
	finishAttempt := func(a store.Attempt, status string, next *int64, msg string) {
		applied, err := d.store.FinishAttempt(a, status, attemptNo, next, msg)
		if err != nil {
			d.log.Error("state update failed", "id", r.ID, "version", r.Version, "err", err)
			return
		}
		if !applied {
			d.stale.Add(1)
			d.log.Info("stale result discarded (edited/cancelled during execution)",
				"id", r.ID, "version", r.Version)
		}
	}
	attempt := store.Attempt{
		ReminderID: r.ID, Version: r.Version, AttemptNo: attemptNo,
		StartedAt: started, FinishedAt: d.clock.NowMs(),
		Outcome: outcome, Error: errStr, LatencyMs: latency,
	}

	switch outcome {
	case store.OutcomeSuccess:
		if err := d.store.MarkDelivered(r.DeliveryKey); err != nil {
			d.log.Error("dedupe record failed", "id", r.ID, "err", err)
		}
		d.delivered.Add(1)
		finishAttempt(attempt, store.StatusDelivered, nil, "")
		return
	case store.OutcomePermanent:
		d.failed.Add(1)
		finishAttempt(attempt, store.StatusFailed, nil, "permanent: "+errStr)
		return
	case store.OutcomeUncertain:
		// It may have landed: record distinctly and keep retrying under the
		// same key (the destination dedupes). If the budget runs out, say
		// "possibly delivered" — never a clean "failed".
		if attemptNo >= d.policy.MaxAttempts {
			d.failed.Add(1)
			finishAttempt(attempt, store.StatusFailed, nil,
				fmt.Sprintf("exhausted after %d attempts; last outcome uncertain — possibly delivered (reconcile via %s)",
					attemptNo, r.DeliveryKey))
			return
		}
		next := d.clock.NowMs() + d.backoff(attemptNo).Milliseconds()
		finishAttempt(attempt, store.StatusRetrying, &next, "uncertain: "+errStr)
		if delay := next - d.clock.NowMs(); delay < 1000 {
			time.AfterFunc(time.Duration(delay)*time.Millisecond, func() { d.Kick() })
		}
	default:
		if attemptNo >= d.policy.MaxAttempts {
			d.failed.Add(1)
			finishAttempt(attempt, store.StatusFailed, nil,
				fmt.Sprintf("exhausted after %d attempts: %s", attemptNo, errStr))
			return
		}
		next := d.clock.NowMs() + d.backoff(attemptNo).Milliseconds()
		finishAttempt(attempt, store.StatusRetrying, &next, "retryable: "+errStr)
		if delay := next - d.clock.NowMs(); delay < 1000 {
			time.AfterFunc(time.Duration(delay)*time.Millisecond, func() { d.Kick() })
		}
	}
}
