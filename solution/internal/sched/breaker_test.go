package sched_test

import (
	"testing"
	"time"

	"github.com/sanskarpan/product-engineer-ps/solution/internal/sched"
)

func TestBreakerOpensAfterThreshold(t *testing.T) {
	b := sched.NewBreaker(sched.BreakerConfig{Threshold: 3, Cooldown: 50 * time.Millisecond})
	b.RecordFailure()
	b.RecordFailure()
	if !b.Allow() {
		t.Fatal("should still be closed after 2")
	}
	b.RecordFailure()
	if b.State() != sched.BreakerOpen || b.Allow() {
		t.Fatalf("expected open+deny, got %s", b.State())
	}
	if b.Opens() != 1 {
		t.Fatalf("opens=%d", b.Opens())
	}
}

func TestBreakerTrialClosesAndReopens(t *testing.T) {
	b := sched.NewBreaker(sched.BreakerConfig{Threshold: 1, Cooldown: 20 * time.Millisecond})
	b.RecordFailure()
	time.Sleep(40 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("expected trial admission")
	}
	if b.Allow() {
		t.Fatal("only one trial in flight")
	}
	b.RecordSuccess()
	if b.State() != sched.BreakerClosed {
		t.Fatalf("trial success should close, got %s", b.State())
	}
	// Re-open, then fail the trial: re-opens.
	b.RecordFailure()
	time.Sleep(40 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("expected second trial")
	}
	b.RecordFailure()
	if b.State() != sched.BreakerOpen || b.Opens() != 3 {
		t.Fatalf("failed trial should re-open: %s opens=%d", b.State(), b.Opens())
	}
}

func TestBreakerReleaseTrial(t *testing.T) {
	b := sched.NewBreaker(sched.BreakerConfig{Threshold: 1, Cooldown: 20 * time.Millisecond})
	b.RecordFailure()
	time.Sleep(40 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("expected trial")
	}
	b.ReleaseTrial()
	if b.State() != sched.BreakerOpen || b.Allow() {
		t.Fatalf("release must revert to open with fresh cooldown: %s", b.State())
	}
}

func TestBreakerDisabled(t *testing.T) {
	b := sched.NewBreaker(sched.BreakerDisabled())
	for i := 0; i < 50; i++ {
		b.RecordFailure()
	}
	if !b.Allow() || b.State() != sched.BreakerClosed {
		t.Fatal("disabled breaker must always allow")
	}
}
