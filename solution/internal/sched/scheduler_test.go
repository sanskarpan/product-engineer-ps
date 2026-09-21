package sched_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sanskarpan/product-engineer-ps/solution/internal/clock"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/notify"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/sched"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/store"
)

var t0 = time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)

type harness struct {
	st   *store.Store
	clk  *clock.ManualClock
	fake *notify.Fake
	sch  *sched.Scheduler
}

func newHarness(t *testing.T, mode string, failFirst int, maxAttempts int) *harness {
	t.Helper()
	clk := clock.NewManual(t0)
	st, err := store.Open(filepath.Join(t.TempDir(), "sched.db"), clk)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	fake := notify.NewFake(mode, failFirst)
	sch := sched.New(st, fake, clk, sched.Policy{MaxAttempts: maxAttempts, BaseDelayMs: 20, MaxDelayMs: 200}, nil)
	sch.Start(2)
	t.Cleanup(sch.Stop)
	return &harness{st: st, clk: clk, fake: fake, sch: sch}
}

func create(t *testing.T, h *harness, id string, fireAt time.Time) {
	t.Helper()
	err := h.st.Create(store.Reminder{ID: id, Content: "msg " + id,
		TZ: "Asia/Kolkata", LocalWall: "2026-09-20T09:00", FireAtMs: fireAt.UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	h.sch.Kick()
}

func waitStatus(t *testing.T, h *harness, id string, timeout time.Duration, wanted ...string) store.Reminder {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got, err := h.st.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range wanted {
			if got.Status == w {
				return got
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("reminder %s did not reach %v", id, wanted)
	return store.Reminder{}
}

// AC1: clock reaches fire time → one notification, delivered, history recorded.
func TestScheduledDeliveryOnClockAdvance(t *testing.T) {
	h := newHarness(t, notify.ModeOK, 0, 5)
	create(t, h, "rem_ac1", t0.Add(time.Hour))
	// Not due yet: must stay scheduled.
	time.Sleep(150 * time.Millisecond)
	if got, _ := h.st.Get("rem_ac1"); got.Status != store.StatusScheduled {
		t.Fatalf("premature delivery: %s", got.Status)
	}
	h.clk.Advance(2 * time.Hour)
	h.sch.Kick()
	got := waitStatus(t, h, "rem_ac1", 5*time.Second, store.StatusDelivered)
	if got.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", got.Attempts)
	}
	if h.fake.LogicalCount() != 1 {
		t.Fatalf("expected 1 logical notification, got %d", h.fake.LogicalCount())
	}
	atts, _ := h.st.Attempts("rem_ac1")
	if len(atts) != 1 || atts[0].Outcome != store.OutcomeSuccess {
		t.Fatalf("attempts: %+v", atts)
	}
}

// AC3: temporary failure → bounded retry → success.
func TestTemporaryFailureThenRetry(t *testing.T) {
	h := newHarness(t, notify.ModeFailFirst, 2, 5)
	create(t, h, "rem_ac3", t0)
	// Drive retries by advancing the manual clock past each backoff.
	for i := 0; i < 30; i++ {
		h.clk.Advance(500 * time.Millisecond)
		h.sch.Kick()
		time.Sleep(20 * time.Millisecond)
		if got, _ := h.st.Get("rem_ac3"); got.Status == store.StatusDelivered {
			break
		}
	}
	got := waitStatus(t, h, "rem_ac3", time.Second, store.StatusDelivered)
	atts, _ := h.st.Attempts("rem_ac3")
	if len(atts) != 3 {
		t.Fatalf("expected 3 attempts (2 temp + success), got %d", len(atts))
	}
	if atts[0].Outcome != store.OutcomeRetryable || atts[2].Outcome != store.OutcomeSuccess {
		t.Fatalf("outcomes: %+v", atts)
	}
	if h.fake.LogicalCount() != 1 {
		t.Fatalf("exactly one logical notification, got %d", h.fake.LogicalCount())
	}
	_ = got
}

// AC3b: persistent temporary failure → failed terminal, bounded.
func TestRetryExhaustion(t *testing.T) {
	h := newHarness(t, notify.ModeAlwaysTemp, 0, 3)
	create(t, h, "rem_exh", t0)
	for i := 0; i < 40; i++ {
		h.clk.Advance(500 * time.Millisecond)
		h.sch.Kick()
		time.Sleep(15 * time.Millisecond)
		if got, _ := h.st.Get("rem_exh"); got.Status == store.StatusFailed {
			break
		}
	}
	got := waitStatus(t, h, "rem_exh", time.Second, store.StatusFailed)
	if got.Attempts != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", got.Attempts)
	}
	n1 := len(mustAttempts(t, h, "rem_exh"))
	time.Sleep(300 * time.Millisecond)
	if n2 := len(mustAttempts(t, h, "rem_exh")); n2 != n1 {
		t.Fatal("attempts continued past exhaustion")
	}
	// Permanent rejection goes failed after a single attempt.
	h2 := newHarness(t, notify.ModeAlwaysPerm, 0, 5)
	create(t, h2, "rem_perm", t0)
	got2 := waitStatus(t, h2, "rem_perm", 5*time.Second, store.StatusFailed)
	if got2.Attempts != 1 {
		t.Fatalf("permanent failure must stop after 1 attempt, got %d", got2.Attempts)
	}
}

func mustAttempts(t *testing.T, h *harness, id string) []store.Attempt {
	t.Helper()
	a, err := h.st.Attempts(id)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// AC4: duplicate execution of the same occurrence → one logical notification.
func TestDuplicateExecutionSingleLogical(t *testing.T) {
	h := newHarness(t, notify.ModeOK, 0, 5)
	create(t, h, "rem_dupe", t0)
	waitStatus(t, h, "rem_dupe", 5*time.Second, store.StatusDelivered)
	got, _ := h.st.Get("rem_dupe")
	// Simulate a redelivery: same delivery key sent again (scheduler firing
	// twice). The destination must not double-notify.
	ok, _, _ := h.fake.Send(t.Context(), got.DeliveryKey, got.Content)
	if !ok {
		t.Fatal("redelivery of same key should be acknowledged")
	}
	if h.fake.LogicalCount() != 1 {
		t.Fatalf("duplicate execution produced %d logical notifications", h.fake.LogicalCount())
	}
}

// AC2: overdue work while stopped is discovered on restart (policy: ASAP).
func TestRestartRecoveryOverdue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.db")
	clk := clock.NewManual(t0)
	st, err := store.Open(path, clk)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Create(store.Reminder{ID: "rem_od", Content: "overdue",
		TZ: "America/New_York", LocalWall: "2026-09-20T09:00", FireAtMs: t0.Add(time.Hour).UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	// Service stopped; clock passes the fire time with nothing running.
	clk.Advance(3 * time.Hour)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	// Restart: reopen + start scheduler at the later clock.
	st2, err := store.Open(path, clk)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st2.Close() })
	fake := notify.NewFake(notify.ModeOK, 0)
	sch := sched.New(st2, fake, clk, sched.Policy{MaxAttempts: 5, BaseDelayMs: 20, MaxDelayMs: 200}, nil)
	sch.Start(1)
	t.Cleanup(sch.Stop)
	sch.Kick()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := st2.Get("rem_od")
		if got.Status == store.StatusDelivered {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("overdue reminder was not recovered after restart")
}

// AC7: lost acknowledgement → retry reconciles to delivered, still once.
// The uncertain attempt is recorded distinctly from plain retryable.
func TestLostAckReconciles(t *testing.T) {
	h := newHarness(t, notify.ModeLostAckFirst, 1, 5)
	create(t, h, "rem_lost", t0)
	for i := 0; i < 30; i++ {
		h.clk.Advance(500 * time.Millisecond)
		h.sch.Kick()
		time.Sleep(20 * time.Millisecond)
		if got, _ := h.st.Get("rem_lost"); got.Status == store.StatusDelivered {
			break
		}
	}
	waitStatus(t, h, "rem_lost", time.Second, store.StatusDelivered)
	if h.fake.LogicalCount() != 1 {
		t.Fatalf("lost-ack retry must keep one logical notification, got %d", h.fake.LogicalCount())
	}
	atts := mustAttempts(t, h, "rem_lost")
	if len(atts) != 2 || atts[0].Outcome != store.OutcomeUncertain {
		t.Fatalf("first attempt must be uncertain, got %+v", atts)
	}
}

// Uncertain outcomes must never terminate as a clean failure. A lost ack on
// the only allowed attempt exhausts honestly: failed, but reporting
// possible delivery (the notification DID land) instead of a lying "failed".
// Note: with budget to spare, a lost-ack retry always reconciles via the
// same delivery key — so this terminal-uncertain case needs maxAttempts=1.
func TestUncertainExhaustionIsHonest(t *testing.T) {
	h := newHarness(t, notify.ModeLostAckFirst, 5, 1)
	create(t, h, "rem_unc", t0)
	h.clk.Advance(time.Hour)
	h.sch.Kick()
	got := waitStatus(t, h, "rem_unc", 5*time.Second, store.StatusFailed)
	if !strings.Contains(got.LastError, "possibly delivered") {
		t.Fatalf("exhausted uncertain run must say possibly delivered: %q", got.LastError)
	}
	if h.fake.LogicalCount() != 1 {
		t.Fatalf("exactly one logical notification, got %d", h.fake.LogicalCount())
	}
	atts := mustAttempts(t, h, "rem_unc")
	if len(atts) != 1 || atts[0].Outcome != store.OutcomeUncertain {
		t.Fatalf("attempt must be uncertain, got %+v", atts)
	}
}

// Durable suppression: a key already delivered (even by a previous process
// generation that crashed before finishing) is reconciled without
// re-sending — zero new destination calls, and the keys table proves it
// across a real close/reopen.
func TestDurableSuppressionAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "supp.db")
	clk := clock.NewManual(t0)
	st, err := store.Open(path, clk)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Create(store.Reminder{ID: "rem_supp", Content: "hi",
		TZ: "Asia/Kolkata", LocalWall: "2026-09-20T09:00", FireAtMs: t0.UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	// Previous generation notified under this key, then crashed before the
	// row left 'running'. Only the durable keys table remembers.
	if err := st.MarkDelivered("rem_supp:v1"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := st.ClaimDue(t0.Add(time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	// New generation, fresh destination memory: reopen, recover, schedule.
	st2, err := store.Open(path, clk)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	fake2 := notify.NewFake(notify.ModeOK, 0)
	sch2 := sched.New(st2, fake2, clk, sched.Policy{MaxAttempts: 5, BaseDelayMs: 20, MaxDelayMs: 200}, nil)
	sch2.Start(1)
	defer sch2.Stop()
	clk.Advance(2 * time.Hour)
	sch2.Kick()
	h2 := &harness{st: st2, clk: clk, fake: fake2, sch: sch2}
	waitStatus(t, h2, "rem_supp", 5*time.Second, store.StatusDelivered)
	if n := fake2.DeliveriesFor("rem_supp:v1"); n != 0 {
		t.Fatalf("restart redelivery re-sent %d times, want 0 (durable suppression)", n)
	}
	atts, _ := st2.Attempts("rem_supp")
	last := atts[len(atts)-1]
	if last.Error != "duplicate suppressed (already delivered)" {
		t.Fatalf("suppression must be recorded in history: %+v", last)
	}
}
