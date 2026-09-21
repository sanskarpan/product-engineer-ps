package store_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sanskarpan/product-engineer-ps/solution/internal/store"
)

func open(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "test.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mk(id string, fireMs int64) store.Reminder {
	return store.Reminder{ID: id, Content: "take pill", TZ: "Asia/Kolkata",
		LocalWall: "2026-09-20T09:00", FireAtMs: fireMs}
}

// A row stuck in 'running' (kill -9 mid-delivery) must become schedulable
// on the next Open via recoverInFlight.
func TestCrashRecoveryReschedulesInFlight(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash.db")
	s, err := store.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create(mk("rem_crash", time.Now().UnixMilli())); err != nil {
		t.Fatal(err)
	}
	claimed, ok, contended, err := s.ClaimDue(time.Now().UnixMilli() + 1000)
	if err != nil || !ok || contended || claimed.Status != store.StatusRunning {
		t.Fatalf("claim: %v ok=%v contended=%v status=%s", err, ok, contended, claimed.Status)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := store.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.Get("rem_crash")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != store.StatusScheduled {
		t.Fatalf("expected scheduled after recovery, got %s", got.Status)
	}
}

// Edit bumps the version and reschedules; the old delivery key retires.
// The retry budget carries across versions so edits cannot mint unbounded
// retries; history rows keep their version for audit.
func TestEditBumpsVersionAndReschedules(t *testing.T) {
	s := open(t)
	now := time.Now().UnixMilli()
	if err := s.Create(mk("rem_edit", now)); err != nil {
		t.Fatal(err)
	}
	// Burn one attempt first so budget-carry is observable (via the atomic
	// path, mirroring production: history row + state move together).
	claimed, ok, _, err := s.ClaimDue(now + 1000)
	if err != nil || !ok {
		t.Fatalf("claim: %v ok=%v", err, ok)
	}
	next := now + 60_000
	applied, err := s.FinishAttempt(store.Attempt{
		ReminderID: "rem_edit", Version: claimed.Version, AttemptNo: 1,
		StartedAt: now, FinishedAt: now + 1, Outcome: store.OutcomeRetryable, Error: "boom",
	}, store.StatusRetrying, 1, &next, "boom")
	if err != nil || !applied {
		t.Fatalf("finish: %v applied=%v", err, applied)
	}
	updated, err := s.Edit("rem_edit", "take two pills", "Asia/Kolkata", "2026-09-20T10:00", now+3600_000, -1)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != 2 || updated.Status != store.StatusScheduled {
		t.Fatalf("bad edit result: %+v", updated)
	}
	if updated.Attempts != 1 {
		t.Fatalf("budget must carry across versions, got %d", updated.Attempts)
	}
	if updated.DeliveryKey != store.DeliveryKey("rem_edit", 2) {
		t.Fatalf("delivery key not rotated: %s", updated.DeliveryKey)
	}
	attempts, _ := s.Attempts("rem_edit")
	if len(attempts) != 1 || attempts[0].Version != 1 {
		t.Fatalf("pre-edit history must be retained with its version: %+v", attempts)
	}
}

// A no-op edit changes nothing: same version, no reschedule.
func TestNoOpEditIsStable(t *testing.T) {
	s := open(t)
	now := time.Now().UnixMilli()
	if err := s.Create(mk("rem_noop", now)); err != nil {
		t.Fatal(err)
	}
	same, err := s.Edit("rem_noop", "take pill", "Asia/Kolkata", "2026-09-20T09:00", now, -1)
	if err != nil {
		t.Fatal(err)
	}
	if same.Version != 1 {
		t.Fatalf("no-op edit bumped version: %+v", same)
	}
}

// expectedVersion guards concurrent editors: stale writers get a conflict
// instead of silent last-writer-wins.
func TestEditVersionConflict(t *testing.T) {
	s := open(t)
	now := time.Now().UnixMilli()
	if err := s.Create(mk("rem_conf", now)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Edit("rem_conf", "v2 content", "Asia/Kolkata", "2026-09-20T10:00", now+1000, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Edit("rem_conf", "stale writer", "Asia/Kolkata", "2026-09-20T11:00", now+2000, 1); err == nil {
		t.Fatal("expected version conflict for stale writer")
	} else if got := err.Error(); !strings.Contains(got, "version conflict") {
		t.Fatalf("wrong error: %s", got)
	}
	got, _ := s.Get("rem_conf")
	if got.Version != 2 || got.Content != "v2 content" {
		t.Fatalf("loser overwrote winner: %+v", got)
	}
}

// An edit that reschedules into the future must not be claimable at the
// old due time: the claim re-checks version and due instant, closing the
// stale-claim half of the edit/execution race.
func TestRescheduledRowNotClaimedEarly(t *testing.T) {
	s := open(t)
	now := time.Now().UnixMilli()
	if err := s.Create(mk("rem_future", now)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Edit("rem_future", "take pill", "Asia/Kolkata", "2026-09-20T10:00", now+3600_000, -1); err != nil {
		t.Fatal(err)
	}
	if _, ok, contended, err := s.ClaimDue(now + 1000); err != nil || ok || contended {
		t.Fatalf("rescheduled row claimed early: ok=%v contended=%v err=%v", ok, contended, err)
	}
}

// FinishDelivery on a stale version must not overwrite the edited row.
// This is the edit-vs-execution race policy.
func TestStaleFinishIsDiscarded(t *testing.T) {
	s := open(t)
	now := time.Now().UnixMilli()
	if err := s.Create(mk("rem_race", now)); err != nil {
		t.Fatal(err)
	}
	claimed, ok, _, err := s.ClaimDue(now + 1000)
	if err != nil || !ok {
		t.Fatalf("claim: %v ok=%v", err, ok)
	}
	// User edits while the worker holds the old version.
	if _, err := s.Edit("rem_race", "new content", "Asia/Kolkata", "2026-09-20T10:00", now+3600_000, -1); err != nil {
		t.Fatal(err)
	}
	// Worker's finish for v1 must be discarded.
	applied, err := s.FinishDelivery("rem_race", claimed.Version, store.StatusDelivered, 1, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("stale finish must not apply")
	}
	got, _ := s.Get("rem_race")
	if got.Status != store.StatusScheduled || got.Version != 2 {
		t.Fatalf("edited row clobbered: %+v", got)
	}
	if n, _ := s.Attempts("rem_race"); len(n) != 0 {
		t.Fatalf("no attempts should exist for the discarded version, got %d", len(n))
	}
}

// FinishAttempt persists attempt + outcome atomically: both land or
// neither does, so history length and attempt_count cannot skew on crash.
func TestFinishAttemptAtomic(t *testing.T) {
	s := open(t)
	now := time.Now().UnixMilli()
	if err := s.Create(mk("rem_atomic", now)); err != nil {
		t.Fatal(err)
	}
	claimed, ok, _, err := s.ClaimDue(now + 1000)
	if err != nil || !ok {
		t.Fatalf("claim: %v ok=%v", err, ok)
	}
	applied, err := s.FinishAttempt(store.Attempt{
		ReminderID: "rem_atomic", Version: claimed.Version, AttemptNo: 1,
		StartedAt: now, FinishedAt: now + 5, Outcome: store.OutcomeSuccess, LatencyMs: 5,
	}, store.StatusDelivered, 1, nil, "")
	if err != nil || !applied {
		t.Fatalf("finish: %v applied=%v", err, applied)
	}
	got, _ := s.Get("rem_atomic")
	atts, _ := s.Attempts("rem_atomic")
	if got.Status != store.StatusDelivered || got.Attempts != 1 || len(atts) != 1 {
		t.Fatalf("atomic finish skewed: %+v attempts=%d", got, len(atts))
	}
	// Stale version via the atomic path: attempt row still recorded for
	// audit, but the row state is untouched.
	if _, err := s.Edit("rem_atomic", "x", "Asia/Kolkata", "2026-09-20T10:00", now, -1); err == nil {
		t.Fatal("expected edit of delivered row to fail")
	}
}

// The delivered-keys table survives reopen: dedupe is durable, not process
// memory, so crash + redelivery still notifies exactly once per key.
func TestDeliveredKeysSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys.db")
	s, err := store.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkDelivered("rem_x:v1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := store.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if ok, err := s2.IsDelivered("rem_x:v1"); err != nil || !ok {
		t.Fatalf("delivered key lost across reopen: %v %v", ok, err)
	}
	if ok, _ := s2.IsDelivered("rem_x:v2"); ok {
		t.Fatal("new version must be a new occurrence")
	}
}

// Cancel wins over an in-flight execution: the late finish is discarded.
func TestCancelWinsOverInFlight(t *testing.T) {
	s := open(t)
	now := time.Now().UnixMilli()
	if err := s.Create(mk("rem_cancel", now)); err != nil {
		t.Fatal(err)
	}
	claimed, ok, _, err := s.ClaimDue(now + 1000)
	if err != nil || !ok {
		t.Fatalf("claim: %v ok=%v", err, ok)
	}
	okCancel, err := s.Cancel("rem_cancel")
	if err != nil || !okCancel {
		t.Fatalf("cancel: %v ok=%v", err, okCancel)
	}
	applied, err := s.FinishDelivery("rem_cancel", claimed.Version, store.StatusDelivered, 1, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("finish after cancel must be discarded")
	}
	got, _ := s.Get("rem_cancel")
	if got.Status != store.StatusCancelled {
		t.Fatalf("expected cancelled, got %s", got.Status)
	}
	// Second cancel is a no-op: already terminal.
	if ok2, _ := s.Cancel("rem_cancel"); ok2 {
		t.Fatal("cancel of terminal row should report false")
	}
}

// Replay redrives a failed row as a new occurrence with a fresh budget —
// the one sanctioned exception to budget-carry, and only for failed rows.
func TestReplayFailed(t *testing.T) {
	s := open(t)
	now := time.Now().UnixMilli()
	if err := s.Create(mk("rem_rp", now)); err != nil {
		t.Fatal(err)
	}
	claimed, ok, _, err := s.ClaimDue(now + 1000)
	if err != nil || !ok {
		t.Fatalf("claim: %v ok=%v", err, ok)
	}
	if _, err := s.FinishDelivery("rem_rp", claimed.Version, store.StatusFailed, 5, nil, "exhausted"); err != nil {
		t.Fatal(err)
	}
	re, err := s.Replay("rem_rp")
	if err != nil {
		t.Fatal(err)
	}
	if re.Version != 2 || re.Status != store.StatusScheduled || re.Attempts != 0 {
		t.Fatalf("bad replay: %+v", re)
	}
	if _, err := s.Replay("rem_rp"); err == nil {
		t.Fatal("replay of scheduled row must fail")
	}
}

// Retention caps history: newest N rows per reminder survive.
func TestPruneAttempts(t *testing.T) {
	s := open(t)
	now := time.Now().UnixMilli()
	if err := s.Create(mk("rem_pr", now)); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if err := s.RecordAttempt(store.Attempt{ReminderID: "rem_pr", Version: 1,
			AttemptNo: i, StartedAt: now, FinishedAt: now, Outcome: store.OutcomeRetryable}); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := s.PruneAttempts(2)
	if err != nil || removed != 3 {
		t.Fatalf("prune: %v removed=%d", err, removed)
	}
	atts, _ := s.Attempts("rem_pr")
	if len(atts) != 2 || atts[0].AttemptNo != 4 || atts[1].AttemptNo != 5 {
		t.Fatalf("kept wrong rows: %+v", atts)
	}
}

// OldestDue reports backlog age; false when idle.
func TestOldestDue(t *testing.T) {
	s := open(t)
	now := time.Now().UnixMilli()
	if _, ok, err := s.OldestDue(now); err != nil || ok {
		t.Fatalf("idle: ok=%v err=%v", ok, err)
	}
	if err := s.Create(mk("rem_od1", now-5000)); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(mk("rem_od2", now-1000)); err != nil {
		t.Fatal(err)
	}
	due, ok, err := s.OldestDue(now)
	if err != nil || !ok || due != now-5000 {
		t.Fatalf("oldest: due=%d ok=%v err=%v", due, ok, err)
	}
}

// Terminal rows are not editable.
func TestEditTerminalRejected(t *testing.T) {
	s := open(t)
	if err := s.Create(mk("rem_term", time.Now().UnixMilli())); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Cancel("rem_term"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Edit("rem_term", "x", "Asia/Kolkata", "2026-09-20T10:00", time.Now().UnixMilli(), -1); err == nil {
		t.Fatal("expected edit of cancelled row to fail")
	}
}
