package store_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/sanskarpan/product-engineer-ps/solution/internal/store"
)

func open(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
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
	s, err := store.Open(path)
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
	s2, err := store.Open(path)
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
func TestEditBumpsVersionAndReschedules(t *testing.T) {
	s := open(t)
	now := time.Now().UnixMilli()
	if err := s.Create(mk("rem_edit", now)); err != nil {
		t.Fatal(err)
	}
	updated, err := s.Edit("rem_edit", "take two pills", "Asia/Kolkata", "2026-09-20T10:00", now+3600_000)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != 2 || updated.Status != store.StatusScheduled || updated.Attempts != 0 {
		t.Fatalf("bad edit result: %+v", updated)
	}
	if updated.DeliveryKey != store.DeliveryKey("rem_edit", 2) {
		t.Fatalf("delivery key not rotated: %s", updated.DeliveryKey)
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
	if _, err := s.Edit("rem_race", "new content", "Asia/Kolkata", "2026-09-20T10:00", now+3600_000); err != nil {
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

// Terminal rows are not editable.
func TestEditTerminalRejected(t *testing.T) {
	s := open(t)
	if err := s.Create(mk("rem_term", time.Now().UnixMilli())); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Cancel("rem_term"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Edit("rem_term", "x", "Asia/Kolkata", "2026-09-20T10:00", time.Now().UnixMilli()); err == nil {
		t.Fatal("expected edit of cancelled row to fail")
	}
}
