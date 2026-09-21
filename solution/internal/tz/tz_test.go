package tz_test

import (
	"testing"
	"time"

	"github.com/sanskarpan/product-engineer-ps/solution/internal/tz"
)

func TestTwoZonesSameWallDifferentInstant(t *testing.T) {
	kolkata := tz.MustResolve("2026-09-20T09:00", "Asia/Kolkata")
	newyork := tz.MustResolve("2026-09-20T09:00", "America/New_York")
	if kolkata.Equal(newyork) {
		t.Fatal("same wall time in different zones must be different instants")
	}
	// Kolkata is UTC+5:30 with no DST: 09:00 IST = 03:30Z.
	if want := "2026-09-20T03:30:00Z"; kolkata.Format(time.RFC3339) != want {
		t.Fatalf("kolkata: got %s want %s", kolkata.Format(time.RFC3339), want)
	}
}

func TestNonexistentSpringForwardIsPushed(t *testing.T) {
	// 2026-03-08 02:30 does not exist in America/New_York (clocks jump
	// 02:00 -> 03:00). Policy: push forward one hour to 03:30 EDT = 07:30Z,
	// same morning, with a note.
	utc, note, err := tz.Resolve("2026-03-08T02:30", "America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	if note == "" {
		t.Fatal("expected a tzNote explaining the gap adjustment")
	}
	if got := utc.Format(time.RFC3339); got != "2026-03-08T07:30:00Z" {
		t.Fatalf("gap instant: got %s", got)
	}
}

func TestAmbiguousFallBackTakesFirst(t *testing.T) {
	// 2026-11-01 01:30 happens twice in America/New_York. Policy: first
	// occurrence (still daylight time, UTC-4).
	utc, note, err := tz.Resolve("2026-11-01T01:30", "America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	if note == "" {
		t.Fatal("expected a tzNote explaining the overlap choice")
	}
	if got := utc.Format(time.RFC3339); got != "2026-11-01T05:30:00Z" {
		t.Fatalf("overlap instant: got %s want 2026-11-01T05:30:00Z", got)
	}
}

func TestUnknownZoneAndBadWallAreErrors(t *testing.T) {
	if _, _, err := tz.Resolve("2026-09-20T09:00", "Mars/Olympus"); err == nil {
		t.Fatal("expected error for unknown zone")
	}
	if _, _, err := tz.Resolve("not-a-time", "Asia/Kolkata"); err == nil {
		t.Fatal("expected error for bad wall time")
	}
}
