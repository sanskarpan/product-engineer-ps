package tz

import (
	"fmt"
	"time"
)

// Layout for wall-clock input without offset, e.g. "2026-09-20T09:00".
const WallLayout = "2006-01-02T15:04"

// Resolve converts a wall-clock time in a named IANA zone to a UTC instant.
//
// Policy (documented, deterministic):
//   - Unknown zone, empty input, or unparsable wall time → error, no guessing.
//   - Nonexistent local time (spring-forward gap, e.g. 2026-03-08 02:30 in
//     America/New_York): pushed forward by the gap so the reminder still
//     fires the same morning. The returned note says what happened.
//   - Ambiguous local time (fall-back overlap, e.g. 2026-11-01 01:30 in
//     America/New_York): first occurrence (still daylight time). The note
//     records the chosen offset.
//
// Go's time.Date already picks the first occurrence for overlaps, but its
// gap normalization is an implementation detail we do not rely on: Resolve
// verifies via round-trip and applies the documented policy itself.
func Resolve(wall, zone string) (utc time.Time, note string, err error) {
	loc, err := time.LoadLocation(zone)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("unknown time zone %q", zone)
	}
	parsed, err := time.ParseInLocation(WallLayout, wall, loc)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("bad local time %q (want YYYY-MM-DDTHH:MM): %w", wall, err)
	}
	// Nonexistent detection: a gap time does not round-trip. Push forward
	// by one hour (all real-world DST gaps are exactly one hour) so the
	// reminder still fires the same morning, deterministically.
	if parsed.Format(WallLayout) != wall {
		shifted := parsed.Add(time.Hour)
		return shifted.UTC(),
			fmt.Sprintf("nonexistent local time %s in %s (DST gap): fired at %s instead",
				wall, zone, shifted.In(loc).Format(WallLayout)),
			nil
	}
	// Ambiguous detection: one hour later shows the same wall at a different
	// offset, proving this wall occurs twice. Go already picked the first.
	later := parsed.Add(time.Hour).In(loc)
	if later.Format(WallLayout) == wall {
		name, off := parsed.Zone()
		_, offLater := later.Zone()
		if offLater != off {
			return parsed.UTC(),
				fmt.Sprintf("ambiguous local time %s in %s (DST overlap): first occurrence (%s)",
					wall, zone, name),
				nil
		}
	}
	return parsed.UTC(), "", nil
}

// MustResolve is Resolve that panics — for tests and fixtures only.
func MustResolve(wall, zone string) time.Time {
	t, _, err := Resolve(wall, zone)
	if err != nil {
		panic(err)
	}
	return t
}
