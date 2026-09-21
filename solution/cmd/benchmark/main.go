// Command benchmark is the Problem 3 verification benchmark: a repeatable,
// deterministic workflow-correctness run (no sleeps for real minutes, no
// paid services). It exits non-zero on any mismatch.
//
// Sequence:
//  1. Creates 20 scheduled items across Asia/Kolkata + America/New_York
//     (delivered, edited, cancelled, temporarily failing, permanently failing,
//     plus DST gap + overlap walls).
//  2. Stops and restarts the service (close + reopen store, new scheduler)
//     before all due work is processed.
//  3. Simulates duplicate execution for one occurrence.
//  4. Advances the injected clock until everything settles.
//  5. Reports counts by terminal state and proves exactly-once logical
//     notification per active successful occurrence.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sanskarpan/product-engineer-ps/solution/internal/clock"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/notify"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/sched"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/store"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/tz"
)

var failures int

func check(cond bool, format string, args ...any) {
	if !cond {
		failures++
		fmt.Printf("MISMATCH: "+format+"\n", args...)
	}
}

func mustCreate(st store.Provider, id, content, zone, wall string) time.Time {
	fire, note, err := tz.Resolve(wall, zone)
	if err != nil {
		panic(err)
	}
	if note != "" {
		fmt.Printf("  tz: %-8s %-16s -> %s (%s)\n", zone, wall, fire.Format(time.RFC3339), note)
	}
	if err := st.Create(store.Reminder{ID: id, Content: content, TZ: zone, LocalWall: wall, FireAtMs: fire.UnixMilli()}); err != nil {
		panic(err)
	}
	return fire
}

func main() {
	dir, err := os.MkdirTemp("", "rem-bench")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "bench.db")

	start := time.Date(2026, 3, 7, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManual(start)
	fake, err := notify.NewFakePersisted(filepath.Join(dir, "deliveries.log"), notify.ModeFailFirst, 2) // first 2 sends fail temp
	if err != nil {
		panic(err)
	}
	defer fake.Close()
	policy := sched.Policy{MaxAttempts: 5, BaseDelayMs: 20, MaxDelayMs: 200}

	st, err := store.Open(dbPath, clk)
	if err != nil {
		panic(err)
	}

	fmt.Println("== create 20 items ==")
	// DST gap wall (nonexistent 02:30 -> pushed to 03:30 EDT).
	mustCreate(st, "b-gap", "dst gap", "America/New_York", "2026-03-08T02:30")
	// 3 temporarily failing (earliest due: absorb the 2 fail-firsts, then succeed).
	for i := 1; i <= 3; i++ {
		mustCreate(st, fmt.Sprintf("b-temp%d", i), "temp", "Asia/Kolkata", "2026-03-08T10:0"+fmt.Sprint(i))
	}
	// 10 plain across both zones, spread Mar -> Nov (one ambiguous overlap wall).
	plains := [][2]string{
		{"Asia/Kolkata", "2026-03-09T09:00"}, {"America/New_York", "2026-04-01T09:00"},
		{"Asia/Kolkata", "2026-05-01T09:00"}, {"America/New_York", "2026-06-01T09:00"},
		{"Asia/Kolkata", "2026-07-01T09:00"}, {"America/New_York", "2026-08-01T09:00"},
		{"Asia/Kolkata", "2026-09-20T09:00"}, {"America/New_York", "2026-10-01T09:00"},
		{"America/New_York", "2026-11-01T01:30"}, {"Asia/Kolkata", "2026-11-02T09:00"},
	}
	for i, p := range plains {
		mustCreate(st, fmt.Sprintf("b-plain%02d", i+1), "plain", p[0], p[1])
	}
	// 2 to edit before execution, 2 to cancel, 2 permanently failing (empty
	// content is rejected by the destination as permanent — no mode juggling).
	for i := 1; i <= 2; i++ {
		mustCreate(st, fmt.Sprintf("b-edit%d", i), "old content", "Asia/Kolkata", "2026-09-21T09:00")
		mustCreate(st, fmt.Sprintf("b-cancel%d", i), "cancel me", "America/New_York", "2026-09-21T09:00")
		if err := st.Create(store.Reminder{ID: fmt.Sprintf("b-doom%d", i), Content: "",
			TZ: "Asia/Kolkata", LocalWall: "2026-09-21T09:00",
			FireAtMs: tz.MustResolve("2026-09-21T09:00", "Asia/Kolkata").UnixMilli()}); err != nil {
			panic(err)
		}
	}

	fmt.Println("== edit 2, cancel 2 ==")
	for i := 1; i <= 2; i++ {
		id := fmt.Sprintf("b-edit%d", i)
		if _, err := st.Edit(id, "new content", "Asia/Kolkata", "2026-09-22T09:00",
			tz.MustResolve("2026-09-22T09:00", "Asia/Kolkata").UnixMilli(), -1); err != nil {
			panic(err)
		}
		if ok, err := st.Cancel(fmt.Sprintf("b-cancel%d", i)); err != nil || !ok {
			panic(fmt.Sprintf("cancel: %v %v", ok, err))
		}
	}

	sch := sched.New(st, fake, clk, policy, nil)
	sch.Start(2)

	// Process partway, then restart before all due work completes.
	fmt.Println("== advance partway, then restart ==")
	clk.Set(time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC))
	sch.Kick()
	settle(st, clk, sch, 3*time.Second)

	sch.Stop()
	if err := st.Close(); err != nil {
		panic(err)
	}
	st, err = store.Open(dbPath, clk) // restart: overdue work must be discovered
	if err != nil {
		panic(err)
	}
	defer st.Close()
	sch = sched.New(st, fake, clk, policy, nil)
	sch.Start(2)
	defer sch.Stop()

	fmt.Println("== advance to end ==")
	end := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	clk.Set(end)
	sch.Kick()
	settle(st, clk, sch, 10*time.Second)

	// Duplicate execution for one delivered occurrence.
	fmt.Println("== duplicate execution ==")
	got, err := st.Get("b-plain01")
	if err != nil {
		panic(err)
	}
	before := fake.LogicalCount()
	for i := 0; i < 3; i++ {
		if ok, _, _ := fake.Send(tCtx(), got.DeliveryKey, got.Content); !ok {
			panic("redelivery of delivered key must be acknowledged")
		}
	}
	check(fake.LogicalCount() == before, "duplicate execution added logical notifications (%d -> %d)",
		before, fake.LogicalCount())

	fmt.Println("== report ==")
	counts, err := st.CountByStatus()
	if err != nil {
		panic(err)
	}
	for _, s := range []string{"delivered", "failed", "cancelled", "scheduled", "retrying", "running"} {
		fmt.Printf("  %-10s %d\n", s, counts[s])
	}
	fmt.Printf("  logicalDelivered %d\n", fake.LogicalCount())

	check(counts["failed"] == 2, "failed: got %d want 2 (doomed)", counts["failed"])
	check(counts["cancelled"] == 2, "cancelled: got %d want 2", counts["cancelled"])
	check(counts["scheduled"] == 0 && counts["retrying"] == 0 && counts["running"] == 0,
		"unsettled work remains: %v", counts)
	// 20 items, 2 cancelled, 2 failed => 16 delivered... plus nothing else.
	// (gap + 3 temp + 10 plain + 2 edited = 16.)
	check(counts["delivered"] == 16, "delivered breakdown mismatch: %v", counts)
	check(fake.LogicalCount() == 16, "exactly-once: logical=%d want 16", fake.LogicalCount())

	// Every delivered reminder: attempts recorded, single ordered chain.
	items, _ := st.List(100)
	for _, r := range items {
		atts, _ := st.Attempts(r.ID)
		switch r.Status {
		case "delivered":
			check(len(atts) == r.Attempts && len(atts) >= 1, "%s: history mismatch", r.ID)
		case "failed":
			check(len(atts) >= 1, "%s: failed without attempts", r.ID)
		case "cancelled":
			check(len(atts) == 0, "%s: cancelled should have no attempts, got %d", r.ID, len(atts))
		}
	}

	sch.Stop()
	if failures > 0 {
		fmt.Printf("BENCHMARK FAIL (%d mismatches)\n", failures)
		os.Exit(1)
	}
	fmt.Println("BENCHMARK PASS")
}

// settle advances the manual clock in small steps until every reminder is
// terminal or the real-time budget runs out (retries need clock movement).
func settle(st store.Provider, clk *clock.ManualClock, sch *sched.Scheduler, budget time.Duration) {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		counts, _ := st.CountByStatus()
		if counts["scheduled"]+counts["retrying"]+counts["running"] == 0 {
			return
		}
		clk.Advance(time.Hour)
		sch.Kick()
		time.Sleep(20 * time.Millisecond)
	}
}
