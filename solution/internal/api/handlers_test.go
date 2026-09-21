package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/sanskarpan/product-engineer-ps/solution/internal/api"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/clock"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/notify"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/sched"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/store"
)

var t0 = time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)

type harness struct {
	server *httptest.Server
	st     *store.Store
	clk    *clock.ManualClock
	fake   *notify.Fake
	sch    *sched.Scheduler
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	clk := clock.NewManual(t0)
	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"), clk)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	fake := notify.NewFake(notify.ModeOK, 0)
	sch := sched.New(st, fake, clk, sched.Policy{MaxAttempts: 5, BaseDelayMs: 20, MaxDelayMs: 200}, nil)
	sch.Start(2)
	t.Cleanup(sch.Stop)
	srv := httptest.NewServer(api.New(st, sch, fake, fake, clk, clk).Handler())
	t.Cleanup(srv.Close)
	return &harness{server: srv, st: st, clk: clk, fake: fake, sch: sch}
}

func post(t *testing.T, h *harness, path string, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(h.server.URL+path, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func patch(t *testing.T, h *harness, path, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPatch, h.server.URL+path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func get(t *testing.T, h *harness, path string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(h.server.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func waitStatus(t *testing.T, h *harness, id string, timeout time.Duration, wanted ...string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		code, out := get(t, h, "/reminders/"+id)
		if code != 200 {
			t.Fatalf("get %s: %d", id, code)
		}
		for _, w := range wanted {
			if out["reminder"].(map[string]any)["status"] == w {
				return out
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("reminder %s did not reach %v", id, wanted)
	return nil
}

func advance(t *testing.T, h *harness, d time.Duration) {
	t.Helper()
	code, _ := post(t, h, "/admin/clock", fmt.Sprintf(`{"advanceMs":%d}`, d.Milliseconds()))
	if code != 200 {
		t.Fatalf("clock advance: %d", code)
	}
}

// AC1 over HTTP: create in Asia/Kolkata, advance clock, delivered.
func TestCreateAndDeliverViaClock(t *testing.T) {
	h := newHarness(t)
	code, out := post(t, h, "/reminders", `{"id":"r1","content":"call mom","tz":"Asia/Kolkata","localTime":"2026-09-20T09:00"}`)
	if code != 201 {
		t.Fatalf("create: %d %v", code, out)
	}
	rem := out["reminder"].(map[string]any)
	if rem["tz"] != "Asia/Kolkata" || rem["version"].(float64) != 1 {
		t.Fatalf("reminder: %v", rem)
	}
	// 09:00 IST = 03:30Z; t0 is midnight UTC.
	advance(t, h, 4*time.Hour)
	got := waitStatus(t, h, "r1", 5*time.Second, "delivered")
	if len(got["attempts"].([]any)) != 1 {
		t.Fatalf("attempts: %v", got["attempts"])
	}
}

// AC5/AC6 over HTTP: edit bumps version and reschedules; cancel wins.
func TestEditAndCancelOverHTTP(t *testing.T) {
	h := newHarness(t)
	post(t, h, "/reminders", `{"id":"r2","content":"old","tz":"Asia/Kolkata","localTime":"2026-09-20T09:00"}`)
	code, out := patch(t, h, "/reminders/r2", `{"content":"new","localTime":"2026-09-20T10:00"}`)
	if code != 200 {
		t.Fatalf("edit: %d %v", code, out)
	}
	if v := out["reminder"].(map[string]any)["version"].(float64); v != 2 {
		t.Fatalf("expected version 2, got %v", out)
	}
	if c := out["reminder"].(map[string]any)["content"]; c != "new" {
		t.Fatalf("content: %v", c)
	}
	code, _ = post(t, h, "/reminders/r2/cancel", `{}`)
	if code != 200 {
		t.Fatalf("cancel: %d", code)
	}
	// Far future: cancelled must never deliver.
	advance(t, h, 48*time.Hour)
	time.Sleep(300 * time.Millisecond)
	_, out2 := get(t, h, "/reminders/r2")
	if s := out2["reminder"].(map[string]any)["status"]; s != "cancelled" {
		t.Fatalf("expected cancelled, got %v", s)
	}
	if h.fake.LogicalCount() != 0 {
		t.Fatalf("cancelled reminder must never notify, got %d", h.fake.LogicalCount())
	}
}

// Idempotent create: same id + identical intent returns the existing row
// (deduped:true); same id with different content is a 409 conflict.
func TestIdempotentCreate(t *testing.T) {
	h := newHarness(t)
	body := `{"id":"dup1","content":"same","tz":"Asia/Kolkata","localTime":"2026-09-20T09:00"}`
	code, _ := post(t, h, "/reminders", body)
	if code != 201 {
		t.Fatalf("first create: %d", code)
	}
	code, out := post(t, h, "/reminders", body)
	if code != 200 || out["deduped"] != true {
		t.Fatalf("identical re-create should be 200 deduped, got %d %v", code, out)
	}
	code, _ = post(t, h, "/reminders", `{"id":"dup1","content":"DIFFERENT","tz":"Asia/Kolkata","localTime":"2026-09-20T09:00"}`)
	if code != 409 {
		t.Fatalf("conflicting re-create should be 409, got %d", code)
	}
}

// expectedVersion: stale editors get 409, not silent last-writer-wins.
func TestEditConflictOverHTTP(t *testing.T) {
	h := newHarness(t)
	post(t, h, "/reminders", `{"id":"cc1","content":"a","tz":"Asia/Kolkata","localTime":"2026-09-20T09:00"}`)
	code, out := patch(t, h, "/reminders/cc1", `{"content":"b","expectedVersion":1}`)
	if code != 200 || out["reminder"].(map[string]any)["version"].(float64) != 2 {
		t.Fatalf("edit v1->v2: %d %v", code, out)
	}
	code, _ = patch(t, h, "/reminders/cc1", `{"content":"stale","expectedVersion":1}`)
	if code != 409 {
		t.Fatalf("stale edit should be 409, got %d", code)
	}
}

// Unknown status filter and negative time travel are 400s, not silent.
func TestFilterAndClockGuards(t *testing.T) {
	h := newHarness(t)
	if code, _ := get(t, h, "/reminders?status=bogus"); code != 400 {
		t.Errorf("unknown status: got %d want 400", code)
	}
	if code, _ := post(t, h, "/admin/clock", `{"advanceMs":-5}`); code != 400 {
		t.Errorf("negative advance: got %d want 400", code)
	}
	if code, _ := post(t, h, "/admin/clock", `{"now":"2020-01-01T00:00:00Z"}`); code != 400 {
		t.Errorf("backward now: got %d want 400", code)
	}
}
func TestGapNoteOverHTTP(t *testing.T) {
	h := newHarness(t)
	code, out := post(t, h, "/reminders",
		`{"id":"r3","content":"dst","tz":"America/New_York","localTime":"2026-03-08T02:30"}`)
	if code != 201 {
		t.Fatalf("create: %d %v", code, out)
	}
	if _, ok := out["tzNote"]; !ok {
		t.Fatalf("expected tzNote for gap time, got %v", out)
	}
}

// Validation: unknown zone, bad time, missing content, unknown id.
func TestValidation(t *testing.T) {
	h := newHarness(t)
	for _, tc := range []struct {
		name, body string
		want       int
	}{
		{"unknown zone", `{"id":"x","content":"c","tz":"Mars/X","localTime":"2026-09-20T09:00"}`, 400},
		{"bad wall", `{"id":"x","content":"c","tz":"Asia/Kolkata","localTime":"nope"}`, 400},
		{"no content", `{"id":"x","tz":"Asia/Kolkata","localTime":"2026-09-20T09:00"}`, 400},
		{"no time", `{"id":"x","content":"c","tz":"Asia/Kolkata"}`, 400},
		{"extra field", `{"id":"x","content":"c","tz":"Asia/Kolkata","localTime":"2026-09-20T09:00","zzz":1}`, 400},
	} {
		if code, _ := post(t, h, "/reminders", tc.body); code != tc.want {
			t.Errorf("%s: got %d want %d", tc.name, code, tc.want)
		}
	}
	if code, _ := get(t, h, "/reminders/nope"); code != 404 {
		t.Errorf("unknown id: got %d want 404", code)
	}
	if code, _ := patch(t, h, "/reminders/nope", `{"content":"x"}`); code != 404 {
		t.Errorf("edit unknown: got %d want 404", code)
	}
	if code, _ := post(t, h, "/reminders/nope/cancel", `{}`); code != 404 {
		t.Errorf("cancel unknown: got %d want 404", code)
	}
}

// Replay redrives failed rows; unknown ids 404, non-failed 400.
func TestReplayEndpoint(t *testing.T) {
	h := newHarness(t)
	h.fake.SetMode(notify.ModeAlwaysPerm, 0)
	post(t, h, "/reminders", `{"id":"rp1","content":"x","tz":"Asia/Kolkata","localTime":"2026-09-20T09:00"}`)
	advance(t, h, 12*time.Hour)
	waitStatus(t, h, "rp1", 5*time.Second, "failed")
	code, out := post(t, h, "/reminders/rp1/replay", `{}`)
	if code != 200 {
		t.Fatalf("replay: %d %v", code, out)
	}
	if v := out["reminder"].(map[string]any)["version"].(float64); v != 2 {
		t.Fatalf("replay must start a new version, got %v", out)
	}
	h.fake.SetMode(notify.ModeOK, 0)
	advance(t, h, time.Hour)
	waitStatus(t, h, "rp1", 5*time.Second, "delivered")
	if code, _ := post(t, h, "/reminders/nope/replay", `{}`); code != 404 {
		t.Errorf("replay unknown: got %d want 404", code)
	}
	if code, _ := post(t, h, "/reminders/rp1/replay", `{}`); code != 400 {
		t.Errorf("replay delivered: got %d want 400", code)
	}
}

// Metrics carry scheduler counters, dedupe count, queue depth, backlog age.
func TestMetricsAndFilter(t *testing.T) {
	h := newHarness(t)
	post(t, h, "/reminders", `{"id":"m1","content":"a","tz":"Asia/Kolkata","localTime":"2026-09-20T09:00"}`)
	post(t, h, "/reminders", `{"id":"m2","content":"b","tz":"Asia/Kolkata","localTime":"2026-09-20T09:00"}`)
	post(t, h, "/reminders/m2/cancel", `{}`)
	advance(t, h, 12*time.Hour)
	waitStatus(t, h, "m1", 5*time.Second, "delivered")

	_, list := get(t, h, "/reminders?status=cancelled")
	items := list["reminders"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["id"] != "m2" {
		t.Fatalf("status filter: %v", list)
	}
	_, m := get(t, h, "/metrics")
	if m["logicalDelivered"].(float64) != 1 {
		t.Fatalf("metrics: %v", m)
	}
	if m["queue"].(map[string]any)["delivered"] == nil {
		t.Fatalf("metrics missing queue depth: %v", m)
	}
}
