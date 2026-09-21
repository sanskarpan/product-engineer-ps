package api

import (
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sanskarpan/product-engineer-ps/solution/internal/clock"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/notify"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/sched"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/store"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/tz"
)

// logicalCounter exposes the destination's dedupe count for /metrics.
// The fake destination satisfies it; a real provider would bring its own
// counter (or move the count into the scheduler snapshot).
type logicalCounter interface{ LogicalCount() int }

// Server owns HTTP ingestion + inspection. Scheduling lives in
// sched.Scheduler, durability in store.Provider, time in clock.Clock,
// delivery in notify.Notifier — all seams, no concrete dependencies.
type Server struct {
	store      store.Provider
	scheduler  *sched.Scheduler
	notifier   notify.Notifier
	deliveries logicalCounter
	clock      clock.Clock
	manual     *clock.ManualClock // non-nil when CLOCK_MODE=manual (admin time travel)
	mux        *http.ServeMux
}

func New(s store.Provider, sch *sched.Scheduler, n notify.Notifier, c logicalCounter, clk clock.Clock, m *clock.ManualClock) *Server {
	srv := &Server{store: s, scheduler: sch, notifier: n, deliveries: c, clock: clk, manual: m, mux: http.NewServeMux()}
	srv.routes()
	return srv
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("POST /reminders", s.handleCreate)
	s.mux.HandleFunc("GET /reminders", s.handleList)
	s.mux.HandleFunc("GET /reminders/{id}", s.handleGet)
	s.mux.HandleFunc("PATCH /reminders/{id}", s.handleEdit)
	s.mux.HandleFunc("POST /reminders/{id}/cancel", s.handleCancel)
	s.mux.HandleFunc("POST /reminders/{id}/replay", s.handleReplay)
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /admin/clock", s.handleClockGet)
	s.mux.HandleFunc("POST /admin/clock", s.handleClockSet)
	s.mux.HandleFunc("POST /admin/notify", s.handleNotifyMode)
	s.mux.HandleFunc("GET /", s.handleIndex)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

type createRequest struct {
	ID        string `json:"id"`
	Content   string `json:"content"`
	TZ        string `json:"tz"`
	LocalTime string `json:"localTime"`
	FireAt    string `json:"fireAt"`
}

func newID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return "rem_" + time.Now().UTC().Format("20060102T150405.000000") +
		fmt.Sprintf("-%08x", b)
}

// resolveFire turns either input form into (fireMs, wall, tzNote).
// Shared by create and edit so both forms behave identically.
func resolveFire(fireAt, localTime, tzName string) (fireMs int64, wall, note string, err error) {
	switch {
	case strings.TrimSpace(fireAt) != "":
		t, err := time.Parse(time.RFC3339, strings.TrimSpace(fireAt))
		if err != nil {
			return 0, "", "", fmt.Errorf("fireAt must be RFC3339")
		}
		loc, _ := time.LoadLocation(tzName) // validated by caller
		return t.UnixMilli(), t.In(loc).Format(tz.WallLayout), "", nil
	case strings.TrimSpace(localTime) != "":
		wall = strings.TrimSpace(localTime)
		t, n, err := tz.Resolve(wall, tzName)
		if err != nil {
			return 0, "", "", err
		}
		return t.UnixMilli(), wall, n, nil
	default:
		return 0, "", "", fmt.Errorf("one of fireAt or localTime is required")
	}
}

// POST /reminders — create scheduled work. Accepts either a zoned instant
// (fireAt RFC3339 + tz for retention) or a wall time (localTime + tz,
// converted via the documented DST policy). tz is always retained.
func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Content = strings.TrimSpace(req.Content)
	req.TZ = strings.TrimSpace(req.TZ)
	if req.Content == "" || req.TZ == "" {
		writeJSON(w, 400, map[string]string{"error": "content and tz are required"})
		return
	}
	if _, err := time.LoadLocation(req.TZ); err != nil {
		writeJSON(w, 400, map[string]string{"error": "unknown time zone " + req.TZ})
		return
	}
	fireMs, wall, note, err := resolveFire(req.FireAt, req.LocalTime, req.TZ)
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	id := strings.TrimSpace(req.ID)
	if id == "" {
		id = newID()
	}
	rec := store.Reminder{ID: id, Content: req.Content, TZ: req.TZ, LocalWall: wall, FireAtMs: fireMs}
	if cerr := s.store.Create(rec); cerr != nil {
		// Idempotent create: the same id with byte-identical scheduling
		// intent returns the existing row (client crashed after persisting
		// but before seeing the response). A conflicting id is a 409;
		// anything else is a 500, never misreported as conflict.
		if !isUniqueViolation(cerr) {
			writeJSON(w, 500, map[string]string{"error": "create: " + cerr.Error()})
			return
		}
		existing, gerr := s.store.Get(id)
		if gerr != nil {
			writeJSON(w, 500, map[string]string{"error": gerr.Error()})
			return
		}
		if existing.Content == rec.Content && existing.TZ == rec.TZ &&
			existing.LocalWall == rec.LocalWall && existing.FireAtMs == rec.FireAtMs {
			attempts, _ := s.store.Attempts(id)
			if attempts == nil {
				attempts = []store.Attempt{}
			}
			writeJSON(w, 200, map[string]any{"reminder": existing, "attempts": attempts, "deduped": true})
			return
		}
		writeJSON(w, 409, map[string]string{"error": "id " + id + " already exists with different content or schedule"})
		return
	}
	s.scheduler.Kick()
	got, _ := s.store.Get(id)
	attempts, _ := s.store.Attempts(id)
	if attempts == nil {
		attempts = []store.Attempt{}
	}
	resp := map[string]any{"reminder": got, "attempts": attempts}
	if note != "" {
		resp["tzNote"] = note
	}
	writeJSON(w, 201, resp)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	want := r.URL.Query().Get("status")
	if want != "" {
		switch want {
		case store.StatusScheduled, store.StatusRunning, store.StatusRetrying,
			store.StatusDelivered, store.StatusCancelled, store.StatusFailed:
		default:
			writeJSON(w, 400, map[string]string{"error": "unknown status " + want})
			return
		}
	}
	var (
		items []store.Reminder
		err   error
	)
	if want != "" {
		items, err = s.store.ListByStatus(100, want)
	} else {
		items, err = s.store.List(100)
	}
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	if items == nil {
		items = []store.Reminder{}
	}
	writeJSON(w, 200, map[string]any{"reminders": items})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	got, err := s.store.Get(id)
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, 404, map[string]string{"error": "not found"})
		return
	}
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	attempts, err := s.store.Attempts(id)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	if attempts == nil {
		attempts = []store.Attempt{}
	}
	writeJSON(w, 200, map[string]any{"reminder": got, "attempts": attempts})
}

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

type editRequest struct {
	Content         string `json:"content"`
	TZ              string `json:"tz"`
	LocalTime       string `json:"localTime"`
	FireAt          string `json:"fireAt"`
	ExpectedVersion *int   `json:"expectedVersion"`
}

// PATCH /reminders/{id} — edit time/content before delivery. Bumps the
// version and reschedules; an in-flight claim on the old version becomes
// stale and its result is discarded (the race policy).
func (s *Server) handleEdit(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	cur, err := s.store.Get(id)
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, 404, map[string]string{"error": "not found"})
		return
	}
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	var req editRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	content := cur.Content
	if strings.TrimSpace(req.Content) != "" {
		content = strings.TrimSpace(req.Content)
	}
	tzName := cur.TZ
	if strings.TrimSpace(req.TZ) != "" {
		tzName = strings.TrimSpace(req.TZ)
		if _, err := time.LoadLocation(tzName); err != nil {
			writeJSON(w, 400, map[string]string{"error": "unknown time zone " + tzName})
			return
		}
	}
	fireMs := cur.FireAtMs
	wall := cur.LocalWall
	var note string
	if strings.TrimSpace(req.FireAt) != "" || strings.TrimSpace(req.LocalTime) != "" {
		var rerr error
		fireMs, wall, note, rerr = resolveFire(req.FireAt, req.LocalTime, tzName)
		if rerr != nil {
			writeJSON(w, 400, map[string]string{"error": rerr.Error()})
			return
		}
	} else if strings.TrimSpace(req.TZ) != "" {
		// Zone-only edit: re-render the same instant in the new zone.
		loc, _ := time.LoadLocation(tzName)
		wall = time.UnixMilli(cur.FireAtMs).In(loc).Format(tz.WallLayout)
	}
	expected := -1
	if req.ExpectedVersion != nil {
		expected = *req.ExpectedVersion
	}
	updated, err := s.store.Edit(id, content, tzName, wall, fireMs, expected)
	if err != nil {
		code := 400
		if strings.Contains(err.Error(), "version conflict") || strings.Contains(err.Error(), "concurrent modification") {
			code = 409
		}
		writeJSON(w, code, map[string]string{"error": err.Error()})
		return
	}
	s.scheduler.Kick()
	resp := map[string]any{"reminder": updated}
	if note != "" {
		resp["tzNote"] = note
	}
	writeJSON(w, 200, resp)
}

// POST /reminders/{id}/cancel — cancellation wins: in-flight or future
// deliveries for the id are discarded, never recorded as delivered.
func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.Get(id); errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, 404, map[string]string{"error": "not found"})
		return
	} else if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	ok, err := s.store.Cancel(id)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	if !ok {
		writeJSON(w, 400, map[string]string{"error": "already terminal, nothing to cancel"})
		return
	}
	got, _ := s.store.Get(id)
	writeJSON(w, 200, map[string]any{"reminder": got})
}

// POST /reminders/{id}/replay — DLQ redrive: a failed row starts a new
// occurrence (version+1, fresh budget — explicit operator action is the one
// sanctioned exception to budget-carry). Only failed rows move.
func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.Get(id); errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, 404, map[string]string{"error": "not found"})
		return
	} else if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	replayed, err := s.store.Replay(id)
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	s.scheduler.Kick()
	writeJSON(w, 200, map[string]any{"reminder": replayed})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]any{"ok": true,
		"time":    s.clock.Now().UTC().Format(time.RFC3339),
		"breaker": s.scheduler.Snapshot().BreakerState})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	queue, err := s.store.CountByStatus()
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	snap := s.scheduler.Snapshot()
	// Backlog age: how overdue the oldest waiting work is. Null when idle.
	var oldestDueMs *int64
	if due, ok, err := s.store.OldestDue(s.clock.NowMs()); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	} else if ok {
		v := due
		oldestDueMs = &v
	}
	writeJSON(w, 200, map[string]any{"scheduler": snap,
		"logicalDelivered": s.deliveries.LogicalCount(), "queue": queue,
		"oldestDueMs": oldestDueMs})
}

func (s *Server) handleClockGet(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]any{
		"now":    s.clock.Now().UTC().Format(time.RFC3339),
		"nowMs":  s.clock.NowMs(),
		"manual": s.manual != nil,
	})
}

func (s *Server) handleClockSet(w http.ResponseWriter, r *http.Request) {
	if s.manual == nil {
		writeJSON(w, 400, map[string]string{"error": "CLOCK_MODE is not manual"})
		return
	}
	var req struct {
		Now       string `json:"now"`
		AdvanceMs *int64 `json:"advanceMs"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	switch {
	case req.Now != "":
		t, err := time.Parse(time.RFC3339, req.Now)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": "now must be RFC3339"})
			return
		}
		// Forward-only like advanceMs: silently rewinding the clock would
		// park retries and void backoff guarantees.
		if t.UnixMilli() < s.clock.NowMs() {
			writeJSON(w, 400, map[string]string{"error": "manual clock is forward-only"})
			return
		}
		s.manual.Set(t)
	case req.AdvanceMs != nil:
		if *req.AdvanceMs < 0 {
			writeJSON(w, 400, map[string]string{"error": "advanceMs must be >= 0 (time travel is forward-only)"})
			return
		}
		s.manual.Advance(time.Duration(*req.AdvanceMs) * time.Millisecond)
	default:
		writeJSON(w, 400, map[string]string{"error": "one of now or advanceMs is required"})
		return
	}
	s.scheduler.Kick()
	writeJSON(w, 200, map[string]any{"now": s.clock.Now().UTC().Format(time.RFC3339)})
}

func (s *Server) handleNotifyMode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mode      string `json:"mode"`
		FailFirst int    `json:"failFirst"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	switch req.Mode {
	case notify.ModeOK, notify.ModeFailFirst, notify.ModeAlwaysTemp,
		notify.ModeAlwaysPerm, notify.ModeLostAckFirst:
		setter, ok := s.notifier.(interface{ SetMode(string, int) })
		if !ok {
			writeJSON(w, 400, map[string]string{"error": "destination mode is not controllable"})
			return
		}
		setter.SetMode(req.Mode, req.FailFirst)
		writeJSON(w, 200, map[string]any{"mode": req.Mode})
	default:
		writeJSON(w, 400, map[string]string{"error": "unknown mode"})
	}
}
