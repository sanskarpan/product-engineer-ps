package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/sanskarpan/product-engineer-ps/solution/internal/clock"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/notify"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/sched"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/store"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/tz"
)

// Server owns HTTP ingestion + inspection. Scheduling lives in
// sched.Scheduler, durability in store.Provider, time in clock.Clock.
type Server struct {
	store     store.Provider
	scheduler *sched.Scheduler
	notifier  *notify.Fake
	clock     clock.Clock
	manual    *clock.ManualClock // non-nil when CLOCK_MODE=manual (admin time travel)
	mux       *http.ServeMux
}

func New(s store.Provider, sch *sched.Scheduler, n *notify.Fake, c clock.Clock, m *clock.ManualClock) *Server {
	srv := &Server{store: s, scheduler: sch, notifier: n, clock: c, manual: m, mux: http.NewServeMux()}
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
	return "rem_" + time.Now().UTC().Format("20060102T150405.000000000")
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
	var fire time.Time
	var wall, note string
	switch {
	case strings.TrimSpace(req.FireAt) != "":
		t, err := time.Parse(time.RFC3339, strings.TrimSpace(req.FireAt))
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": "fireAt must be RFC3339"})
			return
		}
		fire = t
		loc, _ := time.LoadLocation(req.TZ)
		wall = t.In(loc).Format(tz.WallLayout)
	case strings.TrimSpace(req.LocalTime) != "":
		wall = strings.TrimSpace(req.LocalTime)
		t, n, err := tz.Resolve(wall, req.TZ)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		fire = t
		note = n
	default:
		writeJSON(w, 400, map[string]string{"error": "one of fireAt or localTime is required"})
		return
	}
	id := strings.TrimSpace(req.ID)
	if id == "" {
		id = newID()
	}
	rec := store.Reminder{ID: id, Content: req.Content, TZ: req.TZ, LocalWall: wall, FireAtMs: fire.UnixMilli()}
	if err := s.store.Create(rec); err != nil {
		writeJSON(w, 409, map[string]string{"error": "create: " + err.Error()})
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
	var (
		items []store.Reminder
		err   error
	)
	if want := r.URL.Query().Get("status"); want != "" {
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

type editRequest struct {
	Content   string `json:"content"`
	TZ        string `json:"tz"`
	LocalTime string `json:"localTime"`
	FireAt    string `json:"fireAt"`
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
	if strings.TrimSpace(req.FireAt) != "" {
		t, err := time.Parse(time.RFC3339, strings.TrimSpace(req.FireAt))
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": "fireAt must be RFC3339"})
			return
		}
		fireMs = t.UnixMilli()
		loc, _ := time.LoadLocation(tzName)
		wall = t.In(loc).Format(tz.WallLayout)
	} else if strings.TrimSpace(req.LocalTime) != "" {
		wall = strings.TrimSpace(req.LocalTime)
		t, n, err := tz.Resolve(wall, tzName)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		fireMs = t.UnixMilli()
		note = n
	}
	updated, err := s.store.Edit(id, content, tzName, wall, fireMs)
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
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

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]any{"ok": true,
		"time": s.clock.Now().UTC().Format(time.RFC3339)})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	queue, err := s.store.CountByStatus()
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"scheduler": s.scheduler.Snapshot(),
		"logicalDelivered": s.notifier.LogicalCount(), "queue": queue})
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
		s.manual.Set(t)
	case req.AdvanceMs != nil:
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	switch req.Mode {
	case notify.ModeOK, notify.ModeFailFirst, notify.ModeAlwaysTemp,
		notify.ModeAlwaysPerm, notify.ModeLostAckFirst:
		s.notifier.SetMode(req.Mode, req.FailFirst)
		writeJSON(w, 200, map[string]any{"mode": req.Mode})
	default:
		writeJSON(w, 400, map[string]string{"error": "unknown mode"})
	}
}
