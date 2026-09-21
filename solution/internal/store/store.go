package store

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/sanskarpan/product-engineer-ps/solution/internal/clock"
	_ "modernc.org/sqlite"
)

// Reminder states. Terminal: delivered, cancelled, failed.
const (
	StatusScheduled = "scheduled" // waiting for fire_at
	StatusRunning   = "running"   // claimed by a worker, delivery in flight
	StatusRetrying  = "retrying"  // failed retryably, waiting for next run
	StatusDelivered = "delivered" // terminal success
	StatusCancelled = "cancelled" // terminal, user-cancelled
	StatusFailed    = "failed"    // terminal, retries exhausted or permanent failure
)

// Attempt outcomes.
const (
	OutcomeSuccess   = "success"
	OutcomeRetryable = "retryable"
	OutcomeUncertain = "uncertain" // may have landed; ack lost. Retries like retryable.
	OutcomePermanent = "permanent"
)

type Reminder struct {
	ID          string `json:"id"`
	Content     string `json:"content"`
	TZ          string `json:"tz"`
	LocalWall   string `json:"localWall"`
	FireAtMs    int64  `json:"fireAtMs"`
	FireAtUTC   string `json:"fireAtUTC"`
	Status      string `json:"status"`
	Version     int    `json:"version"`
	DeliveryKey string `json:"deliveryKey"`
	Attempts    int    `json:"attemptCount"`
	NextRunAt   *int64 `json:"nextRunAtMs,omitempty"`
	LastError   string `json:"lastError,omitempty"`
	CreatedAt   int64  `json:"createdAtMs"`
	UpdatedAt   int64  `json:"updatedAtMs"`
}

type Attempt struct {
	ID         int64  `json:"id"`
	ReminderID string `json:"reminderId"`
	Version    int    `json:"version"`
	AttemptNo  int    `json:"attemptNo"`
	StartedAt  int64  `json:"startedAtMs"`
	FinishedAt int64  `json:"finishedAtMs"`
	Outcome    string `json:"outcome"`
	Error      string `json:"error,omitempty"`
	LatencyMs  int64  `json:"latencyMs"`
}

// DeliveryKey identifies one logical occurrence: reminder id + version.
// Editing bumps the version, so a superseded schedule can never deliver
// under the old key; duplicate executions of the same version share the key
// and the destination dedupes on it.
func DeliveryKey(id string, version int) string {
	return fmt.Sprintf("%s:v%d", id, version)
}

type Store struct {
	db  *sql.DB
	clk clock.Clock
	// retain caps per-reminder attempt history (0 = unbounded). Set via
	// SetRetention; enforced atomically inside FinishAttempt.
	retain int
}

// SetRetention caps stored attempt history per reminder. Oldest rows are
// dropped inside the finishing transaction, so the cap holds exactly.
func (s *Store) SetRetention(keepPerReminder int) {
	if keepPerReminder < 0 {
		keepPerReminder = 0
	}
	s.retain = keepPerReminder
}

// Provider is the durability seam: scheduler and API program against it,
// so a Postgres (SKIP LOCKED) implementation can replace SQLite untouched.
type Provider interface {
	Create(r Reminder) error
	Get(id string) (Reminder, error)
	List(limit int) ([]Reminder, error)
	ListByStatus(limit int, status string) ([]Reminder, error)
	CountByStatus() (map[string]int64, error)
	Attempts(id string) ([]Attempt, error)
	// IsDelivered/MarkDelivered make the dedupe durable: unlike the
	// destination's in-memory map, this survives process restarts, so a
	// redelivery after a crash still notifies exactly once per key.
	IsDelivered(key string) (bool, error)
	MarkDelivered(key string) error
	// PruneAttempts caps per-reminder history for retention: keeps the
	// newest keep rows per reminder, drops the rest. Returns rows removed.
	PruneAttempts(keepPerReminder int) (int64, error)
	// OldestDue reports the oldest due instant among claimable rows
	// (for backlog-age metrics). ok=false when nothing is due.
	OldestDue(nowMs int64) (dueMs int64, ok bool, err error)
	ClaimDue(nowMs int64) (Reminder, bool, bool, error)
	RecordAttempt(a Attempt) error
	FinishAttempt(a Attempt, status string, attempts int, nextRunAt *int64, lastErr string) (bool, error)
	// FinishDelivery applies an outcome only if the row is still running at
	// the same version. ok=false means stale (edited/cancelled meanwhile):
	// the caller must discard the result, never overwrite the new version.
	FinishDelivery(id string, version int, status string, attempts int, nextRunAt *int64, lastErr string) (ok bool, err error)
	// Edit bumps the version and reschedules. Only non-terminal rows move;
	// a concurrent running claim becomes stale via the version check above.
	Edit(id string, content, tz, wall string, fireAtMs int64, expectedVersion int) (Reminder, error)
	// Cancel marks cancelled where still active. Returns false when the row
	// was already terminal (nothing to cancel).
	Cancel(id string) (bool, error)
	// Replay redrives a failed row as a new occurrence: version+1, fresh
	// retry budget (explicit operator action — the one sanctioned exception
	// to budget-carry), rescheduled now. Only failed rows move.
	Replay(id string) (Reminder, error)
	Close() error
}

var _ Provider = (*Store)(nil)

func (s *Store) nowMs() int64 { return s.clk.NowMs() }

func Open(path string, clk clock.Clock) (*Store, error) {
	if clk == nil {
		clk = clock.SystemClock{}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // SQLite single-writer; deterministic.
	s := &Store{db: db, clk: clk}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	if err := s.recoverInFlight(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	for _, q := range []string{
		`PRAGMA journal_mode=WAL;`,
		`PRAGMA busy_timeout=5000;`,
		`CREATE TABLE IF NOT EXISTS reminders(
			id TEXT PRIMARY KEY,
			content TEXT NOT NULL,
			tz TEXT NOT NULL,
			local_wall TEXT NOT NULL,
			fire_at_ms INTEGER NOT NULL,
			status TEXT NOT NULL,
			version INTEGER NOT NULL DEFAULT 1,
			delivery_key TEXT NOT NULL,
			attempt_count INTEGER NOT NULL DEFAULT 0,
			next_run_at INTEGER,
			last_error TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS attempts(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			reminder_id TEXT NOT NULL REFERENCES reminders(id),
			version INTEGER NOT NULL,
			attempt_no INTEGER NOT NULL,
			started_at INTEGER NOT NULL,
			finished_at INTEGER NOT NULL,
			outcome TEXT NOT NULL,
			error TEXT NOT NULL DEFAULT '',
			latency_ms INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE INDEX IF NOT EXISTS idx_rem_due ON reminders(status, next_run_at);`,
		`CREATE INDEX IF NOT EXISTS idx_rem_fire ON reminders(status, fire_at_ms);`,
		`CREATE INDEX IF NOT EXISTS idx_att_rem ON attempts(reminder_id, id);`,
		`CREATE TABLE IF NOT EXISTS deliveries(
			delivery_key TEXT PRIMARY KEY,
			delivered_at INTEGER NOT NULL
		);`,
	} {
		if _, err := s.db.Exec(q); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

// recoverInFlight reschedules rows left 'running' by a crashed process.
// Overdue policy: fire ASAP in due order — a promise made must be kept,
// late is better than never, and lateness is visible via fire_at_ms.
// next_run_at is deliberately left untouched: a NULL value lets the already
// overdue fire_at_ms drive immediate pickup on any clock, while a set value
// preserves the retry's backoff. (Seeding wall-clock time here once wedged
// manual-clock recoveries behind real time; the clock-agnostic form cannot.)
func (s *Store) recoverInFlight() error {
	_, err := s.db.Exec(
		`UPDATE reminders SET status=CASE WHEN attempt_count=0 THEN 'scheduled' ELSE 'retrying' END,
		 updated_at=? WHERE status='running'`,
		s.nowMs(),
	)
	return err
}

func (s *Store) IsDelivered(key string) (bool, error) {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM deliveries WHERE delivery_key=?`, key).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *Store) MarkDelivered(key string) error {
	_, err := s.db.Exec(`INSERT OR IGNORE INTO deliveries(delivery_key, delivered_at) VALUES(?, ?)`,
		key, s.nowMs())
	return err
}

func (s *Store) PruneAttempts(keepPerReminder int) (int64, error) {
	if keepPerReminder < 1 {
		keepPerReminder = 1
	}
	res, err := s.db.Exec(
		`DELETE FROM attempts WHERE id NOT IN (
			SELECT id FROM attempts AS a2 WHERE a2.reminder_id = attempts.reminder_id
			ORDER BY id DESC LIMIT ?)`, keepPerReminder,
	)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (s *Store) OldestDue(nowMs int64) (int64, bool, error) {
	var due sql.NullInt64
	err := s.db.QueryRow(
		`SELECT MIN(COALESCE(next_run_at, fire_at_ms)) FROM reminders
		 WHERE status IN ('scheduled','retrying')
		   AND COALESCE(next_run_at, fire_at_ms) <= ?`, nowMs,
	).Scan(&due)
	if err != nil {
		return 0, false, err
	}
	if !due.Valid {
		return 0, false, nil
	}
	return due.Int64, true, nil
}

func scanReminder(row interface {
	Scan(...any) error
}) (Reminder, error) {
	var r Reminder
	var next sql.NullInt64
	err := row.Scan(&r.ID, &r.Content, &r.TZ, &r.LocalWall, &r.FireAtMs,
		&r.Status, &r.Version, &r.DeliveryKey, &r.Attempts, &next,
		&r.LastError, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		return Reminder{}, err
	}
	if next.Valid {
		v := next.Int64
		r.NextRunAt = &v
	}
	r.FireAtUTC = time.UnixMilli(r.FireAtMs).UTC().Format(time.RFC3339)
	return r, nil
}

func (s *Store) Create(r Reminder) error {
	now := s.nowMs()
	if r.Version < 1 {
		r.Version = 1
	}
	r.DeliveryKey = DeliveryKey(r.ID, r.Version)
	_, err := s.db.Exec(
		`INSERT INTO reminders(id,content,tz,local_wall,fire_at_ms,status,version,delivery_key,attempt_count,next_run_at,last_error,created_at,updated_at)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		r.ID, r.Content, r.TZ, r.LocalWall, r.FireAtMs, StatusScheduled,
		r.Version, r.DeliveryKey, 0, nil, "", now, now,
	)
	return err
}

func (s *Store) Get(id string) (Reminder, error) {
	return scanReminder(s.db.QueryRow(
		`SELECT id,content,tz,local_wall,fire_at_ms,status,version,delivery_key,attempt_count,next_run_at,last_error,created_at,updated_at
		 FROM reminders WHERE id=?`, id))
}

func (s *Store) listWhere(limit int, where string, args ...any) ([]Reminder, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.Query(
		`SELECT id,content,tz,local_wall,fire_at_ms,status,version,delivery_key,attempt_count,next_run_at,last_error,created_at,updated_at
		 FROM reminders `+where+` ORDER BY fire_at_ms ASC LIMIT ?`, append(args, limit)...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Reminder
	for rows.Next() {
		r, err := scanReminder(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) List(limit int) ([]Reminder, error) {
	return s.listWhere(limit, "")
}

func (s *Store) ListByStatus(limit int, status string) ([]Reminder, error) {
	return s.listWhere(limit, `WHERE status=?`, status)
}

func (s *Store) CountByStatus() (map[string]int64, error) {
	rows, err := s.db.Query(`SELECT status, COUNT(*) FROM reminders GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}

func (s *Store) Attempts(id string) ([]Attempt, error) {
	rows, err := s.db.Query(
		`SELECT id,reminder_id,version,attempt_no,started_at,finished_at,outcome,error,latency_ms
		 FROM attempts WHERE reminder_id=? ORDER BY id ASC`, id,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Attempt
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.ID, &a.ReminderID, &a.Version, &a.AttemptNo,
			&a.StartedAt, &a.FinishedAt, &a.Outcome, &a.Error, &a.LatencyMs); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ClaimDue moves one due reminder to 'running' and reports it with the
// version and due instant observed. The claim UPDATE re-checks both, so an
// edit that rescheduled the row (or bumped the version) between SELECT and
// UPDATE loses the race instead of delivering the new version early.
func (s *Store) ClaimDue(nowMs int64) (r Reminder, claimed, contended bool, err error) {
	var id string
	var version int
	var due int64
	err = s.db.QueryRow(
		`SELECT id, version, COALESCE(next_run_at, fire_at_ms) FROM reminders
		 WHERE status IN ('scheduled','retrying')
		   AND COALESCE(next_run_at, fire_at_ms) <= ?
		 ORDER BY COALESCE(next_run_at, fire_at_ms) ASC LIMIT 1`, nowMs,
	).Scan(&id, &version, &due)
	if err == sql.ErrNoRows {
		return Reminder{}, false, false, nil
	}
	if err != nil {
		return Reminder{}, false, false, err
	}
	res, err := s.db.Exec(
		`UPDATE reminders SET status='running', updated_at=?
		 WHERE id=? AND version=? AND status IN ('scheduled','retrying')
		   AND COALESCE(next_run_at, fire_at_ms) <= ?`,
		nowMs, id, version, nowMs,
	)
	if err != nil {
		return Reminder{}, false, false, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return Reminder{}, false, true, nil // lost race: worker, edit, or cancel moved it
	}
	r, err = s.Get(id)
	if err != nil {
		return Reminder{}, false, false, err
	}
	return r, true, false, nil
}

func (s *Store) RecordAttempt(a Attempt) error {
	_, err := s.db.Exec(
		`INSERT INTO attempts(reminder_id,version,attempt_no,started_at,finished_at,outcome,error,latency_ms)
		 VALUES(?,?,?,?,?,?,?,?)`,
		a.ReminderID, a.Version, a.AttemptNo, a.StartedAt, a.FinishedAt, a.Outcome, a.Error, a.LatencyMs,
	)
	return err
}

var validFinish = map[string]bool{
	StatusDelivered: true, StatusRetrying: true, StatusFailed: true,
}

// FinishAttempt records the attempt and applies its outcome atomically:
// either both persist or neither does, so attempt history and
// attempt_count can never silently diverge across a crash.
func (s *Store) FinishAttempt(a Attempt, status string, attempts int, nextRunAt *int64, lastErr string) (applied bool, err error) {
	if !validFinish[status] {
		return false, fmt.Errorf("invalid transition running->%s", status)
	}
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(
		`INSERT INTO attempts(reminder_id,version,attempt_no,started_at,finished_at,outcome,error,latency_ms)
		 VALUES(?,?,?,?,?,?,?,?)`,
		a.ReminderID, a.Version, a.AttemptNo, a.StartedAt, a.FinishedAt, a.Outcome, a.Error, a.LatencyMs,
	); err != nil {
		return false, err
	}
	res, err := tx.Exec(
		`UPDATE reminders SET status=?, attempt_count=?, next_run_at=?, last_error=?, updated_at=?
		 WHERE id=? AND version=? AND status='running'`,
		status, attempts, nextRunAt, lastErr, s.nowMs(), a.ReminderID, a.Version,
	)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n == 1 && s.retain > 0 {
		// Retention inside the same transaction: history can never exceed
		// the cap, even across a crash between insert and prune.
		if _, err := tx.Exec(
			`DELETE FROM attempts WHERE reminder_id=? AND id NOT IN (
				SELECT id FROM attempts AS a2 WHERE a2.reminder_id=? ORDER BY id DESC LIMIT ?)`,
			a.ReminderID, a.ReminderID, s.retain,
		); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return n == 1, nil // false = stale: edited or cancelled meanwhile, discard
}

func (s *Store) FinishDelivery(id string, version int, status string, attempts int, nextRunAt *int64, lastErr string) (bool, error) {
	if !validFinish[status] {
		return false, fmt.Errorf("invalid transition running->%s", status)
	}
	res, err := s.db.Exec(
		`UPDATE reminders SET status=?, attempt_count=?, next_run_at=?, last_error=?, updated_at=?
		 WHERE id=? AND version=? AND status='running'`,
		status, attempts, nextRunAt, lastErr, s.nowMs(), id, version,
	)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil // false = stale: edited or cancelled meanwhile, discard
}

// Edit bumps the version and reschedules. Only non-terminal rows move; a
// concurrent running claim becomes stale via the version check. The retry
// budget (attempt_count) carries across versions so edits cannot mint
// unbounded retries; history rows keep their version for audit. A no-op
// edit (nothing actually changed) returns the row untouched. Pass
// expectedVersion >= 0 to guard concurrent editors: a mismatch reports a
// version conflict instead of silently winning last-writer-wins.
func (s *Store) Edit(id string, content, tz, wall string, fireAtMs int64, expectedVersion int) (Reminder, error) {
	cur, err := s.Get(id)
	if err != nil {
		return Reminder{}, err
	}
	switch cur.Status {
	case StatusScheduled, StatusRetrying, StatusRunning:
	default:
		return Reminder{}, fmt.Errorf("not editable in status %s", cur.Status)
	}
	if expectedVersion >= 0 && cur.Version != expectedVersion {
		return Reminder{}, fmt.Errorf("version conflict: expected v%d, have v%d", expectedVersion, cur.Version)
	}
	if cur.Content == content && cur.TZ == tz && cur.LocalWall == wall && cur.FireAtMs == fireAtMs {
		return cur, nil // no-op: no new version, no reschedule
	}
	res, err := s.db.Exec(
		`UPDATE reminders SET content=?, tz=?, local_wall=?, fire_at_ms=?,
		 version=version+1, status='scheduled', next_run_at=NULL, last_error='',
		 delivery_key=id || ':v' || (version+1), updated_at=?
		 WHERE id=? AND version=? AND status IN ('scheduled','retrying','running')`,
		content, tz, wall, fireAtMs, s.nowMs(), id, cur.Version,
	)
	if err != nil {
		return Reminder{}, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return Reminder{}, fmt.Errorf("concurrent modification of %s (retry the edit)", id)
	}
	return s.Get(id)
}

func (s *Store) Cancel(id string) (bool, error) {
	res, err := s.db.Exec(
		`UPDATE reminders SET status='cancelled', next_run_at=NULL, updated_at=?
		 WHERE id=? AND status IN ('scheduled','retrying','running')`,
		s.nowMs(), id,
	)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

func (s *Store) Replay(id string) (Reminder, error) {
	res, err := s.db.Exec(
		`UPDATE reminders SET version=version+1, status='scheduled', attempt_count=0,
		 next_run_at=NULL, last_error='', delivery_key=id || ':v' || (version+1), updated_at=?
		 WHERE id=? AND status='failed'`,
		s.nowMs(), id,
	)
	if err != nil {
		return Reminder{}, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		cur, gerr := s.Get(id)
		if gerr != nil {
			return Reminder{}, gerr
		}
		return Reminder{}, fmt.Errorf("only failed rows can be replayed (status=%s)", cur.Status)
	}
	return s.Get(id)
}
