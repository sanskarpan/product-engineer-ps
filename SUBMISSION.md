# Product Engineering Challenge Submission

## Candidate

- **Name:** Sanskar Pandey
- **Email:** (add before submitting)
- **GitHub:** https://github.com/sanskarpan
- **Selected problem:** Problem 3 — Durable Reminders and Follow-Ups
- **Demo video:** TODO — record 3–5 min and paste link here

## Run the project

Prerequisites: Go 1.24+ only. No Docker, no CGO, no paid services.

```text
cd solution
CLOCK_MODE=manual CLOCK_START=2026-09-20T00:00:00Z PORT=8080 BASE_DELAY_MS=200 go run ./cmd/server
# Open http://localhost:8080/ (live list + clock display)
```

Successful scenario: `POST /reminders` with a Kolkata 09:00 wall time →
advance the clock 4h via `POST /admin/clock {"advanceMs":14400000}` →
`GET /reminders/r1` shows `delivered` with 1 attempt (AC1).

Failure/recovery scenario: set notify `fail-first` (2 temp failures),
submit another reminder, step the clock in 60s increments → `retrying` →
`delivered` with `[retryable, retryable, success]` (AC3). `kill -9` the
server, restart with a `CLOCK_START` past a pending item's fire time →
overdue item discovered and delivered on boot (AC2). Full curl transcript
in `solution/README.md`.

## Run the tests

```text
cd solution
go test ./... -count=1
```

20 tests, race-clean: due-work discovery on clock advance, restart recovery,
temp-failure→retry→success, exhaustion bound, permanent rejection,
duplicate execution (exactly-once), lost-ack reconcile, edit versioning +
stale-finish discard, cancel-wins race, terminal edit rejection, crash
recovery, two zones + DST gap + overlap, HTTP validation, metrics/filter.

## Acceptance scenarios and verification

Completed: AC1–AC7 (all). AC4 duplicate execution: same `deliveryKey`
re-sent → destination acknowledges without a second logical notification
(unit + benchmark + live-verified). AC5/AC6: version-gated finish — stale
results discarded, counted in `metrics.staleDiscarded`. AC7: gap pushed +1h
with `tzNote`; overlap takes first occurrence with `tzNote`; both covered
for Asia/Kolkata (no DST) and America/New_York (both transitions).

```text
cd solution
go run ./cmd/benchmark
```

Observed result (`BENCHMARK PASS`): 20 items across two zones → delivered
16 / failed 2 (permanently rejected) / cancelled 2 / logicalDelivered 16,
zero unsettled, duplicate re-execution added 0 logical notifications,
cancelled items have 0 attempts. Mid-run stop/restart included.

Failure/recovery in video: temp-failure→retry→delivered via controlled
clock steps, plus kill -9 restart recovery. Reproduce per `solution/README.md`.

## Architecture and data flow

```
POST /reminders ──► tz.Resolve (wall+zone → UTC + note) ──► store.Create (persisted first)
PATCH /reminders/{id} ──► version++, reschedule (old claim goes stale)
POST /reminders/{id}/cancel ──► cancelled (in-flight finish discarded)
scheduler loop (N workers, 100ms tick + kick, injectable clock):
  ClaimDue (scheduled/retrying + due → running, CAS) ──► notify.Send(deliveryKey)
    ├── ok → delivered (terminal)
    ├── permanent → failed (terminal, 1 attempt)
    └── temporary → retrying (next_run = now + exp backoff+jitter) or failed at MAX_ATTEMPTS
GET /reminders[?status=], GET /reminders/{id} (+attempts), GET /metrics, POST /admin/clock|notify
```

Components: `internal/clock` (system + manual), `internal/tz` (IANA resolve
+ DST policy), `internal/store` (SQLite WAL behind `Provider`; version-gated
finish; `recoverInFlight`), `internal/notify` (fake destination, key-deduped;
`Notifier` seam), `internal/sched` (discovery + delivery + retries),
`internal/api` (ingest/inspect/admin), `cmd/server`, `cmd/benchmark`.
Only the scheduler mutates delivery state; only the API ingests/edits.

## Technology choices

Go + stdlib `net/http` + pure-Go SQLite. Chose Go because my strongest
backend evidence is Go (rate-limiter/circuit-breaker, reverse-proxy/LB,
Raft, websocket-chat with Redis pub/sub) and goroutine poll loops map
cleanly to scheduler workers. SQLite WAL gives durable schedule + restart
recovery with zero services — two commands to run. Alternative: Postgres +
a workflow engine (faithful at scale, breaks 10-minute setup); in-memory
timers only (fails the restart requirement outright). Trade-off: single-
process SQLite caps write throughput and multi-instance operation —
first production change below.

## Important decisions

1. **Version-gated finish instead of locks.** Edit bumps `version` and
   reschedules; `FinishDelivery` applies only for the claimed version on a
   still-`running` row. A late result after edit/cancel is discarded and
   counted, never overwrites. This answers the follow-up (reschedule racing
   a claim) in code and keeps workers lock-free.
2. **Delivery key = `id:v<version>`, deduped at the destination.** Scheduler
   firing twice is normal; the fake destination records the first logical
   notification per key and acknowledges repeats silently. Lost-ack retries
   are safe by construction; receivers must still dedupe on the key.
3. **Manual clock as a first-class citizen, not a test hack.** The server
   itself runs on the injectable clock (`CLOCK_MODE=manual` + admin time
   travel), so the reviewer replays time exactly as tests and the benchmark
   do — no waiting real minutes, no flaky sleeps.
4. **Explicit DST policy with notes, not silent guesses.** Gap → +1h same
   morning; overlap → first occurrence; both return a `tzNote` shown in the
   create response. Verified empirically against Go's `time` behavior rather
   than assumed.

## Assumptions and limitations

- Single-process scheduler; one logical destination (per brief).
- No recurring schedules, no natural-language date parsing (out of scope).
- Overdue policy: fire ASAP in due order, never skip; lateness visible via
  `fireAtUTC`. No max-lateness drop — documented choice.
- Attempt history retained per version chain (insertion order); no retention
  expiry implemented (production would age out old attempts).
- Response bodies: notification content only; no attachments.
- No auth/multi-tenancy/dashboard beyond the minimal list page.

## Production and scale

First changes: (1) Postgres + `SELECT … FOR UPDATE SKIP LOCKED` claiming
behind the existing `store.Provider` seam — one new file, no caller changes;
(2) retention/expiry policy for attempts + a max-lateness rule for ancient
overdue items (currently fire-always); (3) real provider behind the
`notify.Notifier` seam with per-destination retry budgets + DLQ alerting on
`failed` rate, `retrying` backlog age, and `staleDiscarded` spikes (edit
storms). What runs now vs proposed: everything above except Postgres,
retention, and real providers is implemented and tested here.

## AI usage

Used an AI coding assistant (OpenCode / Muse Spark) for scaffolding,
boilerplate, and test-draft iteration. I designed the version-gated finish,
delivery-key idempotency, clock injection, and DST policy; reviewed and ran
every file, test, benchmark, and live curl flow myself. I can explain and
modify any part.

## Credibility note

**websocket-chat-pub-sub** (https://github.com/sanskarpan/websocket-chat-pub-sub):
real-time WebSocket chat with Redis pub/sub fan-out, PostgreSQL history,
JWT auth, and horizontal scaling in Go + TypeScript.

- Problem: rooms needed live broadcast with history replay across scaled instances.
- My contribution: everything — Go gateway, Redis channels, Postgres store,
  reconnect/resume path, container setup.
- Scale/complexity: multi-instance delivery, presence + backlog replay,
  backpressure on slow consumers.
- Hardest decision: Redis pub/sub for transient fan-out + Postgres as source
  of truth for history (not Redis streams everywhere) — kept the hot path
  cheap while making reconnect deterministic. The same durable-history vs
  transient-delivery split shapes this submission (SQLite schedule vs
  in-flight claims).
