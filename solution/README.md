# Durable Reminders — solution (Problem 3)

Small Go service for reminders and scheduled follow-ups that stays correct
across restarts, retries, edits, cancellations, and time-zone boundaries.

## Prerequisites

- Go 1.24+ only. No Docker, no CGO, no paid services
  (SQLite is pure Go via `modernc.org/sqlite`).

## Run (10-minute reviewer path, fully deterministic)

```text
cd solution
# engine with a manual (injectable) clock starting 2026-09-20T00:00Z
CLOCK_MODE=manual CLOCK_START=2026-09-20T00:00:00Z PORT=8080 BASE_DELAY_MS=200 go run ./cmd/server
```

Open `http://localhost:8080/` for the live list, clock display, and links.

```bash
# AC1: schedule in a named zone, advance controlled time, delivered once
curl -s -X POST localhost:8080/reminders -H 'Content-Type: application/json' -d \
 '{"id":"r1","content":"call mom","tz":"Asia/Kolkata","localTime":"2026-09-20T09:00"}'
curl -s -X POST localhost:8080/admin/clock -H 'Content-Type: application/json' -d '{"advanceMs":14400000}'
sleep 1; curl -s localhost:8080/reminders/r1 | python3 -m json.tool  # delivered, 1 attempt

# AC3: temporary failure then bounded retry (fail twice, then succeed)
curl -s -X POST localhost:8080/admin/notify -H 'Content-Type: application/json' -d '{"mode":"fail-first","failFirst":2}'
curl -s -X POST localhost:8080/reminders -H 'Content-Type: application/json' -d \
 '{"id":"r2","content":"standup","tz":"Asia/Kolkata","localTime":"2026-09-20T09:00"}'
for i in 1 2 3 4 5 6; do curl -s -X POST localhost:8080/admin/clock -H 'Content-Type: application/json' -d '{"advanceMs":60000}' >/dev/null; sleep 0.3; done
curl -s localhost:8080/reminders/r2 | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['reminder']['status'], [a['outcome'] for a in d['attempts']])"

# AC4: duplicate execution -> one logical notification
curl -s localhost:8080/metrics | python3 -c "import json,sys; print(json.load(sys.stdin)['logicalDelivered'])"

# AC5/AC6: edit bumps version; cancel wins
curl -s -X PATCH localhost:8080/reminders/r1 -H 'Content-Type: application/json' -d '{"content":"call dad"}'  # 400: already delivered (terminal)
curl -s -X POST localhost:8080/reminders -H 'Content-Type: application/json' -d \
 '{"id":"r3","content":"old","tz":"America/New_York","localTime":"2026-09-21T09:00"}'
curl -s -X PATCH localhost:8080/reminders/r3 -H 'Content-Type: application/json' -d '{"content":"new"}'
curl -s -X POST localhost:8080/reminders/r3/cancel -H 'Content-Type: application/json' -d '{}'
curl -s localhost:8080/reminders/r3 | python3 -c "import json,sys; print(json.load(sys.stdin)['reminder']['status'])"

# AC7: DST gap wall gets an explicit note, not a silent guess
curl -s -X POST localhost:8080/reminders -H 'Content-Type: application/json' -d \
 '{"id":"r4","content":"dst","tz":"America/New_York","localTime":"2026-03-08T02:30"}' | python3 -m json.tool  # tzNote

# AC2: restart recovery — kill -9, restart, overdue work fires ASAP
# (kill the server, re-run the same command, advance clock, watch pending items deliver)
```

Data persists in `./reminders.db` (SQLite WAL). Delete it for a clean slate.

## Tests

```text
cd solution
go test ./... -count=1
```

Covers: due-work discovery on clock advance, restart recovery of overdue
work, temp-failure→retry→success, exhaustion bound, permanent rejection,
uncertain-ack reconcile + honest exhaustion, duplicate execution
(in-memory and durable across restart), idempotent create, edit versioning
+ conflict + no-op, stale-claim/finish discard, cancel-wins race, terminal
edit rejection, crash recovery, atomic attempt/state, two zones + DST gap +
overlap, HTTP validation, metrics/filter.

## Verification benchmark

```text
cd solution
go run ./cmd/benchmark
```

20 items across Asia/Kolkata + America/New_York (delivered, edited,
cancelled, temp-failing, permanently failing, DST gap + overlap), mid-run
stop/restart, duplicate execution, clock advance to settle. Prints counts by
terminal state and fails on any mismatch. Expected: `BENCHMARK PASS` with
delivered 16 / failed 2 / cancelled 2 / logicalDelivered 16.

## Config

| env | default | meaning |
| --- | --- | --- |
| PORT | 8080 | listen port |
| DB_PATH | ./reminders.db | SQLite file |
| WORKERS | 4 | scheduler goroutines |
| MAX_ATTEMPTS | 5 | total attempts incl. first (1..20) |
| BASE_DELAY_MS | 500 | exponential backoff base |
| MAX_DELAY_MS | 30000 | backoff cap |
| CLOCK_MODE | system | `manual` enables admin time travel |
| CLOCK_START | 2026-09-20T00:00:00Z | manual clock start (RFC3339) |
| NOTIFY_MODE | ok | ok / fail-first / always-temp / always-perm / lost-ack |
| NOTIFY_FAIL_FIRST | 2 | temp failures before success in fail-first |
```

---

## Retry policy (documented)

- **Retryable:** temporary destination failures (down, timeout).
- **Uncertain:** the notification may have landed but the ack was lost.
  Retries like temporary under the same delivery key (the destination
  dedupes, so the retry reconciles instead of double-notifying), but the
  attempt is recorded distinctly — and if the budget runs out on an
  uncertain attempt, the terminal state says `possibly delivered`
  (reconcile via the delivery key) instead of a clean `failed`.
- **Permanent:** destination rejections (e.g. empty content) — straight to
  `failed`, 1 attempt. Retrying cannot fix them.
- **Backoff:** `base * 2^(n-1)` + up to 20% jitter, capped at `MAX_DELAY_MS`.
- **Guarantee:** at-least-once execution with exactly-once *logical*
  notification per occurrence. Two layers: the stable `deliveryKey`
  (`id:v<version>`) dedupes repeat executions at the destination, and a
  durable `deliveries` table (survives restarts, unlike process memory)
  lets the scheduler suppress a re-send entirely and reconcile the row.
- **Retained per attempt:** version, attempt number, started/finished stamps,
  outcome (`success`/`retryable`/`uncertain`/`permanent`), error, latency.
  Attempt + state transition persist atomically — no history/count skew.
- **Budget carries across edits:** an edit bumps the version and reschedules
  but does not reset `attempt_count`, so edits cannot mint unbounded retries.
  History rows keep their version for audit.

## Time policy (documented)

- Input is either a wall time (`localTime` + IANA `tz`) or an instant
  (`fireAt` RFC3339 + retained `tz`). `tz` is always stored.
- Nonexistent local times (spring-forward gap): pushed forward one hour,
  same morning, with a `tzNote`. All real-world gaps are 1h.
- Ambiguous local times (fall-back overlap): first occurrence (daylight
  time), with a `tzNote`.
- Restart/overdue policy: fire ASAP in due order. Late beats never; lateness
  is visible via `fireAtUTC`. No occurrence is ever skipped silently.
- Edit/cancel race policy: version check on finish — a result for a stale
  version (edited) or non-running row (cancelled) is discarded and counted
  in `metrics.staleDiscarded`, never overwrites the current version.
  The claim itself re-checks version + due instant, so an interleaved edit
  cannot cause early delivery either. Concurrent editors can pass
  `expectedVersion` for 409-on-conflict instead of last-writer-wins;
  no-op edits return the row untouched.
- **Multi-worker guarantees today:** single process, N goroutines, one
  SQLite writer. Claims are atomic CAS (`UPDATE … WHERE id+version+due`);
  losers back off and re-poll, so one logical execution at a time per row.
  Workers share fate (one crash takes all), and throughput caps at one
  writer — Postgres `SKIP LOCKED` is the multi-instance path (see
  SUBMISSION.md); the `store.Provider` seam already isolates that swap.
