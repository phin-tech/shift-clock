# Durable execution changeover (workflows you can resume)

> **Status: all phases (0–4) shipped, including the follow-ups.** Verified:
> sequence-keyed journal, crash resume, daemon-restart recovery, durable `sleep`
> with process **park/unload**, **signals** (wait/park/resume), per-deployment
> **concurrency** limits, **idempotent** submit, code **version** guarding,
> **exactly-once transactional steps** (`set_state` commits atomically with the
> journal), and **query** (`shift-clock query <id>` reads live durable state).

Goal: a broken **workflow** resumes instead of restarting — surviving both a flow
crash *and* a daemon restart — by journaling each step's result and skipping
completed steps on recovery.

## The key insight

We are already 70% of the way there. Today the worker **relaunches** a crashed
flow from the top and the SDK **skips** tasks whose result is in a resume
manifest. That is a primitive version of what DBOS does. The changeover is mostly
about making that journal **authoritative, durable, and sequence-keyed** — not a
rearchitecture.

## Target model: DBOS-flavored (recommended), Temporal as north star

| | DBOS-flavored (our target) | Temporal (north star) |
|---|---|---|
| Recovery | re-run workflow from top; completed **steps** return journaled output | replay an event history in a deterministic VM |
| Determinism required | ordering only (same steps, same order, up to failure) | strict (no wall-clock/random/IO in workflow body) |
| Per-language runtime | modest SDK extension (step counter + skip) | heavy deterministic replay engine per language |
| Durable timers | journaled `sleep`, in-process short / rescheduled long | first-class, spanning days |
| Store | our SQLite control plane | dedicated history service |
| Fit for a local tool | ✅ excellent | overkill until you need signals / multi-day / human-in-loop |

We build the DBOS-flavored model. It gets "resume a broken workflow" with far
less machinery, and every artifact it produces (durable journal, step RPC,
recovery scan) is *also* the on-ramp to Temporal-grade features if we ever need
them.

## Terminology changes

| Today | Becomes | Notes |
|---|---|---|
| `@flow` | `@workflow` | the durable, resumable entrypoint = one process/run |
| `@task` | `@step` | a journaled unit of work (clean rename — no alias) |
| `flow_start`/`flow_success`/`flow_failure` events | `workflow_*` | protocol rename |
| `task_*` events | `step_*` | protocol rename |
| control-plane `run` | `workflow` (row) | the durable record; gains status + output + idempotency id |
| `deployment` | unchanged | still name + cmd + schedule + policies |

Reversal note vs the original grill: this **partially reverses** two earlier POC
decisions — "SDK-driven, worker only observes" becomes "worker owns a durable
journal the SDK consults," and "within-run checkpointing" becomes "cross-restart
durable resume." The subprocess-supervision architecture itself survives intact.

## Data model (the authoritative journal)

`events`/`logs` stay as observability (SSE, TUI). The **journal** below becomes
the source of truth for resume.

```sql
-- was `runs`; now a durable, idempotent workflow record
workflows(
  id            TEXT PRIMARY KEY,   -- workflow_id: stable idempotency key
  deployment    TEXT NOT NULL,
  input         TEXT NOT NULL,      -- params JSON (frozen at first submit)
  status        TEXT NOT NULL,      -- pending|running|success|failed|cancelled
  output        TEXT,               -- workflow return value JSON
  error         TEXT,
  attempts      INTEGER NOT NULL DEFAULT 0,
  created_at    TEXT NOT NULL,
  updated_at    TEXT NOT NULL
)

-- the step journal, keyed by SEQUENCE (fixes the name-collision bug in loops)
workflow_steps(
  workflow_id   TEXT NOT NULL,
  step_seq      INTEGER NOT NULL,   -- deterministic per-workflow counter, 0..N
  step_name     TEXT NOT NULL,      -- for display; NOT the key
  status        TEXT NOT NULL,      -- started|success|failed
  output        TEXT,               -- recorded result JSON (returned on replay)
  error         TEXT,
  started_at    TEXT NOT NULL,
  finished_at   TEXT,
  PRIMARY KEY (workflow_id, step_seq)
)

-- durable timers (phase 2)
workflow_timers(
  workflow_id   TEXT NOT NULL,
  step_seq      INTEGER NOT NULL,
  wake_at       TEXT NOT NULL,
  fired         INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (workflow_id, step_seq)
)
```

**Why sequence-keyed matters:** today `task_results` keys by *name*, so a
workflow that calls `fetch()` in a loop collides — a real correctness bug. Keying
by a deterministic call counter (`step_seq`) is both the fix and the Temporal/DBOS
identity model.

## Protocol changes (the socket turns bidirectional)

Durable execution forces the request/response channel we flagged early in the
grill. Two changes:

1. **Context handshake** carries the journal, not a name→result map:
   `{"workflow_id","input","steps":[{seq,status,output}...]}`
2. **Step-result RPC (with ack):** on a step boundary the SDK sends
   `{"type":"step_result","seq":N,"name":...,"output":...}` and **waits for the
   worker's `{"type":"ack","seq":N}`** before proceeding. The ack means "durably
   journaled," which is what makes recovery correct. (Fire-and-forget would leave
   a window where a step ran but wasn't recorded → re-run on resume = at-least-once.)

## SDK changes (Python + TS, symmetric)

Per workflow process the SDK keeps a **monotonic `step_seq` counter** starting at
0. Each `step()` call:

```
seq = next_seq()                       # deterministic position
j = journal.get(seq)
if j and j.status == "success":
    emit step_skipped(seq, name); return j.output      # DBOS replay: recorded result
emit step_start(seq, name)
result = run-with-retries(fn)                            # existing retry logic
rpc: step_result(seq, name, output=result); await ack   # durable, before continuing
emit step_success(seq, name); return result
```

- `@workflow` (was `@flow`): on the outermost call — connect, read context+journal,
  emit `workflow_start`, run body, emit `workflow_success`/`workflow_failure`.
- **Durable sleep** (phase 2): `sleep(secs)` is a journaled step — record `wake_at`;
  on replay if already elapsed, return immediately; else wait.
- **Determinism guidance** (doc + lint later): branch on nothing non-deterministic
  in the workflow body; put all IO/time/randomness inside `step`s.

## Worker / orchestrator changes

- **Recovery scan on startup** (the headline feature): find `workflows` with
  `status='running'` (interrupted by a daemon crash) and **re-dispatch** them —
  resume across daemon restarts, not just per-process relaunch.
- **Dispatch = load journal → pass in context → spawn.** Resume and first-run use
  the identical path; the journal (possibly empty) drives skipping.
- **Step-result RPC handler:** persist `workflow_steps` row synchronously, then
  ack. This is the durability barrier.
- **Retry policy** replaces the bounded `max_relaunch`: a workflow-level
  `retries` + backoff; on crash, re-dispatch until policy exhausted, journal makes
  each re-dispatch cheap.
- **Idempotent submit:** triggering with an existing `workflow_id` attaches to /
  resumes the existing workflow instead of creating a duplicate.

## Constraints & known limitations (call them out now)

- **At-least-once side effects:** a crash *after* a step's external effect but
  *before* its ack re-runs that step. Mitigate with idempotency keys; offer an
  optional *transactional step* that writes to the control-plane SQLite atomically
  with its journal row (DBOS-style exactly-once for same-DB work).
- **Determinism debt:** if the workflow body branches on non-journaled
  non-determinism, replay can diverge and mis-map `step_seq`. Documented rule:
  non-determinism lives in steps.
- **Versioning:** editing a workflow's step structure mid-flight breaks seq
  mapping. Phase 0 pins nothing; later, stamp a code version per workflow and
  refuse/branch on mismatch.

## Phased roadmap — ✅ all shipped

**Phase 0 — rename + sequence-keyed journal. ✅**
`flow→workflow`, `task→step` (clean rename, no back-compat). Replace `task_results` with
`workflow_steps` keyed by `(workflow_id, step_seq)`. SDKs add the step counter and
skip-by-seq. Context delivers the journal. *Gets:* correct checkpointing in loops;
durable relaunch within a live daemon. *Still in-process only.*

**Phase 1 — durable resume across daemon restart.**
Step-result RPC with ack; `workflows` status lifecycle + idempotent id; recovery
scan re-dispatches interrupted workflows on startup; workflow-level retry policy.
*Gets:* "resume a broken workflow" even if the whole daemon died. **This is the
feature you asked for.**

**Phase 2 — durable timers / sleep. ✅**
`sleep(secs)` journals `wake_at` before waiting; a crash mid-sleep resumes and
waits only the remainder. Long sleeps unload the process (see Phase 4).

**Phase 3 — ergonomics + exactly-once. ✅**
Idempotent submit (`trigger --id <key>` returns the existing workflow), per-step
exponential backoff + jitter, `shift-clock show <id>` (status + step journal), and
**exactly-once transactional steps**: `set_state(k, v)` calls inside a step are
buffered and committed **in the same SQLite transaction as the step's journal
row** (`store::commit_step_tx`). Either both land or neither does, so a crash
before commit re-runs the step cleanly and a crash after commit skips it —
`charged_total` stays 100, never 200. (External effects outside the store are
still at-least-once; record their completion via `set_state` to get exactly-once.)

**Phase 4 — Temporal-grade. ✅**
- **Park / unload** — a long `sleep` exits the process; the scheduler
  re-dispatches at `wake_at` (zero resources while parked). Verified: 0 processes
  alive during a 6s durable sleep.
- **Signals** — `wait_signal(name)` parks until `shift-clock signal <id> <name>
  <payload>` arrives, then resumes and returns the payload.
- **Concurrency** — per-deployment `concurrency = N` cap; over-limit triggers are
  rejected.
- **Versioning** — a workflow stamps a hash of its command; resume against a
  changed command is refused with a version-mismatch error.
- **Query** — `shift-clock query <id> [key]` reads a workflow's durable state
  (`GET /workflows/:id/state`). Works whether it's running, parked, or done —
  the state lives in the KV table, not in a live process.
*Still open (genuinely optional):* rate-limited queues.

## Mapping to the current codebase

| Change | Files |
|---|---|
| Journal schema + queries | `src/store.rs` |
| Context carries journal; step RPC + ack; `workflow_*`/`step_*` events | `src/protocol.rs` |
| Recovery scan, dispatch=load+spawn, RPC handler, retry policy | `src/worker.rs`, `src/server.rs` |
| step counter, skip-by-seq, `@workflow`/`step`, durable sleep | `sdk/python/shift_clock.py`, `sdk/typescript/shift_clock.mjs` |
| rename in examples | `flows/*`, `flows.toml` |
| status/list surfaces | `src/api.rs`, `src/cli.rs`, `src/tui.rs` |
