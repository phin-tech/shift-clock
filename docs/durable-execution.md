# Durable execution changeover (workflows you can resume)

> **Status: the durable-execution core (phases 0–4) is shipped and verified.**
> Sequence-keyed journal, crash resume, daemon-restart recovery, durable `sleep`
> with process **park/unload**, **signals**, per-deployment **concurrency**,
> **idempotent** submit, code **version** guarding, **exactly-once transactional
> steps**, and **query**. A few optional/nice-to-have items remain — tracked as a
> checklist in [Roadmap (todo)](#roadmap-todo) below.

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

## Roadmap (todo)

**Phase 0 — rename + sequence-keyed journal**
- [x] `flow→workflow`, `task→step` (clean rename)
- [x] `workflow_steps` journal keyed by `(workflow_id, step_seq)` (fixes loop collisions)
- [x] SDK step counter + skip-by-seq; context delivers the journal

**Phase 1 — durable resume across daemon restart**
- [x] `step_result`⇄`ack` RPC (durability barrier)
- [x] `workflows` status lifecycle + idempotent id
- [x] recovery scan re-dispatches interrupted workflows on startup
- [x] workflow-level retry policy

**Phase 2 — durable timers**
- [x] `sleep(secs)` journals `wake_at` before waiting; resume waits only the remainder

**Phase 3 — ergonomics + exactly-once**
- [x] idempotent submit (`trigger --id`)
- [x] per-step exponential backoff + jitter
- [x] `shift-clock show <id>` (status + journal)
- [x] exactly-once transactional steps — `set_state` commits in the same SQLite txn as the journal row (`store::commit_step_tx`)

**Phase 4 — Temporal-grade**
- [x] park / unload — long `sleep` exits the process; scheduler re-dispatches at `wake_at` (0 procs while parked)
- [x] signals — `wait_signal(name)` ⇄ `shift-clock signal <id> <name> <payload>`
- [x] per-deployment `concurrency = N` cap
- [x] version guarding — refuse to resume against a changed command
- [x] `query` — read a workflow's durable KV state (running, parked, or done)
- [ ] rate-limited work queues
- [ ] `query` of a *running* workflow's live in-memory state (today: persisted state only)
- [ ] exactly-once across *external* systems (today: same-DB writes only)

**Packaging / ops**
- [x] self-spawning daemon (tmux/herdr-style), `~/.config/shift-clock`
- [x] CI (fmt/clippy + build/test on mac+linux)
- [x] release-tag builds — mac arm64/x86_64 + linux arm64/x86_64 (Intel mac cross-compiled on Apple Silicon)
- [x] Homebrew tap covering all four arches (`brew install phin-tech/tap/shift-clock`)
- [ ] `HOMEBREW_TAP_TOKEN` secret so releases auto-bump the tap

## Mapping to the current codebase

| Change | Files |
|---|---|
| Journal schema + queries | `src/store.rs` |
| Context carries journal; step RPC + ack; `workflow_*`/`step_*` events | `src/protocol.rs` |
| Recovery scan, dispatch=load+spawn, RPC handler, retry policy | `src/worker.rs`, `src/server.rs` |
| step counter, skip-by-seq, `@workflow`/`step`, durable sleep | `sdk/python/shift_clock.py`, `sdk/typescript/shift_clock.mjs` |
| rename in examples | `flows/*`, `flows.toml` |
| status/list surfaces | `src/api.rs`, `src/cli.rs`, `src/tui.rs` |
