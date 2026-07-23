# shift-clock

A local, language-agnostic durable orchestrator — think **DBOS meets a local
Prefect**. A single Rust binary that schedules (cron) and supervises
**workflows** written in any language, talks to them over a local Unix socket,
journals each **step** durably, and **resumes interrupted workflows** — even
after the daemon itself restarts.

Workflows are just commands — a `python` script, a `node` script, or a bare
`claude -p "…"` / shell script. The SDK is optional enrichment, never required.

```
                 clients (one HTTP/SSE API)
        ┌────────────┬─────────────┬──────────────┐
       CLI       TUI dashboard   (web later)   (remote later)
        └────────────┴─────────────┴──────────────┘
                          │ HTTP / SSE
                 ┌────────▼─────────┐
                 │  CONTROL PLANE   │  SQLite: deployments, workflows,
                 │  (axum HTTP API) │  workflow_steps (journal), events, logs
                 └────────┬─────────┘
                 ┌────────▼─────────┐        ┌──────────────┐
                 │     WORKER       │◀───────│  SCHEDULER   │  cron, overlap=skip
                 │ spawn + supervise│        │  catchup=none │
                 │ + recovery scan  │        └──────────────┘
                 └────────┬─────────┘
        spawn child + UDS handshake │  (journal in; step_result⇄ack)
        ┌────────────────┬──────────┴─────────────┐
   python etl.py     node report.ts          claude -p "…"   ./backup.sh
   (@workflow/@step)  (workflow/step)        (bare, exit-code) (bare)
        └─ events up + step_result RPC ─┘
```

## Quick start

```bash
cargo build

# One-shot: run a single workflow in-process, no daemon, live-streamed:
./target/debug/shift-clock run etl        # Python SDK workflow
./target/debug/shift-clock run report     # TypeScript workflow (node type-stripping)
./target/debug/shift-clock run hello      # bare shell workflow (exit-code judged)
./target/debug/shift-clock run crashy     # crash + resume from the journal

# No setup needed — the FIRST client command auto-spawns a detached background
# daemon (tmux/herdr-style: setsid + stdio→/dev/null), then reuses it:
./target/debug/shift-clock trigger etl     # ← spawns the daemon if none is running
./target/debug/shift-clock status          # RUNNING, pid, log path
./target/debug/shift-clock stop            # SIGTERM via pidfile

# …or run the daemon in the foreground:
./target/debug/shift-clock serve

# …or, if you want it started at LOGIN and kept alive by the OS (macOS launchd):
./deploy/install.sh
# …in another terminal:
./target/debug/shift-clock trigger etl
./target/debug/shift-clock trigger etl --id nightly-2026-07-23   # idempotent submit
./target/debug/shift-clock workflows
./target/debug/shift-clock show <workflow-id>                    # status + step journal
./target/debug/shift-clock signal <workflow-id> approve '{"by":"sam"}'  # wake a parked wf
./target/debug/shift-clock logs <workflow-id> -f                 # follow the merged timeline
./target/debug/shift-clock dashboard                             # TUI (needs a real terminal)
```

## Durable primitives (Phases 2–4)

```python
from shift_clock import workflow, step, sleep, wait_signal, log

@workflow
def main():
    prepare()
    sleep(3600)                    # durable timer — PARKS (unloads the process),
                                   # re-dispatched an hour later, zero resources meanwhile
    decision = wait_signal("approve")   # parks until `shift-clock signal … approve`
    finalize(decision)
```

- **Park / unload** — a long `sleep` or a `wait_signal` exits the process; the
  daemon re-dispatches at wake time or on signal arrival. The journal replays
  completed steps so it picks up exactly where it left off.
- **Signals** — `wait_signal(name)` ⇄ `shift-clock signal <id> <name> <payload>`.
- **Exactly-once steps** — `set_state(k, v)` inside a step commits *atomically*
  with the step's journal entry, so a crash never double-applies it.
- **Query** — `set_state` publishes durable state; read it any time with
  `shift-clock query <id> [key]` (works while running, parked, or done).
- **Concurrency** — `concurrency = N` per deployment caps parallel workflows.
- **Versioning** — a workflow refuses to resume if its command changed since submit.
- **Idempotent submit** — `--id <key>` returns the existing workflow.

```python
@step()
def charge(amount):
    set_state("charged_total", get_state("charged_total", 0) + amount)  # exactly-once
    return {"ok": True}
```

## Durability: resume a broken workflow

Each `@step` is assigned a deterministic sequence number and its result is
journaled in SQLite via a `step_result` RPC the worker **acks before the workflow
proceeds** (the durability barrier). On recovery the workflow re-runs from the
top and completed steps return their journaled output instead of re-executing —
the **DBOS model**.

- **Crash mid-workflow** → the worker re-dispatches (up to `retries`); completed
  steps are skipped.
- **Daemon restart** → on startup the worker's **recovery scan** finds workflows
  left `running` and resumes them.

```
$ shift-clock logs w-2a55afeb
▶ workflow start
  ● #0 stage_one start (attempt 1)
  ✔ #0 stage_one ok (210ms)          ← journaled
  │ sleeping 6s … (daemon killed here)
⟳ recovered — resuming after daemon restart
▶ workflow start
  ⤼ #0 stage_one skipped (journal)   ← not re-executed
  ● #1 stage_two start (attempt 1)
  ✔ #1 stage_two ok (206ms)
✔ workflow SUCCESS
```

See [`docs/durable-execution.md`](docs/durable-execution.md) for the full model,
the DBOS-vs-Temporal reasoning, and the roadmap (durable timers, exactly-once
steps, signals).

## Writing a workflow

Python (`flows/etl.py`):

```python
from shift_clock import workflow, step, get_param

@step(retries=2)
def extract():
    return {"rows": 100, "source": get_param("source", "dev")}

@step(is_complete=lambda: os.path.exists("out/report.txt"))  # Luigi Target
def load(data): ...

@workflow
def main():
    load(extract())

if __name__ == "__main__":
    main()
```

TypeScript (`flows/report.ts`):

```ts
import { workflow, step, getParam } from "../sdk/typescript/shift_clock.mjs";

const fetchData = step(async () => ({ items: 42 }), { name: "fetch", retries: 1 });

await workflow(async () => { await fetchData(); });
```

Bare (`flows.toml`): `cmd = ["claude", "-p", "triage inbox"]` — no SDK, judged by
exit code, stdout/stderr captured as logs.

## Skip precedence (is this step already done?)

1. **Author's Target** (`is_complete`) — artifact-grounded, survives DB loss.
2. **Resume journal** — this step already succeeded in a prior attempt.

## Design decisions

| Area | Decision |
|------|----------|
| Integration | Daemon supervises subprocesses; **not** FFI. One HTTP control plane, worker(s) underneath. |
| Remote | Control-plane/worker split; deployed local. Remote execution is a later add. |
| Workflow ↔ worker | Local **Unix socket**; events up, `step_result`⇄`ack` RPC for durability. |
| Definition | Static manifest (`flows.toml`); a deployment is a stored row. |
| Durability | Sequence-keyed **journal**; crash → re-dispatch; daemon restart → recovery scan (DBOS model). |
| Scheduling | 5-field cron, `overlap=skip`, `catchup=none`. |
| SDK stance | **Optional.** A workflow is minimally just a command judged by exit code. |
| Observability | **TUI dashboard** (Runs + Scheduled tabs) + CLI, both HTTP/SSE clients. |
| Daemon lifecycle | **Self-spawning** (tmux/herdr-style): a client `setsid`-forks a detached `serve` if none is reachable. Transport stays HTTP (remote-ready) — the Unix socket Herdr needs is only for PTY FD-passing, which we don't do. |

## Known limitations (see the design doc)

At-least-once side effects (use idempotency keys), determinism debt (keep
non-determinism inside steps), and no mid-flight versioning yet.
