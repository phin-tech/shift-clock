"""shift-clock Python SDK — durable workflows (stdlib only, no native build).

A workflow is a normal Python program. Wrap the entrypoint with @workflow and
each unit of work with @step. When run by the worker, the SDK connects to the
per-workflow Unix socket (SHIFT_CLOCK_SOCK), reads the context handshake
(input + resume journal), and streams messages upward.

Durable execution (the DBOS model): each @step is assigned a deterministic
sequence number. On a normal run the step executes, then its result is journaled
via a `step_result` RPC that the worker acks *before* the workflow proceeds — the
ack is the durability barrier. On resume (after a crash), a completed step returns
its journaled output instead of re-executing.

Skip precedence:
  1. author's Target predicate (is_complete) — artifact-grounded
  2. resume journal (this step already succeeded in a prior attempt)
"""

import functools
import json
import os
import random
import socket
import time

_sock = None
_rfile = None
_wfile = None
_input = {}
_journal = {}        # "seq" (str) -> {"status": ..., "output": ...}
_signals = []        # unconsumed [{"name", "payload"}] delivered in context
_state = {}          # durable KV snapshot delivered in context
_pending_writes = [] # set_state() calls buffered during the current step
_seq = 0
_connected = False
_workflow_id = None  # this workflow's id (for deterministic child ids)


def _connect():
    global _sock, _rfile, _wfile, _input, _journal, _signals, _state, _connected
    global _workflow_id
    if _connected:
        return
    _connected = True
    _workflow_id = os.environ.get("SHIFT_CLOCK_WORKFLOW_ID")
    path = os.environ.get("SHIFT_CLOCK_SOCK")
    if not path:
        raw = os.environ.get("SHIFT_CLOCK_INPUT")
        if raw:
            try:
                _input = json.loads(raw)
            except Exception:
                _input = {}
        return
    _sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    _sock.connect(path)
    _rfile = _sock.makefile("r")
    _wfile = _sock.makefile("w")
    line = _rfile.readline()
    try:
        ctx = json.loads(line)
        _input = ctx.get("input") or {}
        _journal = ctx.get("journal") or {}
        _signals = list(ctx.get("signals") or [])
        _state = dict(ctx.get("state") or {})
        _workflow_id = ctx.get("workflow_id") or _workflow_id
    except Exception:
        _input, _journal, _signals, _state = {}, {}, [], {}


def _send(obj):
    if _wfile is None:
        return
    try:
        _wfile.write(json.dumps(obj) + "\n")
        _wfile.flush()
    except Exception:
        pass


def _rpc(obj):
    """Send a message and block for the worker's ack (durability barrier)."""
    if _wfile is None or _rfile is None:
        return
    _send(obj)
    try:
        # The only thing the worker writes back after context is acks.
        _rfile.readline()
    except Exception:
        pass


def _close():
    try:
        if _wfile:
            _wfile.flush()
        if _sock:
            _sock.close()
    except Exception:
        pass


def _safe(value):
    try:
        json.dumps(value)
        return value
    except Exception:
        return None


def _next_seq():
    global _seq
    s = _seq
    _seq += 1
    return s


def get_param(key, default=None):
    return _input.get(key, default)


def get_state(key, default=None):
    """Read the workflow's durable state (also what `shift-clock query` reads)."""
    return _state.get(key, default)


def set_state(key, value):
    """Write durable state. When called inside a @step, the write commits
    *atomically* with that step's journal entry — exactly-once, even across a
    crash/resume. Also makes the value visible to `shift-clock query`."""
    _state[key] = value
    _pending_writes.append({"key": key, "value": value})


def log(message, level="info", step=None):
    _send({"type": "log", "level": level, "message": str(message), "step": step})


# Threshold above which a sleep unloads the process instead of blocking (Phase 4).
_PARK_THRESHOLD = 3.0


def sleep(seconds, name="sleep"):
    """A durable timer. `wake_at` is journaled *before* waiting, so a crash
    mid-sleep resumes and only waits the remainder. Long sleeps park the process
    (unload) and are re-dispatched by the daemon at wake time — see _park()."""
    seq = _next_seq()
    key = str(seq)
    now = time.time()

    j = _journal.get(key)
    if j and j.get("status") == "success":
        wake_at = (j.get("output") or {}).get("wake_at", now)
        remaining = wake_at - now
        _send({"type": "step_skipped", "seq": seq, "name": name, "reason": "timer"})
        if remaining <= 0:
            return
        if remaining > _PARK_THRESHOLD:
            _park(wake_at)
        time.sleep(remaining)
        return

    wake_at = now + seconds
    _send({"type": "step_start", "seq": seq, "name": name, "attempt": 1})
    # Journal wake_at first — durability barrier before we wait.
    _rpc({"type": "step_result", "seq": seq, "name": name, "duration_ms": 0,
          "output": {"wake_at": wake_at}})
    if seconds > _PARK_THRESHOLD:
        _park(wake_at)
    time.sleep(seconds)


def _park(wake_at):
    """Unload: tell the worker to mark this workflow sleeping/waiting and
    re-dispatch it later (at wake_at, or when a signal arrives), then exit. On
    re-dispatch the journal replays completed steps. Zero resources while parked."""
    _send({"type": "workflow_park", "wake_at": wake_at})
    _close()
    raise SystemExit(0)


def wait_signal(name):
    """Durably wait for an external signal by `name`. If one is already pending,
    consume and return its payload; otherwise park (unload) until it arrives."""
    seq = _next_seq()
    key = str(seq)

    j = _journal.get(key)
    if j and j.get("status") == "success":
        return j.get("output")

    for i, sig in enumerate(_signals):
        if sig.get("name") == name:
            payload = sig.get("payload")
            del _signals[i]  # single-shot within this run
            _send({"type": "step_start", "seq": seq, "name": "signal:" + name, "attempt": 1})
            _rpc({"type": "signal_consume", "seq": seq, "name": name, "payload": payload})
            return payload

    # No signal yet — park and wait to be re-dispatched when one arrives.
    _send({"type": "log", "level": "info", "message": f"waiting for signal '{name}'…"})
    _park(None)


# -- child workflows (fan-out / fan-in) -------------------------------------

_PENDING = object()


class ChildFailed(Exception):
    """Raised by a join when a child workflow finished in a failed state."""

    def __init__(self, child_id):
        super().__init__(f"child workflow {child_id} failed")
        self.child_id = child_id


class ChildHandle:
    """A durable handle to a spawned child — a 'future' whose resolution survives
    a crash (it's journaled). Join with .result(), or in bulk via wait_all()."""

    def __init__(self, child_id):
        self.child_id = child_id
        self._seq = None  # join seq, assigned by wait_all() (position-stable)

    def result(self):
        return wait_all([self])[0]


def spawn(deployment, params=None):
    """Fork a child workflow (another deployment). Returns a ChildHandle.

    The child id is deterministic (`{workflow_id}.{seq}`), so a re-spawn on replay
    attaches to the existing child (idempotent) instead of duplicating it."""
    seq = _next_seq()
    j = _journal.get(str(seq))
    if j and j.get("status") == "success":
        return ChildHandle(j["output"]["child_id"])
    _send({"type": "step_start", "seq": seq, "name": f"spawn:{deployment}", "attempt": 1})
    child_id = f"{_workflow_id}.{seq}"
    _rpc({"type": "spawn_child", "seq": seq, "deployment": deployment, "input": params or {}})
    return ChildHandle(child_id)


def _child_output(handle, rec):
    if (rec or {}).get("status") != "success":
        raise ChildFailed(handle.child_id)
    return rec.get("output")


def _resolve_child(handle):
    """Return a child's output if it's done (journal hit or an arrived signal),
    else _PENDING. Consuming an arrived child is a journaled RPC."""
    j = _journal.get(str(handle._seq))
    if j and j.get("status") == "success":
        return _child_output(handle, j["output"])
    name = f"child:{handle.child_id}"
    for idx, sig in enumerate(_signals):
        if sig.get("name") == name:
            payload = sig.get("payload")
            del _signals[idx]
            _send({"type": "step_start", "seq": handle._seq, "name": name, "attempt": 1})
            _rpc({"type": "signal_consume", "seq": handle._seq, "name": name, "payload": payload})
            return _child_output(handle, payload)
    return _PENDING


def wait_all(handles):
    """Join on every child, returning outputs in handle (index) order. Drains all
    children that have already finished in a single wake — parking (unloading)
    once per wave of completions, not once per child."""
    # Phase A — reserve a deterministic join-seq per handle, up front, in order.
    # This keeps the seq walk identical on every run regardless of arrival timing.
    for h in handles:
        if h._seq is None:
            h._seq = _next_seq()
    # Phase B — resolve every available child now; park if any remain unfinished.
    results = [None] * len(handles)
    pending = False
    for i, h in enumerate(handles):
        r = _resolve_child(h)
        if r is _PENDING:
            pending = True
        else:
            results[i] = r
    if pending:
        _park(None)  # unload; on redispatch wait_all re-runs and replays journaled joins
    return results


def as_completed(handles):
    """Yield (handle, output) in the order children FINISH. Arrival order is
    nondeterministic, so each yield is recorded as a journaled step — replay
    reproduces the exact same order. Ties within one wake break by index."""
    remaining = set(range(len(handles)))
    while remaining:
        seq = _next_seq()  # one seq per yielded rank (deterministic in rank order)
        j = _journal.get(str(seq))
        if j and j.get("status") == "success":  # replay: rank -> child is recorded
            rec = j["output"]
            i = rec["index"]
            remaining.discard(i)
            yield handles[i], _child_output(handles[i], rec.get("payload"))
            continue
        # Original run: surface the lowest-index child that has already arrived.
        avail = [
            i for i in sorted(remaining)
            if any(s.get("name") == f"child:{handles[i].child_id}" for s in _signals)
        ]
        if not avail:
            _park(None)  # nothing new this wake — unload; redispatch brings more
        i = avail[0]
        name = f"child:{handles[i].child_id}"
        payload = None
        for idx, sig in enumerate(_signals):
            if sig.get("name") == name:
                payload = sig.get("payload")
                del _signals[idx]
                break
        _send({"type": "step_start", "seq": seq, "name": name, "attempt": 1})
        # Journal the rank -> child mapping (the ordering is the durable artifact).
        _rpc({"type": "step_result", "seq": seq, "name": name, "duration_ms": 0,
              "output": {"index": i, "payload": payload}})
        remaining.discard(i)
        yield handles[i], _child_output(handles[i], payload)


def workflow(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        _connect()
        _send({"type": "workflow_start", "name": fn.__name__})
        try:
            result = fn(*args, **kwargs)
        except Exception as e:
            _send({"type": "workflow_failure", "error": repr(e)})
            _close()
            raise
        _send({"type": "workflow_success", "output": _safe(result)})
        _close()
        return result

    return wrapper


def step(fn=None, *, name=None, retries=0, retry_delay=0.5, is_complete=None):
    def deco(f):
        sname = name or f.__name__

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            seq = _next_seq()  # assigned first so skipped steps keep alignment
            key = str(seq)

            # 1) Luigi-style Target.
            if is_complete is not None:
                try:
                    done = bool(is_complete())
                except Exception:
                    done = False
                if done:
                    _send({"type": "step_skipped", "seq": seq, "name": sname, "reason": "target"})
                    j = _journal.get(key)
                    return j.get("output") if j else None

            # 2) Resume journal.
            j = _journal.get(key)
            if j and j.get("status") == "success":
                _send({"type": "step_skipped", "seq": seq, "name": sname, "reason": "journal"})
                return j.get("output")

            attempt = 1
            while True:
                _send({"type": "step_start", "seq": seq, "name": sname, "attempt": attempt})
                _pending_writes.clear()  # buffer set_state() calls for this attempt
                t0 = time.time()
                try:
                    result = f(*args, **kwargs)
                except Exception as e:
                    dur = int((time.time() - t0) * 1000)
                    if attempt <= retries:
                        # Exponential backoff with 10% jitter.
                        delay = retry_delay * (2 ** (attempt - 1)) * (1 + 0.1 * random.random())
                        _send({
                            "type": "step_retry", "seq": seq, "name": sname, "attempt": attempt,
                            "next_attempt": attempt + 1,
                            "delay_ms": int(delay * 1000), "error": repr(e),
                        })
                        time.sleep(delay)
                        attempt += 1
                        continue
                    _send({
                        "type": "step_failure", "seq": seq, "name": sname,
                        "attempt": attempt, "duration_ms": dur, "error": repr(e),
                    })
                    raise
                dur = int((time.time() - t0) * 1000)
                # Durable RPC: journal the result (+ any set_state writes, committed
                # atomically), wait for ack, THEN proceed.
                msg = {
                    "type": "step_result", "seq": seq, "name": sname,
                    "duration_ms": dur, "output": _safe(result),
                }
                if _pending_writes:
                    msg["writes"] = list(_pending_writes)
                    _pending_writes.clear()
                _rpc(msg)
                return result

        return wrapper

    if callable(fn):
        return deco(fn)
    return deco
