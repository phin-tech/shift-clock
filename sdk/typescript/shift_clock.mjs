// shift-clock TypeScript/JS SDK — durable workflows (Node stdlib only).
//
// Same contract as the Python SDK: connect to SHIFT_CLOCK_SOCK, read the context
// handshake (input + resume journal), stream messages upward. Each step gets a
// deterministic sequence number; on a normal run its result is journaled via a
// step_result RPC the worker acks before the workflow proceeds; on resume, a
// completed step returns its journaled output instead of re-executing.
//
//   import { workflow, step, getParam, log } from ".../shift_clock.mjs";
//   const fetchData = step(async () => {...}, { name: "fetch", retries: 2 });
//   await workflow(async () => { await fetchData(); });

import net from "node:net";

let sock = null;
let input = {};
let journal = {}; // "seq" -> { status, output }
let signals = []; // unconsumed [{ name, payload }] delivered in context
let state = {}; // durable KV snapshot delivered in context
let pendingWrites = []; // setState() calls buffered during the current step
let seq = 0;
let connected = false;

const PARK_THRESHOLD = 3.0; // seconds; longer sleeps unload the process (Phase 4)

// A tiny line reader over the socket: resolves one \n-delimited line at a time.
let rbuf = "";
let waiters = [];
function feed(chunk) {
  rbuf += chunk.toString("utf8");
  let nl;
  while ((nl = rbuf.indexOf("\n")) !== -1) {
    const line = rbuf.slice(0, nl);
    rbuf = rbuf.slice(nl + 1);
    const w = waiters.shift();
    if (w) w(line);
  }
}
function readLine() {
  return new Promise((resolve) => waiters.push(resolve));
}

async function connect() {
  if (connected) return;
  connected = true;
  const path = process.env.SHIFT_CLOCK_SOCK;
  if (!path) {
    const raw = process.env.SHIFT_CLOCK_INPUT;
    if (raw) {
      try {
        input = JSON.parse(raw);
      } catch {
        input = {};
      }
    }
    return;
  }
  sock = await new Promise((resolve, reject) => {
    const s = net.createConnection({ path }, () => resolve(s));
    s.on("error", reject);
  });
  sock.on("data", feed);
  const ctxLine = await readLine();
  try {
    const ctx = JSON.parse(ctxLine);
    input = ctx.input || {};
    journal = ctx.journal || {};
    signals = ctx.signals || [];
    state = ctx.state || {};
  } catch {
    input = {};
    journal = {};
    signals = [];
    state = {};
  }
}

function send(obj) {
  if (!sock) return;
  try {
    sock.write(JSON.stringify(obj) + "\n");
  } catch {
    /* ignore */
  }
}

// Send and block for the worker's ack (durability barrier).
async function rpc(obj) {
  if (!sock) return;
  send(obj);
  await readLine();
}

function close() {
  try {
    if (sock) sock.end();
  } catch {
    /* ignore */
  }
}

function safe(value) {
  try {
    JSON.stringify(value);
    return value;
  } catch {
    return null;
  }
}

function nextSeq() {
  return seq++;
}

export function getParam(key, dflt = null) {
  return key in input ? input[key] : dflt;
}

export function getState(key, dflt = null) {
  return key in state ? state[key] : dflt;
}

// Write durable state. Inside a step(), the write commits atomically with the
// step's journal entry — exactly-once across crash/resume. Visible to `query`.
export function setState(key, value) {
  state[key] = value;
  pendingWrites.push({ key, value });
}

export function log(message, level = "info", step = null) {
  send({ type: "log", level, message: String(message), step });
}

// Unload: flush a park message, then exit the process. The daemon re-dispatches
// at wake_at (or on signal arrival); the journal replays completed steps.
async function park(wakeAt) {
  send({ type: "workflow_park", wake_at: wakeAt });
  await new Promise((res) => (sock ? sock.end(res) : res()));
  process.exit(0);
}

// A durable timer. Journals wake_at before waiting; long sleeps park (unload).
export async function sleep(seconds, name = "sleep") {
  const s = nextSeq();
  const key = String(s);
  const now = Date.now() / 1000;

  const j = journal[key];
  if (j && j.status === "success") {
    const wakeAt = (j.output && j.output.wake_at) || now;
    const remaining = wakeAt - now;
    send({ type: "step_skipped", seq: s, name, reason: "timer" });
    if (remaining <= 0) return;
    if (remaining > PARK_THRESHOLD) await park(wakeAt);
    await new Promise((r) => setTimeout(r, remaining * 1000));
    return;
  }

  const wakeAt = now + seconds;
  send({ type: "step_start", seq: s, name, attempt: 1 });
  await rpc({ type: "step_result", seq: s, name, duration_ms: 0, output: { wake_at: wakeAt } });
  if (seconds > PARK_THRESHOLD) await park(wakeAt);
  await new Promise((r) => setTimeout(r, seconds * 1000));
}

// Durably wait for an external signal; park (unload) until it arrives.
export async function waitSignal(name) {
  const s = nextSeq();
  const key = String(s);

  const j = journal[key];
  if (j && j.status === "success") return j.output;

  const idx = signals.findIndex((sig) => sig.name === name);
  if (idx !== -1) {
    const payload = signals[idx].payload;
    signals.splice(idx, 1);
    send({ type: "step_start", seq: s, name: "signal:" + name, attempt: 1 });
    await rpc({ type: "signal_consume", seq: s, name, payload });
    return payload;
  }

  send({ type: "log", level: "info", message: `waiting for signal '${name}'…` });
  await park(null);
}

export async function workflow(fn) {
  await connect();
  send({ type: "workflow_start" });
  let result;
  try {
    result = await fn();
  } catch (e) {
    send({ type: "workflow_failure", error: String(e && e.stack ? e.stack : e) });
    close();
    process.exitCode = 1;
    throw e;
  }
  send({ type: "workflow_success", output: safe(result) });
  close();
  return result;
}

export function step(fn, opts = {}) {
  const name = opts.name || fn.name || "step";
  const retries = opts.retries || 0;
  const retryDelayMs = opts.retryDelay != null ? opts.retryDelay * 1000 : 500;
  const isComplete = opts.isComplete || null;

  return async function (...args) {
    const s = nextSeq(); // assigned first so skipped steps keep alignment
    const key = String(s);

    // 1) Luigi-style Target.
    if (isComplete) {
      let done = false;
      try {
        done = !!(await isComplete());
      } catch {
        done = false;
      }
      if (done) {
        send({ type: "step_skipped", seq: s, name, reason: "target" });
        return journal[key] ? journal[key].output : undefined;
      }
    }
    // 2) Resume journal.
    if (journal[key] && journal[key].status === "success") {
      send({ type: "step_skipped", seq: s, name, reason: "journal" });
      return journal[key].output;
    }

    let attempt = 1;
    for (;;) {
      send({ type: "step_start", seq: s, name, attempt });
      pendingWrites = []; // buffer setState() calls for this attempt
      const t0 = Date.now();
      try {
        const result = await fn(...args);
        // Durable RPC: journal the result (+ any setState writes, atomically),
        // wait for ack, THEN proceed.
        const msg = {
          type: "step_result",
          seq: s,
          name,
          duration_ms: Date.now() - t0,
          output: safe(result),
        };
        if (pendingWrites.length) {
          msg.writes = pendingWrites;
          pendingWrites = [];
        }
        await rpc(msg);
        return result;
      } catch (e) {
        const dur = Date.now() - t0;
        if (attempt <= retries) {
          // Exponential backoff with 10% jitter.
          const delay = retryDelayMs * 2 ** (attempt - 1) * (1 + 0.1 * Math.random());
          send({
            type: "step_retry",
            seq: s,
            name,
            attempt,
            next_attempt: attempt + 1,
            delay_ms: Math.round(delay),
            error: String(e),
          });
          await new Promise((r) => setTimeout(r, delay));
          attempt += 1;
          continue;
        }
        send({ type: "step_failure", seq: s, name, attempt, duration_ms: dur, error: String(e) });
        throw e;
      }
    }
  };
}
