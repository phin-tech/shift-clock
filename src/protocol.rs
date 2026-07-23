//! The workflow <-> worker protocol over a per-workflow Unix socket.
//!
//! Handshake: worker sends one `Context` line (workflow_id, input, and the
//! resume **journal**: seq -> {status, output}). The workflow then streams
//! messages upward. Most are fire-and-forget observability events; `step_result`
//! is a **request** — the worker journals it durably and replies with `ack`
//! before the workflow proceeds. That ack is the durability barrier that makes
//! resume correct.

use crate::store::StepRecord;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// worker -> workflow, sent once on connect.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Context {
    pub workflow_id: String,
    pub input: Value,
    /// Resume journal keyed by step sequence. Empty on the first attempt.
    pub journal: HashMap<u32, StepRecord>,
    /// Unconsumed signals delivered to a workflow waiting on one (Phase 4).
    #[serde(default)]
    pub signals: Vec<SignalDelivery>,
    /// The workflow's durable KV state (for get_state; also what `query` reads).
    #[serde(default)]
    pub state: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SignalDelivery {
    pub name: String,
    pub payload: Value,
}

/// A durable KV write committed atomically with a step's journal entry.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KvWrite {
    pub key: String,
    pub value: Value,
}

/// workflow -> worker, one JSON object per line.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FlowMsg {
    WorkflowStart {
        #[serde(default)]
        name: Option<String>,
    },
    StepStart {
        seq: u32,
        name: String,
        #[serde(default = "one")]
        attempt: u32,
    },
    StepSkipped {
        seq: u32,
        name: String,
        #[serde(default)]
        reason: String,
    },
    /// The durable RPC: a step succeeded. Worker journals it, then replies `ack`.
    /// If `writes` is non-empty, the journal row + those KV writes commit in ONE
    /// transaction (exactly-once for writes to the workflow's own store).
    StepResult {
        seq: u32,
        name: String,
        #[serde(default)]
        duration_ms: u64,
        #[serde(default)]
        output: Value,
        #[serde(default)]
        writes: Vec<KvWrite>,
    },
    StepRetry {
        seq: u32,
        name: String,
        #[serde(default = "one")]
        attempt: u32,
        #[serde(default)]
        next_attempt: u32,
        #[serde(default)]
        delay_ms: u64,
        #[serde(default)]
        error: String,
    },
    StepFailure {
        seq: u32,
        name: String,
        #[serde(default)]
        duration_ms: u64,
        #[serde(default)]
        error: String,
    },
    Log {
        #[serde(default)]
        level: String,
        message: String,
        #[serde(default)]
        step: Option<String>,
    },
    WorkflowSuccess {
        #[serde(default)]
        output: Value,
    },
    WorkflowFailure {
        #[serde(default)]
        error: String,
    },
    /// Durable park (Phase 4): unload the process; the daemon re-dispatches at
    /// `wake_at` (unix epoch seconds). If absent, the workflow waits for a signal.
    WorkflowPark {
        #[serde(default)]
        wake_at: Option<f64>,
    },
    /// Consume a signal (Phase 4): like step_result, but also marks the matching
    /// signal consumed. Worker journals the payload and acks.
    SignalConsume {
        seq: u32,
        name: String,
        #[serde(default)]
        payload: Value,
    },
}

/// worker -> workflow, in reply to a `step_result`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Ack {
    #[serde(rename = "type")]
    pub kind: String, // always "ack"
    pub seq: u32,
}

fn one() -> u32 {
    1
}

/// What clients receive over SSE. `kind` is "event" or "log".
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Envelope {
    pub workflow_id: String,
    pub seq: i64,
    pub ts: String,
    pub kind: String,
    pub payload: Value,
}
