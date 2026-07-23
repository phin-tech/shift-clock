//! The worker: spawns a workflow as a child process, supervises it, journals
//! step results durably (DBOS model), and resumes on crash / restart.
//!
//! Phase 4 adds durable **park**: a long `sleep` (or a wait-for-signal) unloads
//! the process; the daemon re-dispatches it at wake time (scheduler) or when a
//! signal arrives. Also: per-deployment concurrency limits, idempotent submit,
//! and code-version stamping (refuse to resume mismatched code).

use crate::config::Deployment;
use crate::protocol::{Ack, Context, Envelope, FlowMsg, SignalDelivery};
use crate::store::{now_iso, Store, Workflow};
use anyhow::Result;
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast;
use uuid::Uuid;

#[derive(Clone)]
pub struct Worker {
    pub store: Store,
    pub tx: broadcast::Sender<Envelope>,
    pub root: PathBuf,
    active: Arc<Mutex<HashSet<String>>>,
}

/// How a single execution ended.
enum Terminal {
    Success,
    Failure,
    Parked(Option<f64>), // Some(wake_at) = timer; None = waiting for a signal
}

struct ExecResult {
    terminal: Option<Terminal>,
    exit_ok: bool,
}

/// A short, stable fingerprint of a deployment's command — the "code version".
fn version_of(cmd: &[String]) -> String {
    let mut h = DefaultHasher::new();
    cmd.hash(&mut h);
    format!("{:x}", h.finish())
}

fn now_epoch() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

impl Worker {
    pub fn new(store: Store, tx: broadcast::Sender<Envelope>, root: PathBuf) -> Worker {
        Worker {
            store,
            tx,
            root,
            active: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn is_active(&self, name: &str) -> bool {
        self.active.lock().unwrap().contains(name)
    }

    /// Create (or attach to) a workflow and spawn its supervising task.
    /// `id` makes submission idempotent: an existing id is returned as-is.
    pub fn trigger(
        &self,
        dep: Deployment,
        trigger: &str,
        extra_input: Value,
        id: Option<String>,
    ) -> Result<String> {
        // Idempotent submit: same id -> return the existing workflow.
        if let Some(ref wid) = id {
            if self.store.get_workflow(wid)?.is_some() {
                return Ok(wid.clone());
            }
        }
        // Concurrency limit.
        if dep.concurrency > 0 && self.store.count_running(&dep.name)? >= dep.concurrency as i64 {
            anyhow::bail!(
                "concurrency limit {} reached for '{}'",
                dep.concurrency,
                dep.name
            );
        }
        // Overlap = skip.
        {
            let mut active = self.active.lock().unwrap();
            if dep.overlap == "skip" && active.contains(&dep.name) {
                anyhow::bail!(
                    "overlap=skip: '{}' already has an active workflow",
                    dep.name
                );
            }
            active.insert(dep.name.clone());
        }
        let mut input = Value::Object(dep.params.clone());
        merge(&mut input, extra_input);

        let workflow_id =
            id.unwrap_or_else(|| format!("w-{}", &Uuid::new_v4().simple().to_string()[..8]));
        let version = version_of(&dep.cmd);
        self.store
            .create_workflow(&workflow_id, &dep.name, trigger, &input, &version)?;
        self.spawn_supervise(dep, workflow_id.clone(), input);
        Ok(workflow_id)
    }

    /// On daemon startup: resume any workflow left `running` by a previous life.
    pub fn recover(&self) -> Result<usize> {
        let running = self.store.list_running()?;
        for wf in &running {
            self.resume(wf, "workflow_recovered");
        }
        Ok(running.len())
    }

    /// Re-dispatch a parked workflow (called by the scheduler at wake time, or on
    /// signal arrival). Shared with recovery.
    fn resume(&self, wf: &Workflow, event: &str) {
        let Some(dep) = self.store.get_deployment(&wf.deployment).ok().flatten() else {
            let _ = self
                .store
                .finish_workflow(&wf.id, "failed", None, Some("deployment removed"));
            return;
        };
        // Version guard: refuse to resume against changed code.
        if !wf.version.is_empty() && version_of(&dep.cmd) != wf.version {
            let _ = self.store.finish_workflow(
                &wf.id,
                "failed",
                None,
                Some("version mismatch: deployment command changed since submission"),
            );
            return;
        }
        self.active.lock().unwrap().insert(dep.name.clone());
        let _ = self.store.set_running(&wf.id);
        let payload = json!({ "type": event });
        let (seq, ts) = self
            .store
            .record_event(&wf.id, event, &payload)
            .unwrap_or((0, now_iso()));
        self.broadcast(&wf.id, seq, ts, "event", payload);
        self.spawn_supervise(dep, wf.id.clone(), wf.input.clone());
    }

    /// Scheduler hook: re-dispatch timer-parked workflows whose wake has arrived.
    pub fn resume_due(&self) {
        if let Ok(due) = self.store.list_sleeping_due(now_epoch()) {
            for wf in &due {
                if !self.is_active(&wf.deployment) {
                    self.resume(wf, "workflow_wake");
                }
            }
        }
    }

    /// A signal arrived — if the workflow is parked waiting, wake it now.
    pub fn notify_signal(&self, workflow_id: &str) {
        if let Ok(Some(wf)) = self.store.get_workflow(workflow_id) {
            if (wf.status == "waiting" || wf.status == "sleeping")
                && !self.is_active(&wf.deployment)
            {
                self.resume(&wf, "workflow_signalled");
            }
        }
    }

    fn spawn_supervise(&self, dep: Deployment, workflow_id: String, input: Value) {
        let this = self.clone();
        tokio::spawn(async move {
            let name = dep.name.clone();
            if let Err(e) = this.supervise(dep, workflow_id.clone(), input).await {
                let _ =
                    this.store
                        .finish_workflow(&workflow_id, "failed", None, Some(&e.to_string()));
                eprintln!("[worker] workflow {workflow_id} errored: {e:#}");
            }
            this.active.lock().unwrap().remove(&name);
        });
    }

    async fn supervise(&self, dep: Deployment, workflow_id: String, input: Value) -> Result<()> {
        loop {
            let journal = self.store.get_journal(&workflow_id)?;
            let signals = self.store.unconsumed_signals(&workflow_id)?;
            let res = self
                .execute_once(&dep, &workflow_id, &input, journal, signals)
                .await?;

            match res.terminal {
                Some(Terminal::Success) => {
                    self.store
                        .finish_workflow(&workflow_id, "success", None, None)?;
                    return Ok(());
                }
                Some(Terminal::Failure) => {
                    self.store.finish_workflow(
                        &workflow_id,
                        "failed",
                        None,
                        Some("workflow reported failure"),
                    )?;
                    return Ok(());
                }
                Some(Terminal::Parked(wake_at)) => {
                    // Unloaded; scheduler / signal will re-dispatch. Not finished.
                    self.store.park_workflow(&workflow_id, wake_at)?;
                    return Ok(());
                }
                None => {
                    if res.exit_ok {
                        self.store
                            .finish_workflow(&workflow_id, "success", None, None)?;
                        return Ok(());
                    }
                    // Crash: re-dispatch up to `retries`.
                    let attempts = self
                        .store
                        .get_workflow(&workflow_id)?
                        .map(|w| w.attempts)
                        .unwrap_or(0);
                    if (attempts as u32) < dep.retries {
                        self.store.mark_attempt(&workflow_id)?;
                        let payload = json!({ "type": "workflow_resume", "attempt": attempts + 1 });
                        let (seq, ts) =
                            self.store
                                .record_event(&workflow_id, "workflow_resume", &payload)?;
                        self.broadcast(&workflow_id, seq, ts, "event", payload);
                        continue;
                    }
                    self.store
                        .finish_workflow(&workflow_id, "failed", None, Some("crashed"))?;
                    return Ok(());
                }
            }
        }
    }

    async fn execute_once(
        &self,
        dep: &Deployment,
        workflow_id: &str,
        input: &Value,
        journal: std::collections::HashMap<u32, crate::store::StepRecord>,
        signals: Vec<(String, Value)>,
    ) -> Result<ExecResult> {
        let sock_dir = if cfg!(unix) {
            PathBuf::from("/tmp")
        } else {
            std::env::temp_dir()
        };
        let sock_path = sock_dir.join(format!("sc-{workflow_id}.sock"));
        let _ = std::fs::remove_file(&sock_path);
        let listener = UnixListener::bind(&sock_path)?;

        let (prog, args) = dep
            .cmd
            .split_first()
            .ok_or_else(|| anyhow::anyhow!("empty cmd"))?;
        let mut cmd = tokio::process::Command::new(prog);
        cmd.args(args)
            .current_dir(&self.root)
            .env("SHIFT_CLOCK_SOCK", &sock_path)
            .env("SHIFT_CLOCK_WORKFLOW_ID", workflow_id)
            .env("SHIFT_CLOCK_INPUT", input.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let py_path = self.root.join("sdk/python");
        let existing = std::env::var("PYTHONPATH").unwrap_or_default();
        let joined = if existing.is_empty() {
            py_path.display().to_string()
        } else {
            format!("{}:{}", py_path.display(), existing)
        };
        cmd.env("PYTHONPATH", joined);
        if let Value::Object(map) = input {
            for (k, v) in map {
                let val = match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                cmd.env(format!("SC_PARAM_{}", k.to_uppercase()), val);
            }
        }

        let mut child = cmd.spawn()?;
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        self.spawn_log_pump(workflow_id.to_string(), stdout, "stdout");
        self.spawn_log_pump(workflow_id.to_string(), stderr, "stderr");

        let ctx = Context {
            workflow_id: workflow_id.to_string(),
            input: input.clone(),
            journal,
            signals: signals
                .into_iter()
                .map(|(name, payload)| SignalDelivery { name, payload })
                .collect(),
            state: self
                .store
                .get_state(workflow_id)
                .unwrap_or_default()
                .into_iter()
                .collect(),
        };

        let mut wait = Box::pin(child.wait());
        let mut terminal: Option<Terminal> = None;

        tokio::select! {
            biased;
            accepted = listener.accept() => {
                if let Ok((stream, _)) = accepted {
                    terminal = self.handle_conn(workflow_id, stream, ctx).await;
                }
            }
            status = &mut wait => {
                let exit_ok = status.map(|s| s.success()).unwrap_or(false);
                let _ = std::fs::remove_file(&sock_path);
                return Ok(ExecResult { terminal: None, exit_ok });
            }
        }

        let status = (&mut wait).await;
        let exit_ok = status.map(|s| s.success()).unwrap_or(false);
        let _ = std::fs::remove_file(&sock_path);
        Ok(ExecResult { terminal, exit_ok })
    }

    async fn handle_conn(
        &self,
        workflow_id: &str,
        stream: UnixStream,
        ctx: Context,
    ) -> Option<Terminal> {
        let (rd, mut wr) = stream.into_split();
        let ctx_line = format!("{}\n", serde_json::to_string(&ctx).unwrap());
        if wr.write_all(ctx_line.as_bytes()).await.is_err() {
            return None;
        }
        let _ = wr.flush().await;

        let mut terminal: Option<Terminal> = None;
        let mut lines = BufReader::new(rd).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let msg: FlowMsg = match serde_json::from_str(&line) {
                Ok(m) => m,
                Err(_) => {
                    self.emit_log(workflow_id, "sdk", &line);
                    continue;
                }
            };

            // Durable RPCs (journal + ack before the workflow proceeds).
            match &msg {
                FlowMsg::StepResult {
                    seq,
                    name,
                    duration_ms,
                    output,
                    writes,
                } => {
                    if writes.is_empty() {
                        self.journal_step(workflow_id, *seq, name, output, *duration_ms);
                    } else {
                        // Exactly-once: journal row + KV writes in one transaction.
                        let w: Vec<(String, Value)> = writes
                            .iter()
                            .map(|k| (k.key.clone(), k.value.clone()))
                            .collect();
                        let _ = self.store.commit_step_tx(
                            workflow_id,
                            *seq as i64,
                            name,
                            output,
                            *duration_ms as i64,
                            &w,
                        );
                        let payload = json!({
                            "type": "step_success", "seq": seq, "task": name,
                            "duration_ms": duration_ms, "writes": writes.len()
                        });
                        let (s, ts) = self
                            .store
                            .record_event(workflow_id, "step_success", &payload)
                            .unwrap_or((0, now_iso()));
                        self.broadcast(workflow_id, s, ts, "event", payload);
                    }
                    if self.ack(&mut wr, *seq).await.is_err() {
                        return terminal;
                    }
                    continue;
                }
                FlowMsg::SignalConsume { seq, name, payload } => {
                    let _ = self.store.consume_signal(workflow_id, name);
                    self.journal_step(workflow_id, *seq, &format!("signal:{name}"), payload, 0);
                    if self.ack(&mut wr, *seq).await.is_err() {
                        return terminal;
                    }
                    continue;
                }
                FlowMsg::WorkflowPark { wake_at } => {
                    let payload = json!({ "type": "workflow_park", "wake_at": wake_at });
                    let (s, ts) = self
                        .store
                        .record_event(workflow_id, "workflow_park", &payload)
                        .unwrap_or((0, now_iso()));
                    self.broadcast(workflow_id, s, ts, "event", payload);
                    terminal = Some(Terminal::Parked(*wake_at));
                    continue;
                }
                _ => {}
            }

            if let Some(t) = self.apply_msg(workflow_id, &msg) {
                terminal = Some(t);
            }
        }
        terminal
    }

    fn journal_step(
        &self,
        workflow_id: &str,
        seq: u32,
        name: &str,
        output: &Value,
        duration_ms: u64,
    ) {
        let _ = self.store.upsert_step(
            workflow_id,
            seq as i64,
            name,
            "success",
            Some(output),
            None,
            Some(duration_ms as i64),
        );
        let payload =
            json!({ "type": "step_success", "seq": seq, "task": name, "duration_ms": duration_ms });
        let (s, ts) = self
            .store
            .record_event(workflow_id, "step_success", &payload)
            .unwrap_or((0, now_iso()));
        self.broadcast(workflow_id, s, ts, "event", payload);
    }

    async fn ack(
        &self,
        wr: &mut tokio::net::unix::OwnedWriteHalf,
        seq: u32,
    ) -> std::io::Result<()> {
        let ack = Ack {
            kind: "ack".into(),
            seq,
        };
        let line = format!("{}\n", serde_json::to_string(&ack).unwrap());
        wr.write_all(line.as_bytes()).await?;
        wr.flush().await
    }

    fn apply_msg(&self, workflow_id: &str, msg: &FlowMsg) -> Option<Terminal> {
        let payload = serde_json::to_value(msg).unwrap_or(Value::Null);
        let etype = payload
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("event")
            .to_string();

        match msg {
            FlowMsg::StepStart { seq, name, .. } => {
                let _ = self.store.upsert_step(
                    workflow_id,
                    *seq as i64,
                    name,
                    "started",
                    None,
                    None,
                    None,
                );
            }
            FlowMsg::StepSkipped { seq, name, .. } => {
                let _ = self.store.upsert_step(
                    workflow_id,
                    *seq as i64,
                    name,
                    "skipped",
                    None,
                    None,
                    Some(0),
                );
            }
            FlowMsg::StepFailure {
                seq,
                name,
                duration_ms,
                error,
            } => {
                let _ = self.store.upsert_step(
                    workflow_id,
                    *seq as i64,
                    name,
                    "failed",
                    None,
                    Some(error),
                    Some(*duration_ms as i64),
                );
            }
            _ => {}
        }

        let (seq, ts) = self
            .store
            .record_event(workflow_id, &etype, &payload)
            .unwrap_or((0, now_iso()));
        self.broadcast(workflow_id, seq, ts, "event", payload);

        match msg {
            FlowMsg::WorkflowSuccess { .. } => Some(Terminal::Success),
            FlowMsg::WorkflowFailure { .. } => Some(Terminal::Failure),
            _ => None,
        }
    }

    fn spawn_log_pump<R>(&self, workflow_id: String, reader: R, stream: &'static str)
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        let this = self.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                this.emit_log(&workflow_id, stream, &line);
            }
        });
    }

    fn emit_log(&self, workflow_id: &str, stream: &str, line: &str) {
        let (seq, ts) = self
            .store
            .record_log(workflow_id, stream, line)
            .unwrap_or((0, now_iso()));
        self.broadcast(
            workflow_id,
            seq,
            ts,
            "log",
            json!({ "stream": stream, "line": line }),
        );
    }

    fn broadcast(&self, workflow_id: &str, seq: i64, ts: String, kind: &str, payload: Value) {
        let _ = self.tx.send(Envelope {
            workflow_id: workflow_id.to_string(),
            seq,
            ts,
            kind: kind.to_string(),
            payload,
        });
    }
}

fn merge(a: &mut Value, b: Value) {
    if let (Value::Object(am), Value::Object(bm)) = (a, b) {
        for (k, v) in bm {
            am.insert(k, v);
        }
    }
}
