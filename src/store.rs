//! SQLite persistence — now the durable substrate for resumable workflows.
//!
//! The `workflow_steps` journal is the source of truth for resume: it records
//! each step's output keyed by `(workflow_id, step_seq)`. On recovery the
//! workflow re-runs from the top and completed steps return their journaled
//! output instead of re-executing (the DBOS model). `events`/`logs` remain pure
//! observability.

use crate::config::Deployment;
use anyhow::Result;
use rusqlite::{params, Connection};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub fn now_iso() -> String {
    chrono::Local::now().to_rfc3339()
}

#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Workflow {
    pub id: String,
    pub deployment: String,
    pub trigger: String,
    pub status: String,
    #[serde(skip_serializing)]
    pub input: Value,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub attempts: i64,
    pub wake_at: Option<f64>,
    #[serde(skip_serializing)]
    pub version: String,
    pub created_at: String,
    pub finished_at: Option<String>,
}

/// A journal entry as delivered to the SDK in the context handshake.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StepRecord {
    pub status: String,
    pub output: Value,
}

/// A step row for display (TUI / CLI).
#[derive(Debug, Clone, serde::Serialize)]
pub struct StepView {
    pub seq: i64,
    pub name: String,
    pub status: String,
    pub duration_ms: Option<i64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LogLine {
    pub ts: String,
    pub stream: String,
    pub line: String,
}

impl Store {
    pub fn open(path: &str) -> Result<Store> {
        let conn = Connection::open(path)?;
        Self::migrate(&conn)?;
        Ok(Store {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn migrate(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS deployments (
                name TEXT PRIMARY KEY,
                cmd TEXT NOT NULL,
                cron TEXT,
                timezone TEXT,
                params TEXT NOT NULL,
                catchup TEXT NOT NULL,
                overlap TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                retries INTEGER NOT NULL,
                concurrency INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS workflows (
                id TEXT PRIMARY KEY,
                deployment TEXT NOT NULL,
                trigger TEXT NOT NULL,
                status TEXT NOT NULL,
                input TEXT NOT NULL,
                output TEXT,
                error TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                wake_at REAL,
                version TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                finished_at TEXT
            );
            CREATE TABLE IF NOT EXISTS kv (
                workflow_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (workflow_id, key)
            );
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_id TEXT NOT NULL,
                name TEXT NOT NULL,
                payload TEXT NOT NULL,
                consumed INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_signals_wf ON signals(workflow_id, consumed);
            CREATE TABLE IF NOT EXISTS workflow_steps (
                workflow_id TEXT NOT NULL,
                step_seq INTEGER NOT NULL,
                step_name TEXT NOT NULL,
                status TEXT NOT NULL,
                output TEXT,
                error TEXT,
                duration_ms INTEGER,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                PRIMARY KEY (workflow_id, step_seq)
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                type TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_wf ON events(workflow_id, id);
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                stream TEXT NOT NULL,
                line TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_logs_wf ON logs(workflow_id, id);
            "#,
        )?;
        Ok(())
    }

    // -- deployments --------------------------------------------------------

    pub fn upsert_deployment(&self, d: &Deployment) -> Result<()> {
        let c = self.conn.lock().unwrap();
        c.execute(
            "INSERT INTO deployments(name,cmd,cron,timezone,params,catchup,overlap,enabled,retries,concurrency)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
             ON CONFLICT(name) DO UPDATE SET
               cmd=?2, cron=?3, timezone=?4, params=?5, catchup=?6,
               overlap=?7, enabled=?8, retries=?9, concurrency=?10",
            params![
                d.name,
                serde_json::to_string(&d.cmd)?,
                d.cron,
                d.timezone,
                serde_json::to_string(&d.params)?,
                d.catchup,
                d.overlap,
                d.enabled as i64,
                d.retries as i64,
                d.concurrency as i64,
            ],
        )?;
        Ok(())
    }

    pub fn get_deployment(&self, name: &str) -> Result<Option<Deployment>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT name,cmd,cron,timezone,params,catchup,overlap,enabled,retries,concurrency
             FROM deployments WHERE name=?1",
        )?;
        let mut rows = stmt.query(params![name])?;
        if let Some(r) = rows.next()? {
            Ok(Some(row_to_deployment(r)?))
        } else {
            Ok(None)
        }
    }

    pub fn list_deployments(&self) -> Result<Vec<Deployment>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT name,cmd,cron,timezone,params,catchup,overlap,enabled,retries,concurrency
             FROM deployments ORDER BY name",
        )?;
        let rows = stmt.query_map([], |r| Ok(row_to_deployment(r)))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r??);
        }
        Ok(out)
    }

    // -- workflows ----------------------------------------------------------

    pub fn create_workflow(
        &self,
        id: &str,
        deployment: &str,
        trigger: &str,
        input: &Value,
        version: &str,
    ) -> Result<()> {
        let c = self.conn.lock().unwrap();
        let now = now_iso();
        c.execute(
            "INSERT INTO workflows(id,deployment,trigger,status,input,attempts,version,created_at,updated_at)
             VALUES(?1,?2,?3,'running',?4,0,?5,?6,?6)",
            params![id, deployment, trigger, input.to_string(), version, now],
        )?;
        Ok(())
    }

    /// Park a workflow: unloaded, to be re-dispatched at `wake_at` (or when a
    /// signal arrives, if wake_at is None → status 'waiting').
    pub fn park_workflow(&self, id: &str, wake_at: Option<f64>) -> Result<()> {
        let c = self.conn.lock().unwrap();
        let status = if wake_at.is_some() {
            "sleeping"
        } else {
            "waiting"
        };
        c.execute(
            "UPDATE workflows SET status=?2, wake_at=?3, updated_at=?4 WHERE id=?1",
            params![id, status, wake_at, now_iso()],
        )?;
        Ok(())
    }

    /// Resume a parked workflow to running (no retry bump).
    pub fn set_running(&self, id: &str) -> Result<()> {
        let c = self.conn.lock().unwrap();
        c.execute(
            "UPDATE workflows SET status='running', wake_at=NULL, updated_at=?2 WHERE id=?1",
            params![id, now_iso()],
        )?;
        Ok(())
    }

    /// Parked-on-timer workflows whose wake time has arrived.
    pub fn list_sleeping_due(&self, now_epoch: f64) -> Result<Vec<Workflow>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT id,deployment,trigger,status,input,output,error,attempts,wake_at,version,created_at,finished_at
             FROM workflows WHERE status='sleeping' AND wake_at IS NOT NULL AND wake_at <= ?1",
        )?;
        let rows = stmt.query_map(params![now_epoch], |r| Ok(row_to_workflow(r)))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r??);
        }
        Ok(out)
    }

    /// Count workflows currently executing for a deployment (concurrency limit).
    pub fn count_running(&self, deployment: &str) -> Result<i64> {
        let c = self.conn.lock().unwrap();
        Ok(c.query_row(
            "SELECT COUNT(*) FROM workflows WHERE deployment=?1 AND status='running'",
            params![deployment],
            |r| r.get(0),
        )?)
    }

    pub fn finish_workflow(
        &self,
        id: &str,
        status: &str,
        output: Option<&Value>,
        error: Option<&str>,
    ) -> Result<()> {
        let c = self.conn.lock().unwrap();
        let now = now_iso();
        c.execute(
            "UPDATE workflows SET status=?2, output=?3, error=?4, updated_at=?5, finished_at=?5 WHERE id=?1",
            params![id, status, output.map(|o| o.to_string()), error, now],
        )?;
        Ok(())
    }

    /// Mark a workflow running again (recovery re-dispatch) and count the attempt.
    pub fn mark_attempt(&self, id: &str) -> Result<()> {
        let c = self.conn.lock().unwrap();
        c.execute(
            "UPDATE workflows SET status='running', attempts=attempts+1, updated_at=?2, finished_at=NULL WHERE id=?1",
            params![id, now_iso()],
        )?;
        Ok(())
    }

    pub fn get_workflow(&self, id: &str) -> Result<Option<Workflow>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT id,deployment,trigger,status,input,output,error,attempts,wake_at,version,created_at,finished_at
             FROM workflows WHERE id=?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        if let Some(r) = rows.next()? {
            Ok(Some(row_to_workflow(r)?))
        } else {
            Ok(None)
        }
    }

    pub fn list_workflows(&self, limit: i64) -> Result<Vec<Workflow>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT id,deployment,trigger,status,input,output,error,attempts,wake_at,version,created_at,finished_at
             FROM workflows ORDER BY created_at DESC, rowid DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], |r| Ok(row_to_workflow(r)))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r??);
        }
        Ok(out)
    }

    /// Workflows still marked running — interrupted by a daemon crash; to resume.
    pub fn list_running(&self) -> Result<Vec<Workflow>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT id,deployment,trigger,status,input,output,error,attempts,wake_at,version,created_at,finished_at
             FROM workflows WHERE status='running' ORDER BY created_at",
        )?;
        let rows = stmt.query_map([], |r| Ok(row_to_workflow(r)))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r??);
        }
        Ok(out)
    }

    // -- signals (Phase 4) --------------------------------------------------

    pub fn add_signal(&self, workflow_id: &str, name: &str, payload: &Value) -> Result<()> {
        let c = self.conn.lock().unwrap();
        c.execute(
            "INSERT INTO signals(workflow_id,name,payload,created_at) VALUES(?1,?2,?3,?4)",
            params![workflow_id, name, payload.to_string(), now_iso()],
        )?;
        Ok(())
    }

    /// Unconsumed signals for a workflow, delivered in the context handshake.
    pub fn unconsumed_signals(&self, workflow_id: &str) -> Result<Vec<(String, Value)>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT name,payload FROM signals WHERE workflow_id=?1 AND consumed=0 ORDER BY id",
        )?;
        let rows = stmt.query_map(params![workflow_id], |r| {
            let name: String = r.get(0)?;
            let payload: String = r.get(1)?;
            Ok((name, payload))
        })?;
        let mut out = Vec::new();
        for r in rows {
            let (name, payload) = r?;
            out.push((name, serde_json::from_str(&payload).unwrap_or(Value::Null)));
        }
        Ok(out)
    }

    /// Mark the oldest unconsumed signal of `name` consumed (single-shot delivery).
    pub fn consume_signal(&self, workflow_id: &str, name: &str) -> Result<()> {
        let c = self.conn.lock().unwrap();
        c.execute(
            "UPDATE signals SET consumed=1 WHERE id = (
               SELECT id FROM signals WHERE workflow_id=?1 AND name=?2 AND consumed=0 ORDER BY id LIMIT 1)",
            params![workflow_id, name],
        )?;
        Ok(())
    }

    // -- step journal (authoritative for resume) ----------------------------

    #[allow(clippy::too_many_arguments)]
    pub fn upsert_step(
        &self,
        workflow_id: &str,
        seq: i64,
        name: &str,
        status: &str,
        output: Option<&Value>,
        error: Option<&str>,
        duration_ms: Option<i64>,
    ) -> Result<()> {
        let c = self.conn.lock().unwrap();
        let now = now_iso();
        let finished = if status == "started" {
            None
        } else {
            Some(now.clone())
        };
        c.execute(
            "INSERT INTO workflow_steps(workflow_id,step_seq,step_name,status,output,error,duration_ms,started_at,finished_at)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)
             ON CONFLICT(workflow_id,step_seq) DO UPDATE SET
               step_name=?3, status=?4, output=COALESCE(?5,output), error=?6,
               duration_ms=COALESCE(?7,duration_ms), finished_at=?9",
            params![
                workflow_id, seq, name, status,
                output.map(|o| o.to_string()), error, duration_ms, now, finished
            ],
        )?;
        Ok(())
    }

    /// Commit a step's journal row AND its durable KV writes in ONE transaction
    /// (DBOS-style exactly-once for writes to the workflow's own store). Either
    /// both land or neither does — so a crash before commit re-runs the step
    /// cleanly, and a crash after commit skips it on resume.
    #[allow(clippy::too_many_arguments)]
    pub fn commit_step_tx(
        &self,
        workflow_id: &str,
        seq: i64,
        name: &str,
        output: &Value,
        duration_ms: i64,
        writes: &[(String, Value)],
    ) -> Result<()> {
        let mut guard = self.conn.lock().unwrap();
        let tx = guard.transaction()?;
        let now = now_iso();
        tx.execute(
            "INSERT INTO workflow_steps(workflow_id,step_seq,step_name,status,output,error,duration_ms,started_at,finished_at)
             VALUES(?1,?2,?3,'success',?4,NULL,?5,?6,?6)
             ON CONFLICT(workflow_id,step_seq) DO UPDATE SET
               step_name=?3, status='success', output=?4, duration_ms=?5, finished_at=?6",
            params![workflow_id, seq, name, output.to_string(), duration_ms, now],
        )?;
        for (k, v) in writes {
            tx.execute(
                "INSERT INTO kv(workflow_id,key,value) VALUES(?1,?2,?3)
                 ON CONFLICT(workflow_id,key) DO UPDATE SET value=?3",
                params![workflow_id, k, v.to_string()],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// A workflow's published durable state (for get_state and query).
    pub fn get_state(
        &self,
        workflow_id: &str,
    ) -> Result<std::collections::BTreeMap<String, Value>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare("SELECT key,value FROM kv WHERE workflow_id=?1 ORDER BY key")?;
        let rows = stmt.query_map(params![workflow_id], |r| {
            let k: String = r.get(0)?;
            let v: String = r.get(1)?;
            Ok((k, v))
        })?;
        let mut out = std::collections::BTreeMap::new();
        for r in rows {
            let (k, v) = r?;
            out.insert(k, serde_json::from_str(&v).unwrap_or(Value::Null));
        }
        Ok(out)
    }

    /// The resume journal: seq -> {status, output} for all recorded steps.
    pub fn get_journal(&self, workflow_id: &str) -> Result<HashMap<u32, StepRecord>> {
        let c = self.conn.lock().unwrap();
        let mut stmt =
            c.prepare("SELECT step_seq,status,output FROM workflow_steps WHERE workflow_id=?1")?;
        let rows = stmt.query_map(params![workflow_id], |r| {
            let seq: i64 = r.get(0)?;
            let status: String = r.get(1)?;
            let output: Option<String> = r.get(2)?;
            Ok((seq, status, output))
        })?;
        let mut out = HashMap::new();
        for r in rows {
            let (seq, status, output) = r?;
            out.insert(
                seq as u32,
                StepRecord {
                    status,
                    output: output
                        .and_then(|s| serde_json::from_str(&s).ok())
                        .unwrap_or(Value::Null),
                },
            );
        }
        Ok(out)
    }

    pub fn list_steps(&self, workflow_id: &str) -> Result<Vec<StepView>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT step_seq,step_name,status,duration_ms,error
             FROM workflow_steps WHERE workflow_id=?1 ORDER BY step_seq",
        )?;
        let rows = stmt.query_map(params![workflow_id], |r| {
            Ok(StepView {
                seq: r.get(0)?,
                name: r.get(1)?,
                status: r.get(2)?,
                duration_ms: r.get(3)?,
                error: r.get(4)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    // -- events / logs (observability) --------------------------------------

    pub fn record_event(
        &self,
        workflow_id: &str,
        etype: &str,
        payload: &Value,
    ) -> Result<(i64, String)> {
        let c = self.conn.lock().unwrap();
        let ts = now_iso();
        c.execute(
            "INSERT INTO events(workflow_id,ts,type,payload) VALUES(?1,?2,?3,?4)",
            params![workflow_id, ts, etype, payload.to_string()],
        )?;
        Ok((c.last_insert_rowid(), ts))
    }

    pub fn record_log(&self, workflow_id: &str, stream: &str, line: &str) -> Result<(i64, String)> {
        let c = self.conn.lock().unwrap();
        let ts = now_iso();
        c.execute(
            "INSERT INTO logs(workflow_id,ts,stream,line) VALUES(?1,?2,?3,?4)",
            params![workflow_id, ts, stream, line],
        )?;
        Ok((c.last_insert_rowid(), ts))
    }

    pub fn get_logs(&self, workflow_id: &str) -> Result<Vec<LogLine>> {
        let c = self.conn.lock().unwrap();
        let mut stmt =
            c.prepare("SELECT ts,stream,line FROM logs WHERE workflow_id=?1 ORDER BY id")?;
        let rows = stmt.query_map(params![workflow_id], |r| {
            Ok(LogLine {
                ts: r.get(0)?,
                stream: r.get(1)?,
                line: r.get(2)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    pub fn get_events(&self, workflow_id: &str) -> Result<Vec<(i64, String, String, Value)>> {
        let c = self.conn.lock().unwrap();
        let mut stmt =
            c.prepare("SELECT id,ts,type,payload FROM events WHERE workflow_id=?1 ORDER BY id")?;
        let rows = stmt.query_map(params![workflow_id], |r| {
            let payload: String = r.get(3)?;
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                serde_json::from_str(&payload).unwrap_or(Value::Null),
            ))
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }
}

fn row_to_deployment(r: &rusqlite::Row) -> Result<Deployment> {
    let cmd: String = r.get(1)?;
    let params: String = r.get(4)?;
    Ok(Deployment {
        name: r.get(0)?,
        cmd: serde_json::from_str(&cmd)?,
        cron: r.get(2)?,
        timezone: r.get(3)?,
        params: serde_json::from_str(&params)?,
        catchup: r.get(5)?,
        overlap: r.get(6)?,
        enabled: r.get::<_, i64>(7)? != 0,
        retries: r.get::<_, i64>(8)? as u32,
        concurrency: r.get::<_, i64>(9)? as u32,
    })
}

fn row_to_workflow(r: &rusqlite::Row) -> Result<Workflow> {
    let input: String = r.get(4)?;
    let output: Option<String> = r.get(5)?;
    Ok(Workflow {
        id: r.get(0)?,
        deployment: r.get(1)?,
        trigger: r.get(2)?,
        status: r.get(3)?,
        input: serde_json::from_str(&input).unwrap_or(Value::Null),
        output: output.and_then(|s| serde_json::from_str(&s).ok()),
        error: r.get(6)?,
        attempts: r.get(7)?,
        wake_at: r.get(8)?,
        version: r.get(9)?,
        created_at: r.get(10)?,
        finished_at: r.get(11)?,
    })
}
