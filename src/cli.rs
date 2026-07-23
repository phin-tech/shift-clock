//! Non-serve subcommands. `workflows`, `logs`, `trigger` are HTTP clients of a
//! running daemon. `run` is a self-contained one-shot: an ephemeral in-memory
//! control plane + worker executes one workflow and streams it live — no daemon.

use crate::client::Client;
use crate::config::load_manifest;
use crate::protocol::Envelope;
use crate::store::Store;
use crate::worker::Worker;
use anyhow::{anyhow, Result};
use serde_json::{Map, Value};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::broadcast;

pub fn parse_params(pairs: &[String]) -> Value {
    let mut map = Map::new();
    for p in pairs {
        if let Some((k, v)) = p.split_once('=') {
            map.insert(k.to_string(), Value::String(v.to_string()));
        }
    }
    Value::Object(map)
}

pub async fn trigger(host: &str, name: &str, params: Value, id: Option<String>) -> Result<()> {
    crate::daemon::ensure(host).await?;
    let client = Client::new(host);
    let workflow_id = client.trigger(name, params, id.as_deref()).await?;
    println!("triggered {name} -> {workflow_id}");
    println!("watch:  shift-clock logs {workflow_id} -f");
    Ok(())
}

pub async fn signal(host: &str, id: &str, name: &str, payload: Value) -> Result<()> {
    crate::daemon::ensure(host).await?;
    let client = Client::new(host);
    client.signal(id, name, payload).await?;
    println!("signalled {id} <- {name}");
    Ok(())
}

pub async fn query(host: &str, id: &str, key: Option<String>) -> Result<()> {
    crate::daemon::ensure(host).await?;
    let state = Client::new(host).query(id).await?;
    match key {
        Some(k) => println!(
            "{}",
            state
                .get(&k)
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".into())
        ),
        None => println!(
            "{}",
            serde_json::to_string_pretty(&state).unwrap_or_default()
        ),
    }
    Ok(())
}

pub async fn show(host: &str, id: &str) -> Result<()> {
    crate::daemon::ensure(host).await?;
    let client = Client::new(host);
    let d = client.get_workflow(id).await?;
    let w = &d["workflow"];
    println!("workflow {}", s(w, "id"));
    println!("  flow      {}", s(w, "deployment"));
    println!("  status    {}", s(w, "status"));
    println!("  trigger   {}", s(w, "trigger"));
    println!(
        "  attempts  {}",
        w.get("attempts").and_then(|v| v.as_i64()).unwrap_or(0)
    );
    if let Some(wake) = w.get("wake_at").and_then(|v| v.as_f64()) {
        println!("  wake_at   {wake} (epoch)");
    }
    println!("  created   {}", s(w, "created_at"));
    if let Some(fin) = w.get("finished_at").and_then(|v| v.as_str()) {
        println!("  finished  {fin}");
    }
    if let Some(err) = w.get("error").and_then(|v| v.as_str()) {
        println!("  error     {err}");
    }
    println!("  steps:");
    if let Some(steps) = d["steps"].as_array() {
        for st in steps {
            let dur = st
                .get("duration_ms")
                .and_then(|v| v.as_i64())
                .map(|d| format!("{d}ms"))
                .unwrap_or_else(|| "—".into());
            println!(
                "    #{:<3} {:<18} {:<9} {}",
                st.get("seq").and_then(|v| v.as_i64()).unwrap_or(0),
                s(st, "name"),
                s(st, "status"),
                dur
            );
        }
    }
    Ok(())
}

pub async fn workflows(host: &str, limit: i64) -> Result<()> {
    crate::daemon::ensure(host).await?;
    let client = Client::new(host);
    let workflows = client.list_workflows(limit).await?;
    println!(
        "{:<12} {:<18} {:<9} {:<8} {:<4} CREATED",
        "ID", "FLOW", "STATUS", "TRIGGER", "ATT"
    );
    for w in workflows {
        println!(
            "{:<12} {:<18} {:<9} {:<8} {:<4} {}",
            s(&w, "id"),
            s(&w, "deployment"),
            s(&w, "status"),
            s(&w, "trigger"),
            w.get("attempts").and_then(|v| v.as_i64()).unwrap_or(0),
            s(&w, "created_at"),
        );
    }
    Ok(())
}

pub async fn logs(host: &str, id: &str, follow: bool) -> Result<()> {
    crate::daemon::ensure(host).await?;
    let client = Client::new(host);
    if follow {
        client
            .stream(id, |env| println!("{}", format_envelope(&env)))
            .await?;
    } else {
        let mut envs: Vec<Envelope> = Vec::new();
        for e in client.get_events(id).await? {
            envs.push(Envelope {
                workflow_id: id.to_string(),
                seq: e.get("seq").and_then(|v| v.as_i64()).unwrap_or(0),
                ts: s(&e, "ts"),
                kind: "event".into(),
                payload: e.get("payload").cloned().unwrap_or(Value::Null),
            });
        }
        for l in client.get_logs(id).await? {
            envs.push(Envelope {
                workflow_id: id.to_string(),
                seq: 0,
                ts: s(&l, "ts"),
                kind: "log".into(),
                payload: l,
            });
        }
        envs.sort_by(|a, b| a.ts.cmp(&b.ts).then(a.seq.cmp(&b.seq)));
        for env in &envs {
            println!("{}", format_envelope(env));
        }
    }
    Ok(())
}

/// One-shot: no daemon. Ephemeral in-memory store + worker, live-streamed.
pub async fn run_oneshot(flows_path: &str, name: &str, params: Value) -> Result<()> {
    let store = Store::open(":memory:")?;
    let manifest = load_manifest(flows_path)?;
    let dep = manifest
        .flows
        .into_iter()
        .find(|d| d.name == name)
        .ok_or_else(|| anyhow!("no workflow named '{name}' in {flows_path}"))?;
    store.upsert_deployment(&dep)?;

    let (tx, mut rx) = broadcast::channel::<Envelope>(4096);
    let root = std::fs::canonicalize(std::env::current_dir()?).unwrap_or(PathBuf::from("."));
    let worker = Worker::new(store.clone(), tx, root);

    println!("running '{name}' (one-shot)…\n");
    let workflow_id = worker.trigger(dep, "manual", params, None)?;

    loop {
        tokio::select! {
            r = rx.recv() => match r {
                Ok(env) if env.workflow_id == workflow_id => {
                    println!("{}", format_envelope(&env));
                    let terminal = env.kind == "event"
                        && matches!(
                            env.payload.get("type").and_then(|v| v.as_str()),
                            Some("workflow_success") | Some("workflow_failure")
                        );
                    if terminal { break; }
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            },
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                if let Ok(Some(w)) = store.get_workflow(&workflow_id) {
                    if w.status != "running" { break; }
                }
            }
        }
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let wf = store
        .get_workflow(&workflow_id)?
        .ok_or_else(|| anyhow!("workflow vanished"))?;
    let steps = store.list_steps(&workflow_id)?;
    println!("\n── summary ─────────────────────────");
    println!(
        "workflow {}  status={}  attempts={}",
        wf.id, wf.status, wf.attempts
    );
    for st in steps {
        let dur = st
            .duration_ms
            .map(|d| format!("{d}ms"))
            .unwrap_or_else(|| "—".into());
        let tag = if st.status == "skipped" {
            " (resumed from journal)"
        } else {
            ""
        };
        println!(
            "  #{:<3} {:<16} {:<9} {}{}",
            st.seq, st.name, st.status, dur, tag
        );
    }
    if wf.status != "success" {
        std::process::exit(1);
    }
    Ok(())
}

pub fn format_envelope(env: &Envelope) -> String {
    if env.kind == "log" {
        let stream = env
            .payload
            .get("stream")
            .and_then(|v| v.as_str())
            .unwrap_or("out");
        let line = env
            .payload
            .get("line")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        return format!("  │ [{stream}] {line}");
    }
    let ty = env
        .payload
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("event");
    let p = &env.payload;
    match ty {
        "workflow_start" => "▶ workflow start".to_string(),
        "workflow_success" => "✔ workflow SUCCESS".to_string(),
        "workflow_failure" => format!("✗ workflow FAILURE: {}", ps(p, "error")),
        "workflow_resume" => format!("↻ resume (attempt {})", pi(p, "attempt")),
        "workflow_recovered" => "⟳ recovered — resuming after daemon restart".to_string(),
        "workflow_park" => match p.get("wake_at").and_then(|v| v.as_f64()) {
            Some(_) => "⏸ parked (sleeping) — process unloaded".to_string(),
            None => "⏸ parked (waiting for signal) — process unloaded".to_string(),
        },
        "workflow_wake" => "⏰ woke — re-dispatched at wake time".to_string(),
        "workflow_signalled" => "✉ signal received — re-dispatched".to_string(),
        "step_start" => format!(
            "  ● #{} {} start (attempt {})",
            pi(p, "seq"),
            name_of(p),
            pi(p, "attempt")
        ),
        "step_success" => format!(
            "  ✔ #{} {} ok ({}ms)",
            pi(p, "seq"),
            name_of(p),
            pi(p, "duration_ms")
        ),
        "step_skipped" => format!(
            "  ⤼ #{} {} skipped ({})",
            pi(p, "seq"),
            name_of(p),
            ps(p, "reason")
        ),
        "step_retry" => format!(
            "  ↻ #{} {} retry -> attempt {} ({})",
            pi(p, "seq"),
            name_of(p),
            pi(p, "next_attempt"),
            ps(p, "error")
        ),
        "step_failure" => format!(
            "  ✗ #{} {} failed: {}",
            pi(p, "seq"),
            name_of(p),
            ps(p, "error")
        ),
        "log" => format!("  │ {}", ps(p, "message")),
        other => format!("  · {other}"),
    }
}

fn name_of(v: &Value) -> String {
    v.get("name")
        .or_else(|| v.get("task"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string()
}
fn s(v: &Value, k: &str) -> String {
    v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string()
}
fn ps(v: &Value, k: &str) -> String {
    v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string()
}
fn pi(v: &Value, k: &str) -> i64 {
    v.get(k).and_then(|x| x.as_i64()).unwrap_or(0)
}
