//! The control-plane HTTP API. One JSON+SSE surface shared by the CLI, the TUI,
//! and (later) a web UI or a remote client.

use crate::protocol::Envelope;
use crate::store::Store;
use crate::worker::Worker;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::sse::{Event, Sse},
    routing::{get, post},
    Json, Router,
};
use futures::stream::{self, Stream, StreamExt};
use serde_json::{json, Value};
use std::convert::Infallible;
use tokio_stream::wrappers::BroadcastStream;

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub worker: Worker,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/flows", get(list_flows))
        .route("/flows/:name/trigger", post(trigger_flow))
        .route("/workflows", get(list_workflows))
        .route("/workflows/:id", get(get_workflow))
        .route("/workflows/:id/events", get(get_events))
        .route("/workflows/:id/logs", get(get_logs))
        .route("/workflows/:id/stream", get(stream_workflow))
        .route("/workflows/:id/signals", post(send_signal))
        .route("/workflows/:id/state", get(query_state))
        .with_state(state)
}

/// Query a workflow's durable state — works whether it's running, parked, or done.
async fn query_state(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if s.store.get_workflow(&id)?.is_none() {
        return Err(ApiError::not_found(format!("no such workflow '{id}'")));
    }
    let state = s.store.get_state(&id)?;
    Ok(Json(json!({ "state": state })))
}

async fn list_flows(State(s): State<AppState>) -> Result<Json<Value>, ApiError> {
    let flows = s.store.list_deployments()?;
    Ok(Json(json!({ "flows": flows })))
}

#[derive(serde::Deserialize)]
struct TriggerBody {
    #[serde(default)]
    params: Value,
    /// Optional idempotency key: same id returns the existing workflow.
    #[serde(default)]
    id: Option<String>,
}

async fn trigger_flow(
    State(s): State<AppState>,
    Path(name): Path<String>,
    body: Option<Json<TriggerBody>>,
) -> Result<Json<Value>, ApiError> {
    let dep = s
        .store
        .get_deployment(&name)?
        .ok_or_else(|| ApiError::not_found(format!("no such flow '{name}'")))?;
    let (extra, id) = body
        .map(|b| (b.0.params, b.0.id))
        .unwrap_or((Value::Null, None));
    let workflow_id = s
        .worker
        .trigger(dep, "manual", extra, id)
        .map_err(|e| ApiError::conflict(e.to_string()))?;
    Ok(Json(json!({ "workflow_id": workflow_id })))
}

#[derive(serde::Deserialize)]
struct SignalBody {
    name: String,
    #[serde(default)]
    payload: Value,
}

async fn send_signal(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<SignalBody>,
) -> Result<Json<Value>, ApiError> {
    if s.store.get_workflow(&id)?.is_none() {
        return Err(ApiError::not_found(format!("no such workflow '{id}'")));
    }
    s.store.add_signal(&id, &body.name, &body.payload)?;
    s.worker.notify_signal(&id); // wake it if parked-waiting
    Ok(Json(json!({ "ok": true })))
}

#[derive(serde::Deserialize)]
struct ListQuery {
    #[serde(default = "default_limit")]
    limit: i64,
}
fn default_limit() -> i64 {
    50
}

async fn list_workflows(
    State(s): State<AppState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    let workflows = s.store.list_workflows(q.limit)?;
    Ok(Json(json!({ "workflows": workflows })))
}

async fn get_workflow(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let wf = s
        .store
        .get_workflow(&id)?
        .ok_or_else(|| ApiError::not_found(format!("no such workflow '{id}'")))?;
    let steps = s.store.list_steps(&id)?;
    Ok(Json(json!({ "workflow": wf, "steps": steps })))
}

async fn get_events(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let events = s.store.get_events(&id)?;
    let events: Vec<Value> = events
        .into_iter()
        .map(|(seq, ts, ty, payload)| json!({ "seq": seq, "ts": ts, "type": ty, "payload": payload }))
        .collect();
    Ok(Json(json!({ "events": events })))
}

async fn get_logs(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let logs = s.store.get_logs(&id)?;
    Ok(Json(json!({ "logs": logs })))
}

/// Backlog (persisted) then live (broadcast), filtered to this workflow.
async fn stream_workflow(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut backlog: Vec<Envelope> = Vec::new();
    if let Ok(events) = s.store.get_events(&id) {
        for (seq, ts, _ty, payload) in events {
            backlog.push(Envelope {
                workflow_id: id.clone(),
                seq,
                ts,
                kind: "event".into(),
                payload,
            });
        }
    }
    if let Ok(logs) = s.store.get_logs(&id) {
        for l in logs {
            backlog.push(Envelope {
                workflow_id: id.clone(),
                seq: 0,
                ts: l.ts,
                kind: "log".into(),
                payload: json!({ "stream": l.stream, "line": l.line }),
            });
        }
    }
    backlog.sort_by_key(|e| e.seq);

    let rx = s.worker.tx.subscribe();
    let id_live = id.clone();
    let live = BroadcastStream::new(rx).filter_map(move |res| {
        let id = id_live.clone();
        async move {
            match res {
                Ok(env) if env.workflow_id == id => Some(env),
                _ => None,
            }
        }
    });

    let combined = stream::iter(backlog).chain(live).map(|env| {
        Ok(Event::default()
            .json_data(&env)
            .unwrap_or_else(|_| Event::default().data("{}")))
    });

    Sse::new(combined)
}

// -- error plumbing ---------------------------------------------------------

pub struct ApiError {
    code: StatusCode,
    msg: String,
}
impl ApiError {
    fn not_found(msg: String) -> Self {
        ApiError {
            code: StatusCode::NOT_FOUND,
            msg,
        }
    }
    fn conflict(msg: String) -> Self {
        ApiError {
            code: StatusCode::CONFLICT,
            msg,
        }
    }
}
impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        ApiError {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: e.to_string(),
        }
    }
}
impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.code, Json(json!({ "error": self.msg }))).into_response()
    }
}
