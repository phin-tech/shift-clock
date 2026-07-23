//! Thin HTTP/SSE client used by the CLI and the TUI. Every non-serve subcommand
//! is just a client of the running control plane — the same API a remote client
//! would use.

use crate::protocol::Envelope;
use anyhow::{anyhow, Result};
use futures::StreamExt;
use serde_json::{json, Value};

#[derive(Clone)]
pub struct Client {
    base: String,
    http: reqwest::Client,
}

impl Client {
    pub fn new(host: &str) -> Client {
        let base = if host.starts_with("http") {
            host.trim_end_matches('/').to_string()
        } else {
            format!("http://{}", host.trim_end_matches('/'))
        };
        Client {
            base,
            http: reqwest::Client::new(),
        }
    }

    pub async fn trigger(&self, name: &str, params: Value, id: Option<&str>) -> Result<String> {
        let url = format!("{}/flows/{}/trigger", self.base, name);
        let mut body = json!({ "params": params });
        if let Some(id) = id {
            body["id"] = json!(id);
        }
        let resp = self.http.post(&url).json(&body).send().await?;
        let status = resp.status();
        let body: Value = resp.json().await?;
        if !status.is_success() {
            return Err(anyhow!(
                "{}",
                body.get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("trigger failed")
            ));
        }
        Ok(body["workflow_id"].as_str().unwrap_or_default().to_string())
    }

    pub async fn signal(&self, id: &str, name: &str, payload: Value) -> Result<()> {
        let url = format!("{}/workflows/{}/signals", self.base, id);
        let resp = self
            .http
            .post(&url)
            .json(&json!({ "name": name, "payload": payload }))
            .send()
            .await?;
        if !resp.status().is_success() {
            let body: Value = resp.json().await.unwrap_or(Value::Null);
            return Err(anyhow!(
                "{}",
                body.get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("signal failed")
            ));
        }
        Ok(())
    }

    pub async fn list_flows(&self) -> Result<Vec<Value>> {
        let url = format!("{}/flows", self.base);
        let body: Value = self.http.get(&url).send().await?.json().await?;
        Ok(body["flows"].as_array().cloned().unwrap_or_default())
    }

    pub async fn list_workflows(&self, limit: i64) -> Result<Vec<Value>> {
        let url = format!("{}/workflows?limit={}", self.base, limit);
        let body: Value = self.http.get(&url).send().await?.json().await?;
        Ok(body["workflows"].as_array().cloned().unwrap_or_default())
    }

    pub async fn get_workflow(&self, id: &str) -> Result<Value> {
        let url = format!("{}/workflows/{}", self.base, id);
        Ok(self.http.get(&url).send().await?.json().await?)
    }

    pub async fn get_logs(&self, id: &str) -> Result<Vec<Value>> {
        let url = format!("{}/workflows/{}/logs", self.base, id);
        let body: Value = self.http.get(&url).send().await?.json().await?;
        Ok(body["logs"].as_array().cloned().unwrap_or_default())
    }

    pub async fn query(&self, id: &str) -> Result<Value> {
        let url = format!("{}/workflows/{}/state", self.base, id);
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            let body: Value = resp.json().await.unwrap_or(Value::Null);
            return Err(anyhow!(
                "{}",
                body.get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("query failed")
            ));
        }
        let body: Value = resp.json().await?;
        Ok(body["state"].clone())
    }

    pub async fn get_events(&self, id: &str) -> Result<Vec<Value>> {
        let url = format!("{}/workflows/{}/events", self.base, id);
        let body: Value = self.http.get(&url).send().await?.json().await?;
        Ok(body["events"].as_array().cloned().unwrap_or_default())
    }

    /// Stream envelopes for a workflow over SSE, invoking `on_env` for each.
    pub async fn stream<F: FnMut(Envelope)>(&self, id: &str, mut on_env: F) -> Result<()> {
        let url = format!("{}/workflows/{}/stream", self.base, id);
        let resp = self.http.get(&url).send().await?;
        let mut bytes = resp.bytes_stream();
        let mut buf = String::new();
        while let Some(chunk) = bytes.next().await {
            let chunk = chunk?;
            buf.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(pos) = buf.find("\n\n") {
                let frame = buf[..pos].to_string();
                buf.drain(..pos + 2);
                let mut data = String::new();
                for line in frame.lines() {
                    if let Some(rest) = line.strip_prefix("data:") {
                        data.push_str(rest.trim_start());
                    }
                }
                if data.is_empty() {
                    continue;
                }
                if let Ok(env) = serde_json::from_str::<Envelope>(&data) {
                    on_env(env);
                }
            }
        }
        Ok(())
    }
}
