//! The cron scheduler. Ticks once a second, evaluates each enabled deployment's
//! expression against *now*, and triggers via the worker. Because it only ever
//! looks at the current minute, occurrences missed while the process was down
//! (laptop asleep) are simply never seen — that is `catchup = none` for free.
//! A per-deployment "last fired minute" guard prevents double-firing within the
//! same minute across the sub-minute ticks.

use crate::config::Cron;
use crate::worker::Worker;
use chrono::{Local, Timelike};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

pub async fn run(worker: Worker) {
    let mut last_fired: HashMap<String, String> = HashMap::new();
    loop {
        // Phase 4: re-dispatch timer-parked workflows whose wake time has come.
        worker.resume_due();

        let now = Local::now();
        let minute_key = format!("{}", now.format("%Y-%m-%dT%H:%M"));

        let deployments = match worker.store.list_deployments() {
            Ok(d) => d,
            Err(e) => {
                eprintln!("[scheduler] list_deployments failed: {e:#}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        for dep in deployments {
            if !dep.enabled {
                continue;
            }
            let Some(expr) = dep.cron.clone() else {
                continue;
            };
            let cron = match Cron::parse(&expr) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[scheduler] '{}' bad cron '{expr}': {e}", dep.name);
                    continue;
                }
            };
            if !cron.matches(&now) {
                continue;
            }
            // Already fired this deployment during this wall-clock minute?
            if last_fired.get(&dep.name) == Some(&minute_key) {
                continue;
            }
            last_fired.insert(dep.name.clone(), minute_key.clone());

            if dep.overlap == "skip" && worker.is_active(&dep.name) {
                eprintln!("[scheduler] skip '{}': previous run still active", dep.name);
                continue;
            }
            match worker.trigger(dep.clone(), "cron", Value::Null, None) {
                Ok(run_id) => eprintln!("[scheduler] fired '{}' -> {run_id}", dep.name),
                Err(e) => eprintln!("[scheduler] trigger '{}' failed: {e}", dep.name),
            }
        }

        // Align roughly to the next second boundary.
        let sub = now.nanosecond() as u64;
        let to_next = 1_000_000_000u64.saturating_sub(sub % 1_000_000_000);
        tokio::time::sleep(Duration::from_nanos(to_next.max(50_000_000))).await;
    }
}
