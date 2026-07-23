//! Flow definitions (the static manifest) + a small 5-field cron matcher.
//!
//! A "deployment" is `name + cmd + schedule + params + policies`. The manifest
//! (`flows.toml`) is just one writer into the deployments table; later, dynamic
//! self-registration would be another writer with no change to this model.

use anyhow::{anyhow, Result};
use chrono::{DateTime, Datelike, Local, Timelike};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

fn default_catchup() -> String {
    "none".into()
}
fn default_overlap() -> String {
    "skip".into()
}
fn default_true() -> bool {
    true
}

/// One schedulable unit. Deserialized from `flows.toml` and persisted verbatim.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Deployment {
    pub name: String,
    /// The command to run, argv-style. A flow is minimally *just a command* —
    /// e.g. `["claude", "-p", "triage inbox"]` or `["python3", "flows/etl.py"]`.
    pub cmd: Vec<String>,
    #[serde(default)]
    pub cron: Option<String>,
    #[serde(default)]
    pub timezone: Option<String>,
    /// Default params handed to the flow (JSON object) via the socket handshake
    /// (SDK flows) and `SHIFT_CLOCK_PARAMS` / `SC_PARAM_*` env (bare flows).
    #[serde(default)]
    pub params: Map<String, Value>,
    /// Missed-run policy: "none" (default) | "once". "all"/backfill not in POC.
    #[serde(default = "default_catchup")]
    pub catchup: String,
    /// Overlap policy: "skip" (default) | "queue" | "concurrent".
    #[serde(default = "default_overlap")]
    pub overlap: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// How many times the worker re-dispatches the workflow after a *crash*
    /// (not an explicit workflow_failure). The step journal makes each
    /// re-dispatch cheap — completed steps are skipped.
    #[serde(default)]
    pub retries: u32,
    /// Max concurrent workflows for this deployment (0 = unlimited). Phase 4.
    #[serde(default)]
    pub concurrency: u32,
}

#[derive(Deserialize)]
pub struct Manifest {
    #[serde(rename = "flow", default)]
    pub flows: Vec<Deployment>,
}

pub fn load_manifest(path: &str) -> Result<Manifest> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| anyhow!("reading manifest {path}: {e}"))?;
    let m: Manifest = toml::from_str(&text)?;
    Ok(m)
}

// ---------------------------------------------------------------------------
// Minimal 5-field cron: `minute hour day-of-month month day-of-week`
// Supports: `*`, `*/n`, `a`, `a-b`, `a-b/n`, and comma lists of those.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Cron {
    minute: Field,
    hour: Field,
    dom: Field,
    month: Field,
    dow: Field,
}

#[derive(Clone, Debug)]
struct Field {
    // Sorted, deduped set of allowed values. Empty means "*" (any).
    allowed: Vec<u32>,
    any: bool,
}

impl Cron {
    pub fn parse(expr: &str) -> Result<Cron> {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() != 5 {
            return Err(anyhow!(
                "cron '{expr}' must have 5 fields (min hour dom month dow), got {}",
                parts.len()
            ));
        }
        Ok(Cron {
            minute: Field::parse(parts[0], 0, 59)?,
            hour: Field::parse(parts[1], 0, 23)?,
            dom: Field::parse(parts[2], 1, 31)?,
            month: Field::parse(parts[3], 1, 12)?,
            dow: Field::parse(parts[4], 0, 6)?,
        })
    }

    /// The next firing at or after `from` (scanning minute by minute, capped at
    /// a year). Used by the TUI to show "next fire" for each scheduled job.
    pub fn next_after(&self, from: &DateTime<Local>) -> Option<DateTime<Local>> {
        use chrono::Duration;
        // Start at the next whole minute so we don't return `from` itself.
        let mut t = (*from + Duration::minutes(1))
            .with_second(0)?
            .with_nanosecond(0)?;
        for _ in 0..(366 * 24 * 60) {
            if self.matches(&t) {
                return Some(t);
            }
            t += Duration::minutes(1);
        }
        None
    }

    /// Does this expression fire at the given local wall-clock minute?
    pub fn matches(&self, dt: &DateTime<Local>) -> bool {
        // cron day-of-week: 0 and 7 are both Sunday; chrono weekday 0 = Monday.
        let dow = dt.weekday().num_days_from_sunday(); // 0=Sun .. 6=Sat
        self.minute.contains(dt.minute())
            && self.hour.contains(dt.hour())
            && self.dom.contains(dt.day())
            && self.month.contains(dt.month())
            && self.dow.contains(dow)
    }
}

impl Field {
    fn parse(spec: &str, min: u32, max: u32) -> Result<Field> {
        if spec == "*" {
            return Ok(Field {
                allowed: vec![],
                any: true,
            });
        }
        let mut allowed = Vec::new();
        for part in spec.split(',') {
            // Split optional step: "range/step"
            let (range, step) = match part.split_once('/') {
                Some((r, s)) => (r, s.parse::<u32>().map_err(|_| anyhow!("bad step '{s}'"))?),
                None => (part, 1),
            };
            let (lo, hi) = if range == "*" {
                (min, max)
            } else if let Some((a, b)) = range.split_once('-') {
                (
                    a.parse::<u32>().map_err(|_| anyhow!("bad range '{range}'"))?,
                    b.parse::<u32>().map_err(|_| anyhow!("bad range '{range}'"))?,
                )
            } else {
                let v = range.parse::<u32>().map_err(|_| anyhow!("bad value '{range}'"))?;
                (v, v)
            };
            if step == 0 {
                return Err(anyhow!("step must be > 0"));
            }
            let mut v = lo;
            while v <= hi {
                if v >= min && v <= max {
                    allowed.push(v);
                }
                v += step;
            }
        }
        allowed.sort_unstable();
        allowed.dedup();
        Ok(Field {
            allowed,
            any: false,
        })
    }

    fn contains(&self, v: u32) -> bool {
        // Normalize cron Sunday-as-7 into 0 for the dow field via the caller;
        // here we just check membership.
        self.any || self.allowed.binary_search(&v).is_ok()
    }
}
