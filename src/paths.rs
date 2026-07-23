//! Everything lives under `~/.config/shift-clock/` (honoring `$XDG_CONFIG_HOME`):
//!   flows.toml            the manifest
//!   flows/                flow scripts
//!   sdk/{python,typescript}/  the bundled SDKs (embedded in the binary, written
//!                             out on scaffold so they always match this version)
//!   shift-clock.db        durable state
//!   daemon-<port>.{pid,log}
//!
//! The binary is self-contained: `shift-clock init` (also run automatically by
//! `serve`) scaffolds the config dir so `shift-clock` works from anywhere.

use std::io;
use std::path::PathBuf;

const SDK_PY: &str = include_str!("../sdk/python/shift_clock.py");
const SDK_TS: &str = include_str!("../sdk/typescript/shift_clock.mjs");

const SAMPLE_MANIFEST: &str = r#"# shift-clock flows — edit me. Each [[flow]] is a schedulable workflow.
# Docs: a flow is just a command; add `cron`, `params`, `retries`, `concurrency`.

[[flow]]
name = "hello"
cmd = ["bash", "-c", "echo hello from shift-clock; date"]

[[flow]]
name = "example"
cmd = ["python3", "flows/example.py"]
cron = "*/5 * * * *"
"#;

const SAMPLE_FLOW: &str = r#"import time

from shift_clock import workflow, step, log


@step()
def work():
    time.sleep(0.2)
    return {"ok": True}


@workflow
def main():
    log("hello from an SDK workflow")
    work()


if __name__ == "__main__":
    main()
"#;

pub fn config_dir() -> PathBuf {
    let base = std::env::var("XDG_CONFIG_HOME")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
            PathBuf::from(home).join(".config")
        });
    let d = base.join("shift-clock");
    let _ = std::fs::create_dir_all(&d);
    d
}

pub fn default_manifest() -> PathBuf {
    config_dir().join("flows.toml")
}
pub fn default_db() -> PathBuf {
    config_dir().join("shift-clock.db")
}
pub fn pid_file(port: &str) -> PathBuf {
    config_dir().join(format!("daemon-{port}.pid"))
}
pub fn log_file(port: &str) -> PathBuf {
    config_dir().join(format!("daemon-{port}.log"))
}

/// Write the bundled SDKs (always, to stay in sync with the binary) and a sample
/// manifest + flow (only if no manifest exists yet). Returns the config dir.
pub fn ensure_scaffold() -> io::Result<PathBuf> {
    let dir = config_dir();

    let py = dir.join("sdk/python");
    std::fs::create_dir_all(&py)?;
    std::fs::write(py.join("shift_clock.py"), SDK_PY)?;

    let ts = dir.join("sdk/typescript");
    std::fs::create_dir_all(&ts)?;
    std::fs::write(ts.join("shift_clock.mjs"), SDK_TS)?;

    // Node ESM: mark the sdk dir as module so `.mjs` imports are unambiguous.
    let _ = std::fs::write(dir.join("package.json"), "{\"type\":\"module\"}\n");

    let manifest = default_manifest();
    if !manifest.exists() {
        std::fs::create_dir_all(dir.join("flows"))?;
        std::fs::write(dir.join("flows/example.py"), SAMPLE_FLOW)?;
        std::fs::write(&manifest, SAMPLE_MANIFEST)?;
    }
    Ok(dir)
}
