//! Herdr/tmux-style lifecycle: clients lazily spawn a background daemon if none
//! is reachable, so there's no install step — the first `trigger`/`dashboard`
//! brings the server up and it persists on its own.
//!
//! Transport stays HTTP (unlike Herdr's Unix socket — we don't pass FDs). The
//! daemonization is Herdr's: re-exec `serve` with stdio → /dev/null and
//! `setsid()` in a pre_exec hook so it detaches from the controlling terminal
//! and survives the client's terminal closing.

use anyhow::{anyhow, Result};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

fn port_of(host: &str) -> String {
    host.rsplit(':').next().unwrap_or("8080").to_string()
}
fn pid_file(host: &str) -> PathBuf {
    crate::paths::pid_file(&port_of(host))
}
fn log_file(host: &str) -> PathBuf {
    crate::paths::log_file(&port_of(host))
}

fn addr_of(host: &str) -> String {
    host.trim_start_matches("http://").trim_start_matches("https://").trim_end_matches('/').to_string()
}

/// Only local daemons can be auto-spawned; a remote host must already be up.
pub fn is_local(host: &str) -> bool {
    let hostname = addr_of(host);
    let hostname = hostname.split(':').next().unwrap_or("");
    matches!(hostname, "127.0.0.1" | "localhost" | "::1" | "0.0.0.0" | "")
}

/// Is a daemon reachable at `host`?
pub async fn is_up(host: &str) -> bool {
    health_ok(host).await
}

async fn health_ok(host: &str) -> bool {
    let url = format!("http://{}/health", addr_of(host));
    let client = match reqwest::Client::builder().timeout(Duration::from_millis(600)).build() {
        Ok(c) => c,
        Err(_) => return false,
    };
    matches!(client.get(&url).send().await, Ok(r) if r.status().is_success())
}

/// Ensure a daemon is reachable, spawning a detached local one if needed.
pub async fn ensure(host: &str) -> Result<()> {
    if health_ok(host).await {
        return Ok(());
    }
    if !is_local(host) {
        return Err(anyhow!(
            "no shift-clock daemon reachable at {host} (remote — start `shift-clock serve` there)"
        ));
    }
    spawn_detached(host)?;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if health_ok(host).await {
            eprintln!("[shift-clock] started background daemon on {}", addr_of(host));
            return Ok(());
        }
    }
    Err(anyhow!("daemon did not become ready; see {}", log_file(host).display()))
}

fn spawn_detached(host: &str) -> Result<()> {
    let exe = std::env::current_exe()?;
    let log = std::fs::OpenOptions::new().create(true).append(true).open(log_file(host))?;
    let log2 = log.try_clone()?;
    let mut cmd = std::process::Command::new(exe);
    cmd.arg("serve")
        .arg("--addr")
        .arg(addr_of(host))
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log2));
    // Detach: new session (no controlling terminal) so it outlives this client.
    #[cfg(unix)]
    unsafe {
        use std::os::unix::process::CommandExt;
        cmd.pre_exec(|| {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = cmd.spawn()?;
    let _ = std::fs::write(pid_file(host), child.id().to_string());
    Ok(())
}

/// Called by `serve` on startup so a directly-run daemon is also discoverable.
pub fn record_self(host: &str) {
    let _ = std::fs::write(pid_file(host), std::process::id().to_string());
}

pub async fn status(host: &str) -> Result<()> {
    let up = health_ok(host).await;
    println!("daemon @ {}: {}", addr_of(host), if up { "RUNNING" } else { "not running" });
    if let Ok(pid) = std::fs::read_to_string(pid_file(host)) {
        println!("  pid: {}", pid.trim());
    }
    println!("  log: {}", log_file(host).display());
    Ok(())
}

pub fn stop(host: &str) -> Result<()> {
    let path = pid_file(host);
    let pid: i32 = std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| anyhow!("no daemon pid file at {}", path.display()))?;
    #[cfg(unix)]
    unsafe {
        libc::kill(pid, libc::SIGTERM);
    }
    let _ = std::fs::remove_file(&path);
    println!("stopped daemon (pid {pid})");
    Ok(())
}
