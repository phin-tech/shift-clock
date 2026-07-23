//! `shift-clock serve`: load the manifest into the deployments table, start the
//! worker + scheduler + HTTP control plane, and run until Ctrl-C.

use crate::api::{router, AppState};
use crate::config::load_manifest;
use crate::protocol::Envelope;
use crate::store::Store;
use crate::worker::Worker;
use anyhow::Result;
use std::path::PathBuf;
use tokio::sync::broadcast;

pub async fn serve(
    db: Option<String>,
    flows: Option<String>,
    addr: String,
    attach: bool,
) -> Result<()> {
    // `--attach` with a daemon already up → just attach the dashboard to it.
    if attach && crate::daemon::is_up(&addr).await {
        println!("[serve] a daemon is already running on {addr}; attaching dashboard");
        return crate::tui::run(&addr).await;
    }

    // Make the config dir self-contained (writes the bundled SDKs + a sample).
    let _ = crate::paths::ensure_scaffold();

    // Resolve paths: explicit flags win, else the ~/.config/shift-clock defaults.
    let db = db.unwrap_or_else(|| crate::paths::default_db().display().to_string());
    let flows_path =
        flows.unwrap_or_else(|| crate::paths::default_manifest().display().to_string());

    crate::daemon::record_self(&addr); // so `shift-clock stop/status` can find us
    let store = Store::open(&db)?;

    // The manifest is one writer into the deployments table.
    let manifest = load_manifest(&flows_path)?;
    for dep in &manifest.flows {
        store.upsert_deployment(dep)?;
    }
    println!(
        "[serve] loaded {} flow(s) from {flows_path}",
        manifest.flows.len()
    );

    // Flows resolve relative to the manifest's directory (so `flows/x.py` and the
    // sibling `sdk/` work wherever the manifest lives — not tied to the cwd).
    let root = PathBuf::from(&flows_path)
        .parent()
        .map(|p| p.to_path_buf())
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| PathBuf::from("."));
    let root: PathBuf = std::fs::canonicalize(&root).unwrap_or(root);

    let (tx, _rx) = broadcast::channel::<Envelope>(4096);
    let worker = Worker::new(store.clone(), tx, root);

    // Durable recovery: resume any workflow left running by a previous daemon.
    match worker.recover() {
        Ok(0) => {}
        Ok(n) => println!("[serve] recovered {n} interrupted workflow(s)"),
        Err(e) => eprintln!("[serve] recovery failed: {e:#}"),
    }

    // Scheduler runs in the background.
    tokio::spawn(crate::scheduler::run(worker.clone()));

    let state = AppState { store, worker };
    let app = router(state);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("[serve] control plane on http://{addr}");

    if attach {
        // Foreground: run the daemon in-process AND the dashboard. Quitting the
        // TUI (q) drops this process, which stops the daemon.
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        // Wait for the server to be ready so the TUI doesn't try to auto-spawn.
        for _ in 0..40 {
            if crate::daemon::is_up(&addr).await {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        crate::tui::run(&addr).await?;
        println!("[serve] dashboard closed — stopping foreground daemon");
        return Ok(());
    }

    println!("[serve] try:  shift-clock dashboard   |   shift-clock trigger <flow>");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            println!("\n[serve] shutting down");
        })
        .await?;
    Ok(())
}
