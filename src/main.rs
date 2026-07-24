//! shift-clock: a local, language-agnostic Prefect. A single binary that
//! schedules and supervises flows written in any language, talks to them over a
//! local Unix socket, and exposes one HTTP/SSE API that the CLI and TUI consume.

mod api;
mod cli;
mod client;
mod config;
mod daemon;
mod paths;
mod protocol;
mod scheduler;
mod server;
mod store;
mod tui;
mod worker;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "shift-clock",
    version,
    about = "Local, language-agnostic flow orchestrator"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Scaffold ~/.config/shift-clock (SDKs + a sample manifest).
    Init,
    /// Run the daemon: scheduler + worker + HTTP control plane.
    Serve {
        /// SQLite path (default: ~/.config/shift-clock/shift-clock.db).
        #[arg(long)]
        db: Option<String>,
        /// Manifest path (default: ~/.config/shift-clock/flows.toml).
        #[arg(long)]
        flows: Option<String>,
        #[arg(long, default_value = "127.0.0.1:8080")]
        addr: String,
        /// Foreground: also open the dashboard; quitting it stops the daemon.
        #[arg(long)]
        attach: bool,
    },
    /// Trigger a flow on a running daemon.
    Trigger {
        name: String,
        #[arg(long = "param", value_name = "KEY=VALUE")]
        params: Vec<String>,
        /// Idempotency key: re-triggering with the same id returns the existing workflow.
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        host: Option<String>,
    },
    /// Send a signal to a (possibly parked) workflow.
    Signal {
        workflow_id: String,
        name: String,
        #[arg(default_value = "null")]
        payload: String,
        #[arg(long)]
        host: Option<String>,
    },
    /// Show a workflow's status and step journal.
    Show {
        workflow_id: String,
        #[arg(long)]
        host: Option<String>,
    },
    /// Query a workflow's durable state (optionally one key).
    Query {
        workflow_id: String,
        key: Option<String>,
        #[arg(long)]
        host: Option<String>,
    },
    /// Show whether the background daemon is running.
    Status {
        #[arg(long)]
        host: Option<String>,
    },
    /// Stop the background daemon.
    Stop {
        #[arg(long)]
        host: Option<String>,
    },
    /// Run one flow once, in-process, with no daemon (live-streamed).
    Run {
        name: String,
        #[arg(long, default_value = "flows.toml")]
        flows: String,
        #[arg(long = "param", value_name = "KEY=VALUE")]
        params: Vec<String>,
    },
    /// List recent workflows from a running daemon.
    Workflows {
        #[arg(long, default_value = "50")]
        limit: i64,
        #[arg(long)]
        host: Option<String>,
    },
    /// Show (or follow) a workflow's logs/events.
    Logs {
        workflow_id: String,
        #[arg(short, long)]
        follow: bool,
        #[arg(long)]
        host: Option<String>,
    },
    /// Launch the TUI dashboard against a running daemon.
    Dashboard {
        #[arg(long)]
        host: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        // Bare `shift-clock` → open the dashboard (auto-spawns the daemon).
        None => tui::run(&daemon::resolve_host(None)).await,
        Some(Cmd::Init) => {
            let dir = paths::ensure_scaffold()?;
            println!("scaffolded {}", dir.display());
            Ok(())
        }
        Some(Cmd::Serve {
            db,
            flows,
            addr,
            attach,
        }) => server::serve(db, flows, addr, attach).await,
        Some(Cmd::Trigger {
            name,
            params,
            id,
            host,
        }) => {
            let host = daemon::resolve_host(host);
            cli::trigger(&host, &name, cli::parse_params(&params), id).await
        }
        Some(Cmd::Signal {
            workflow_id,
            name,
            payload,
            host,
        }) => {
            let host = daemon::resolve_host(host);
            let payload =
                serde_json::from_str(&payload).unwrap_or(serde_json::Value::String(payload));
            cli::signal(&host, &workflow_id, &name, payload).await
        }
        Some(Cmd::Show { workflow_id, host }) => {
            cli::show(&daemon::resolve_host(host), &workflow_id).await
        }
        Some(Cmd::Query {
            workflow_id,
            key,
            host,
        }) => cli::query(&daemon::resolve_host(host), &workflow_id, key).await,
        Some(Cmd::Status { host }) => daemon::status(&daemon::resolve_host(host)).await,
        Some(Cmd::Stop { host }) => daemon::stop(&daemon::resolve_host(host)),
        Some(Cmd::Run {
            name,
            flows,
            params,
        }) => cli::run_oneshot(&flows, &name, cli::parse_params(&params)).await,
        Some(Cmd::Workflows { limit, host }) => {
            cli::workflows(&daemon::resolve_host(host), limit).await
        }
        Some(Cmd::Logs {
            workflow_id,
            follow,
            host,
        }) => cli::logs(&daemon::resolve_host(host), &workflow_id, follow).await,
        Some(Cmd::Dashboard { host }) => tui::run(&daemon::resolve_host(host)).await,
    }
}
