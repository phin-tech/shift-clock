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
#[command(name = "shift-clock", version, about = "Local, language-agnostic flow orchestrator")]
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
        #[arg(long, default_value = "127.0.0.1:8080")]
        host: String,
    },
    /// Send a signal to a (possibly parked) workflow.
    Signal {
        workflow_id: String,
        name: String,
        #[arg(default_value = "null")]
        payload: String,
        #[arg(long, default_value = "127.0.0.1:8080")]
        host: String,
    },
    /// Show a workflow's status and step journal.
    Show {
        workflow_id: String,
        #[arg(long, default_value = "127.0.0.1:8080")]
        host: String,
    },
    /// Query a workflow's durable state (optionally one key).
    Query {
        workflow_id: String,
        key: Option<String>,
        #[arg(long, default_value = "127.0.0.1:8080")]
        host: String,
    },
    /// Show whether the background daemon is running.
    Status {
        #[arg(long, default_value = "127.0.0.1:8080")]
        host: String,
    },
    /// Stop the background daemon.
    Stop {
        #[arg(long, default_value = "127.0.0.1:8080")]
        host: String,
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
        #[arg(long, default_value = "127.0.0.1:8080")]
        host: String,
    },
    /// Show (or follow) a workflow's logs/events.
    Logs {
        workflow_id: String,
        #[arg(short, long)]
        follow: bool,
        #[arg(long, default_value = "127.0.0.1:8080")]
        host: String,
    },
    /// Launch the TUI dashboard against a running daemon.
    Dashboard {
        #[arg(long, default_value = "127.0.0.1:8080")]
        host: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    const DEFAULT_HOST: &str = "127.0.0.1:8080";
    let cli = Cli::parse();
    match cli.cmd {
        // Bare `shift-clock` → open the dashboard (auto-spawns the daemon).
        None => tui::run(DEFAULT_HOST).await,
        Some(Cmd::Init) => {
            let dir = paths::ensure_scaffold()?;
            println!("scaffolded {}", dir.display());
            Ok(())
        }
        Some(Cmd::Serve { db, flows, addr, attach }) => server::serve(db, flows, addr, attach).await,
        Some(Cmd::Trigger { name, params, id, host }) => {
            cli::trigger(&host, &name, cli::parse_params(&params), id).await
        }
        Some(Cmd::Signal { workflow_id, name, payload, host }) => {
            let payload = serde_json::from_str(&payload).unwrap_or(serde_json::Value::String(payload));
            cli::signal(&host, &workflow_id, &name, payload).await
        }
        Some(Cmd::Show { workflow_id, host }) => cli::show(&host, &workflow_id).await,
        Some(Cmd::Query { workflow_id, key, host }) => cli::query(&host, &workflow_id, key).await,
        Some(Cmd::Status { host }) => daemon::status(&host).await,
        Some(Cmd::Stop { host }) => daemon::stop(&host),
        Some(Cmd::Run { name, flows, params }) => {
            cli::run_oneshot(&flows, &name, cli::parse_params(&params)).await
        }
        Some(Cmd::Workflows { limit, host }) => cli::workflows(&host, limit).await,
        Some(Cmd::Logs { workflow_id, follow, host }) => cli::logs(&host, &workflow_id, follow).await,
        Some(Cmd::Dashboard { host }) => tui::run(&host).await,
    }
}
