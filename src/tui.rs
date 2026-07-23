//! A k9s-style TUI dashboard, purely an HTTP client of the control plane (so
//! the identical view can watch a remote daemon later with --host). Two tabs:
//!   Runs      — recent runs (left) + the selected run's tasks/logs (right)
//!   Scheduled — deployments (left) + the selected job's config/next-fire (right)
//! Polls once a second. Keys: Tab/←→/1-2 switch tabs, ↑/↓ select, r refresh, q quit.

use crate::client::Client;
use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Tabs, Wrap};
use serde_json::Value;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq)]
enum Tab {
    Runs,
    Scheduled,
}

pub async fn run(host: &str) -> Result<()> {
    let client = Client::new(host);

    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let res = event_loop(&mut terminal, &client).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    res
}

struct AppUi {
    tab: Tab,
    flows: Vec<Value>,
    flow_selected: usize,
    runs: Vec<Value>,
    selected: usize,
    detail: Option<Value>,
    logs: Vec<Value>,
    events: Vec<Value>,
    status: String,
}

async fn event_loop<B: Backend>(terminal: &mut Terminal<B>, client: &Client) -> Result<()> {
    let mut ui = AppUi {
        tab: Tab::Runs,
        flows: Vec::new(),
        flow_selected: 0,
        runs: Vec::new(),
        selected: 0,
        detail: None,
        logs: Vec::new(),
        events: Vec::new(),
        status: "connecting…".to_string(),
    };
    let mut last_poll = Instant::now() - Duration::from_secs(10);

    loop {
        if last_poll.elapsed() >= Duration::from_millis(1000) {
            refresh(client, &mut ui).await;
            last_poll = Instant::now();
        }

        terminal.draw(|f| draw(f, &ui))?;

        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(k) = event::read()? {
                match k.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Char('r') => {
                        refresh(client, &mut ui).await;
                        last_poll = Instant::now();
                    }
                    KeyCode::Tab | KeyCode::Right | KeyCode::Char('l') => {
                        ui.tab = match ui.tab {
                            Tab::Runs => Tab::Scheduled,
                            Tab::Scheduled => Tab::Runs,
                        };
                    }
                    KeyCode::BackTab | KeyCode::Left | KeyCode::Char('h') => {
                        ui.tab = match ui.tab {
                            Tab::Runs => Tab::Scheduled,
                            Tab::Scheduled => Tab::Runs,
                        };
                    }
                    KeyCode::Char('1') => ui.tab = Tab::Runs,
                    KeyCode::Char('2') => ui.tab = Tab::Scheduled,
                    KeyCode::Down | KeyCode::Char('j') => match ui.tab {
                        Tab::Runs => {
                            if !ui.runs.is_empty() {
                                ui.selected = (ui.selected + 1).min(ui.runs.len() - 1);
                                refresh_detail(client, &mut ui).await;
                            }
                        }
                        Tab::Scheduled => {
                            if !ui.flows.is_empty() {
                                ui.flow_selected = (ui.flow_selected + 1).min(ui.flows.len() - 1);
                            }
                        }
                    },
                    KeyCode::Up | KeyCode::Char('k') => match ui.tab {
                        Tab::Runs => {
                            ui.selected = ui.selected.saturating_sub(1);
                            refresh_detail(client, &mut ui).await;
                        }
                        Tab::Scheduled => ui.flow_selected = ui.flow_selected.saturating_sub(1),
                    },
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

async fn refresh(client: &Client, ui: &mut AppUi) {
    ui.flows = client.list_flows().await.unwrap_or_default();
    if ui.flow_selected >= ui.flows.len() {
        ui.flow_selected = ui.flows.len().saturating_sub(1);
    }
    match client.list_workflows(100).await {
        Ok(runs) => {
            ui.runs = runs;
            if ui.selected >= ui.runs.len() {
                ui.selected = ui.runs.len().saturating_sub(1);
            }
            ui.status = format!("{} run(s) · updated {}", ui.runs.len(), now_hms());
            refresh_detail(client, ui).await;
        }
        Err(e) => ui.status = format!("error: {e}"),
    }
}

async fn refresh_detail(client: &Client, ui: &mut AppUi) {
    let Some(run) = ui.runs.get(ui.selected) else {
        ui.detail = None;
        ui.logs = Vec::new();
        ui.events = Vec::new();
        return;
    };
    let id = run.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
    ui.detail = client.get_workflow(&id).await.ok();
    ui.logs = client.get_logs(&id).await.unwrap_or_default();
    ui.events = client.get_events(&id).await.unwrap_or_default();
}

fn draw(f: &mut Frame, ui: &AppUi) {
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(3), Constraint::Length(1)])
        .split(f.area());

    // Tab bar
    let tabs = Tabs::new(vec!["Runs", "Scheduled"])
        .select(match ui.tab {
            Tab::Runs => 0,
            Tab::Scheduled => 1,
        })
        .block(Block::default().borders(Borders::ALL).title(" shift-clock "))
        .highlight_style(Style::default().fg(Color::Black).bg(Color::Cyan).add_modifier(Modifier::BOLD))
        .divider("│");
    f.render_widget(tabs, outer[0]);

    match ui.tab {
        Tab::Runs => draw_runs(f, ui, outer[1]),
        Tab::Scheduled => draw_scheduled(f, ui, outer[1]),
    }

    let hint = match ui.tab {
        Tab::Runs => "[Tab] scheduled  [↑/↓] select run  [r] refresh  [q] quit",
        Tab::Scheduled => "[Tab] runs  [↑/↓] select job  [r] refresh  [q] quit",
    };
    let status = Paragraph::new(format!(" shift-clock · {} · {}", ui.status, hint))
        .style(Style::default().fg(Color::Black).bg(Color::Cyan));
    f.render_widget(status, outer[2]);
}

fn draw_runs(f: &mut Frame, ui: &AppUi, area: Rect) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(42), Constraint::Percentage(58)])
        .split(area);

    let items: Vec<ListItem> = ui
        .runs
        .iter()
        .map(|r| {
            let id = r.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let flow = r.get("deployment").and_then(|v| v.as_str()).unwrap_or("");
            let state = r.get("status").and_then(|v| v.as_str()).unwrap_or("");
            let dot = state_dot(state);
            ListItem::new(format!("{dot} {id}  {flow}  {state}")).style(state_style(state))
        })
        .collect();
    let mut lstate = ListState::default();
    if !ui.runs.is_empty() {
        lstate.select(Some(ui.selected));
    }
    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(" workflows "))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED))
        .highlight_symbol("▶ ");
    f.render_stateful_widget(list, cols[0], &mut lstate);

    let mut body = String::new();
    if let Some(d) = &ui.detail {
        let wf = &d["workflow"];
        body.push_str(&format!(
            "flow     {}\nworkflow {}\nstatus   {}\ntrigger  {}\nattempts {}\n\nsteps:\n",
            jstr(wf, "deployment"),
            jstr(wf, "id"),
            jstr(wf, "status"),
            jstr(wf, "trigger"),
            wf.get("attempts").and_then(|v| v.as_i64()).unwrap_or(0),
        ));
        if let Some(steps) = d["steps"].as_array() {
            for t in steps {
                let dur = t.get("duration_ms").and_then(|v| v.as_i64()).map(|d| format!("{d}ms")).unwrap_or_else(|| "—".into());
                body.push_str(&format!("  #{:<3} {:<16} {:<9} {}\n", t.get("seq").and_then(|v| v.as_i64()).unwrap_or(0), jstr(t, "name"), jstr(t, "status"), dur));
            }
        }
        // Merged activity: task events + SDK log() messages + stdout/stderr.
        body.push_str("\nactivity:\n");
        let feed = merged_activity(&ui.events, &ui.logs);
        let n = feed.len();
        for line in feed.into_iter().skip(n.saturating_sub(16)) {
            body.push_str(&format!("{line}\n"));
        }
    } else {
        body.push_str("no run selected");
    }
    let detail = Paragraph::new(body)
        .block(Block::default().borders(Borders::ALL).title(" detail "))
        .wrap(Wrap { trim: false });
    f.render_widget(detail, cols[1]);
}

fn draw_scheduled(f: &mut Frame, ui: &AppUi, area: Rect) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(42), Constraint::Percentage(58)])
        .split(area);

    let now = chrono::Local::now();
    let items: Vec<ListItem> = ui
        .flows
        .iter()
        .map(|fl| {
            let name = jstr(fl, "name");
            let enabled = fl.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true);
            let cron = fl.get("cron").and_then(|v| v.as_str());
            let (sched, style) = match cron {
                Some(expr) if enabled => (format!("{expr:<11} next {}", next_fire(expr, &now)), Style::default().fg(Color::Cyan)),
                Some(expr) => (format!("{expr:<11} (disabled)"), Style::default().fg(Color::DarkGray)),
                None => ("manual only".to_string(), Style::default().fg(Color::DarkGray)),
            };
            ListItem::new(format!("⏱ {name:<12} {sched}")).style(style)
        })
        .collect();
    let mut lstate = ListState::default();
    if !ui.flows.is_empty() {
        lstate.select(Some(ui.flow_selected));
    }
    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(" scheduled jobs "))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED))
        .highlight_symbol("▶ ");
    f.render_stateful_widget(list, cols[0], &mut lstate);

    // Right: selected job's config + its recent runs.
    let mut body = String::new();
    if let Some(fl) = ui.flows.get(ui.flow_selected) {
        let name = jstr(fl, "name");
        let cmd = fl
            .get("cmd")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str()).collect::<Vec<_>>().join(" "))
            .unwrap_or_default();
        let cron = fl.get("cron").and_then(|v| v.as_str());
        body.push_str(&format!("job      {name}\ncommand  {cmd}\n"));
        match cron {
            Some(expr) => body.push_str(&format!(
                "cron     {expr}\nnext     {}\n",
                next_fire(expr, &now)
            )),
            None => body.push_str("cron     — (manual only)\n"),
        }
        body.push_str(&format!(
            "enabled  {}\ncatchup  {}\noverlap  {}\n",
            fl.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true),
            jstr(fl, "catchup"),
            jstr(fl, "overlap"),
        ));
        if let Some(params) = fl.get("params") {
            if params.as_object().map(|o| !o.is_empty()).unwrap_or(false) {
                body.push_str(&format!("params   {}\n", params));
            }
        }
        body.push_str("\nrecent runs:\n");
        let mut any = false;
        for r in ui.runs.iter().filter(|r| jstr(r, "deployment") == name).take(8) {
            any = true;
            body.push_str(&format!(
                "  {} {:<9} {:<7} {}\n",
                state_dot(&jstr(r, "status")),
                jstr(r, "status"),
                jstr(r, "trigger"),
                jstr(r, "started_at"),
            ));
        }
        if !any {
            body.push_str("  (none yet)\n");
        }
    } else {
        body.push_str("no job selected");
    }
    let detail = Paragraph::new(body)
        .block(Block::default().borders(Borders::ALL).title(" job detail "))
        .wrap(Wrap { trim: false });
    f.render_widget(detail, cols[1]);
}

/// Merge task events and stdout/stderr logs into one ordered, formatted feed —
/// the same timeline `shift-clock logs <id>` prints.
fn merged_activity(events: &[Value], logs: &[Value]) -> Vec<String> {
    use crate::protocol::Envelope;
    let mut envs: Vec<Envelope> = Vec::new();
    for e in events {
        envs.push(Envelope {
            workflow_id: String::new(),
            seq: e.get("seq").and_then(|v| v.as_i64()).unwrap_or(0),
            ts: jstr(e, "ts"),
            kind: "event".into(),
            payload: e.get("payload").cloned().unwrap_or(Value::Null),
        });
    }
    for l in logs {
        envs.push(Envelope {
            workflow_id: String::new(),
            seq: 0,
            ts: jstr(l, "ts"),
            kind: "log".into(),
            payload: l.clone(),
        });
    }
    envs.sort_by(|a, b| a.ts.cmp(&b.ts).then(a.seq.cmp(&b.seq)));
    envs.iter().map(crate::cli::format_envelope).collect()
}

fn next_fire(expr: &str, now: &chrono::DateTime<chrono::Local>) -> String {
    crate::config::Cron::parse(expr)
        .ok()
        .and_then(|c| c.next_after(now))
        .map(|t| t.format("%H:%M").to_string())
        .unwrap_or_else(|| "—".into())
}

fn state_dot(state: &str) -> &'static str {
    match state {
        "success" => "●",
        "failed" => "✗",
        "running" => "◐",
        "sleeping" | "waiting" => "⏸",
        _ => "·",
    }
}

fn state_style(state: &str) -> Style {
    match state {
        "success" => Style::default().fg(Color::Green),
        "failed" => Style::default().fg(Color::Red),
        "running" => Style::default().fg(Color::Yellow),
        "sleeping" | "waiting" => Style::default().fg(Color::Magenta),
        _ => Style::default(),
    }
}

fn jstr(v: &Value, k: &str) -> String {
    v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string()
}

fn now_hms() -> String {
    chrono::Local::now().format("%H:%M:%S").to_string()
}
