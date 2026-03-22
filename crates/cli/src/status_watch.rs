use std::io::{self, Write};

use crossterm::{
    cursor,
    event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers},
    style::{Attribute, Color, SetAttribute, SetForegroundColor, ResetColor},
    terminal::{self, Clear, ClearType},
    QueueableCommand,
};
use futures::StreamExt;

use distvirt_client::format;
use distvirt_client::model::{NamespaceModel, NamespaceState, ServiceState, WorkloadState};
use distvirt_client::watcher::NamespaceWatcher;
use distvirt_client_protocol::NamespaceEvent;

const SEPARATOR: &str = "── Status ";

/// A block of lines at the bottom of the terminal that get redrawn in-place.
/// Lines printed before calling `update` scroll up naturally into terminal history.
struct LiveBlock {
    /// Number of lines we printed last time (that we need to overwrite).
    last_line_count: usize,
}

impl LiveBlock {
    fn new() -> Self {
        Self { last_line_count: 0 }
    }

    /// Rewrite the live block with new content.
    /// Moves cursor up over previously printed lines, clears them, and prints new ones.
    fn update(&mut self, lines: &[String]) -> io::Result<()> {
        let mut stdout = io::stdout();

        // Move cursor up to the start of the previous live block
        if self.last_line_count > 0 {
            stdout.queue(cursor::MoveUp(self.last_line_count as u16))?;
            stdout.queue(cursor::MoveToColumn(0))?;
        }

        // Print new lines, clearing each line first
        for line in lines {
            stdout.queue(Clear(ClearType::CurrentLine))?;
            stdout.write_all(line.as_bytes())?;
            stdout.write_all(b"\r\n")?;
        }

        // If we now have fewer lines than before, clear the leftover lines
        for _ in lines.len()..self.last_line_count {
            stdout.queue(Clear(ClearType::CurrentLine))?;
            stdout.write_all(b"\r\n")?;
        }

        // If we shrank, move cursor back up past the blank lines we just wrote
        let extra = self.last_line_count.saturating_sub(lines.len());
        if extra > 0 {
            stdout.queue(cursor::MoveUp(extra as u16))?;
        }

        stdout.flush()?;
        self.last_line_count = lines.len();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Color helpers
// ---------------------------------------------------------------------------

fn workload_state_color(state: &WorkloadState) -> Color {
    match state {
        WorkloadState::Running { .. } => Color::Green,
        WorkloadState::Launching { .. } | WorkloadState::Suspending { .. } => Color::Yellow,
        WorkloadState::Failed { .. } => Color::Red,
        WorkloadState::RetryBackoff => Color::Red,
        WorkloadState::Completed { .. } => Color::Cyan,
        WorkloadState::Dormant | WorkloadState::Suspended => Color::DarkGrey,
        WorkloadState::WaitingForSpec => Color::Yellow,
    }
}

fn service_state_color(state: &ServiceState) -> Color {
    match state {
        ServiceState::Active { .. } => Color::Green,
        ServiceState::Pending | ServiceState::NeedBackend => Color::Yellow,
        ServiceState::Idle => Color::DarkGrey,
    }
}

fn namespace_state_color(state: &NamespaceState) -> Color {
    match state {
        NamespaceState::Active => Color::Green,
        NamespaceState::Creating => Color::Yellow,
        NamespaceState::Destroying => Color::Red,
    }
}

/// Format a colored string (embeds ANSI codes).
fn colored(text: &str, color: Color) -> String {
    format!(
        "{}{}{}",
        SetForegroundColor(color),
        text,
        ResetColor,
    )
}

fn dim(text: &str) -> String {
    format!(
        "{}{}{}",
        SetAttribute(Attribute::Dim),
        text,
        SetAttribute(Attribute::Reset),
    )
}

// ---------------------------------------------------------------------------
// Status block rendering
// ---------------------------------------------------------------------------

/// Build the status lines for the live block (with ANSI colors).
fn build_status_lines(model: &NamespaceModel, cols: u16) -> Vec<String> {
    let mut lines = Vec::new();

    // Blank line for spacing between events and status
    lines.push(String::new());

    // Separator line
    let sep_pad = (cols as usize).saturating_sub(SEPARATOR.len());
    let mut sep = dim(SEPARATOR);
    sep.push_str(&dim(&"─".repeat(sep_pad)));
    lines.push(sep);

    // Namespace header
    let ns_state = model.state.label();
    let ns_color = namespace_state_color(&model.state);
    lines.push(format!(
        "Namespace: {}  State: {}",
        model.namespace_id,
        colored(ns_state, ns_color),
    ));

    if model.workloads.is_empty() && model.services.is_empty() {
        lines.push(dim("  (no workloads)"));
        return lines;
    }

    lines.push(String::new());

    let mut sorted_workloads: Vec<_> = model.workloads.iter().collect();
    sorted_workloads.sort_by_key(|(id, _)| id.as_str());
    let mut sorted_services: Vec<_> = model.services.iter().collect();
    sorted_services.sort_by_key(|(id, _)| id.as_str());

    for (workload_id, workload) in &sorted_workloads {
        let state_label = workload.state.label();
        let state_color = workload_state_color(&workload.state);
        let spliced = if workload.spliced {
            format!("  {}", dim("[spliced]"))
        } else {
            String::new()
        };
        let ip = workload.ip.as_deref().unwrap_or("");
        let ip_part = if ip.is_empty() {
            String::new()
        } else {
            format!("  {}", dim(ip))
        };

        lines.push(format!(
            "  workload/{:<20} {}{}{}",
            workload_id,
            colored(&format!("{:<14}", state_label), state_color),
            ip_part,
            spliced,
        ));

        for (svc_id, svc) in &sorted_services {
            if svc.workload_id.as_str() == workload_id.as_str() {
                let svc_state_label = svc.state.label();
                let svc_color = service_state_color(&svc.state);
                let activation = if svc.activation_enabled {
                    format!("  {}", dim("(activation)"))
                } else {
                    String::new()
                };
                lines.push(format!(
                    "    service/{:<18} {}{}",
                    svc_id,
                    colored(&format!("{:<14}", svc_state_label), svc_color),
                    activation,
                ));
            }
        }
    }

    lines
}

// ---------------------------------------------------------------------------
// Event rendering (with colors)
// ---------------------------------------------------------------------------

fn render_colored_event(event: &NamespaceEvent) -> String {
    let ts = dim(&format::format_timestamp(event.timestamp_unix_ms));
    match &event.event {
        Some(distvirt_client_protocol::namespace_event::Event::Workload(we)) => {
            let desc = format::workload_event_description(we);
            format!("{}  workload/{}  {}", ts, we.workload_id, desc)
        }
        Some(distvirt_client_protocol::namespace_event::Event::Pod(pe)) => {
            let desc = format::pod_event_description(pe);
            format!(
                "{}  pod/{} {}  {}",
                ts,
                pe.pod_id,
                dim(&format!("(workload/{})", pe.workload_id)),
                desc,
            )
        }
        Some(distvirt_client_protocol::namespace_event::Event::Endpoint(ee)) => {
            let desc = format::endpoint_event_description(ee);
            let owner = if let Some(ref svc) = ee.service_id {
                format!("service/{}", svc)
            } else if let Some(ref wl) = ee.workload_id {
                format!("workload/{}", wl)
            } else {
                "unknown".to_string()
            };
            format!(
                "{}  endpoint/{} {}  {}",
                ts,
                ee.endpoint_id,
                dim(&format!("({})", owner)),
                desc,
            )
        }
        None => {
            format!("{}  {}", ts, dim("(unknown event)"))
        }
    }
}

// ---------------------------------------------------------------------------
// Event printing
// ---------------------------------------------------------------------------

/// Print an event line above the live block (it scrolls into terminal history).
fn print_event(live: &mut LiveBlock, event_line: &str) -> io::Result<()> {
    let mut stdout = io::stdout();

    // Move up to top of live block
    if live.last_line_count > 0 {
        stdout.queue(cursor::MoveUp(live.last_line_count as u16))?;
        stdout.queue(cursor::MoveToColumn(0))?;
    }

    // Clear all old live block lines to avoid leftover text
    for _ in 0..live.last_line_count {
        stdout.queue(Clear(ClearType::CurrentLine))?;
        stdout.write_all(b"\r\n")?;
    }

    // Move back up
    if live.last_line_count > 0 {
        stdout.queue(cursor::MoveUp(live.last_line_count as u16))?;
    }

    // Print the event line (this becomes part of scrollback)
    stdout.write_all(event_line.as_bytes())?;
    stdout.write_all(b"\r\n")?;
    stdout.flush()?;

    // We've cleared the old content, so reset last_line_count to 0
    // so the next `update` call prints fresh without trying to move up.
    live.last_line_count = 0;
    Ok(())
}

// ---------------------------------------------------------------------------
// Terminal guard
// ---------------------------------------------------------------------------

/// Guard that enables raw mode for key event detection and restores on drop.
struct RawModeGuard;

impl RawModeGuard {
    fn enter() -> io::Result<Self> {
        terminal::enable_raw_mode()?;
        Ok(Self)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = terminal::disable_raw_mode();
    }
}

// ---------------------------------------------------------------------------
// Main loop
// ---------------------------------------------------------------------------

pub async fn run(watcher: NamespaceWatcher) -> anyhow::Result<()> {
    let _guard = RawModeGuard::enter()?;

    let (mut model, mut event_stream) = watcher.into_parts();

    let mut live = LiveBlock::new();
    let mut term_events = EventStream::new();

    // Initial render
    let (cols, _) = terminal::size()?;
    let status_lines = build_status_lines(&model, cols);
    live.update(&status_lines)?;

    loop {
        tokio::select! {
            // gRPC event stream
            msg = event_stream.message() => {
                match msg {
                    Ok(Some(event)) => {
                        let line = render_colored_event(&event);
                        print_event(&mut live, &line)?;

                        model.apply_event(&event);
                        let (cols, _) = terminal::size()?;
                        let status_lines = build_status_lines(&model, cols);
                        live.update(&status_lines)?;
                    }
                    Ok(None) => {
                        print_event(&mut live, &dim("(event stream ended)"))?;
                        let (cols, _) = terminal::size()?;
                        let status_lines = build_status_lines(&model, cols);
                        live.update(&status_lines)?;
                        // Wait for user to quit
                        loop {
                            if let Some(Ok(Event::Key(key))) = term_events.next().await {
                                if should_quit(&key) {
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Err(status) => {
                        return Err(distvirt_client::connection::handle_grpc_error(status).into());
                    }
                }
            }

            // Terminal events (key presses)
            ev = term_events.next() => {
                match ev {
                    Some(Ok(Event::Key(key))) => {
                        if should_quit(&key) {
                            return Ok(());
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        return Err(e.into());
                    }
                    None => {
                        return Ok(());
                    }
                }
            }
        }
    }
}

fn should_quit(key: &KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char('q'))
        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
}
