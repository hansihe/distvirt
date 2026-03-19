use std::collections::VecDeque;
use std::io::{self, Write};

use crossterm::{
    cursor,
    event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers},
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use distvirt_client_protocol::*;
use futures::StreamExt;
use tokio::time::{self, Duration};
use tonic::Streaming;

use crate::client::{self, Client};
use crate::format;

const DEBOUNCE_MS: u64 = 100;
const SEPARATOR: &str = "── Recent Events ";

struct WatchState {
    report: Option<NamespaceStatusReport>,
    events: VecDeque<String>,
    max_events: usize,
}

impl WatchState {
    fn new() -> Self {
        Self {
            report: None,
            events: VecDeque::new(),
            max_events: 15,
        }
    }

    fn push_event(&mut self, line: String) {
        self.events.push_back(line);
        while self.events.len() > self.max_events {
            self.events.pop_front();
        }
    }

    fn update_max_events(&mut self, terminal_height: u16, status_lines: usize) {
        // Reserve: status lines + separator line + 1 line bottom margin
        let reserved = status_lines + 2;
        self.max_events = (terminal_height as usize).saturating_sub(reserved).max(3);
        while self.events.len() > self.max_events {
            self.events.pop_front();
        }
    }
}

/// Render the full screen into a buffer string.
fn render(state: &WatchState, width: u16, height: u16) -> String {
    let mut buf = String::new();

    // Status section
    let status_text = match &state.report {
        Some(report) => format::render_namespace_overview(report),
        None => "Loading...\n".to_string(),
    };

    let status_lines: Vec<&str> = status_text.lines().collect();
    let n_status = status_lines.len();

    for line in &status_lines {
        buf.push_str(line);
        // Clear to end of line
        buf.push_str("\x1b[K");
        buf.push('\n');
    }

    // Separator
    let sep_pad = (width as usize).saturating_sub(SEPARATOR.len());
    buf.push_str(SEPARATOR);
    for _ in 0..sep_pad {
        buf.push('─');
    }
    buf.push('\n');

    // Events section — fill remaining lines
    let used = n_status + 1; // status + separator
    let event_rows = (height as usize).saturating_sub(used);

    // Show most recent events that fit
    let start = state.events.len().saturating_sub(event_rows);
    let mut printed = 0;
    for line in state.events.iter().skip(start) {
        if printed >= event_rows {
            break;
        }
        buf.push_str(line);
        buf.push_str("\x1b[K");
        buf.push('\n');
        printed += 1;
    }

    // Clear remaining rows
    for _ in printed..event_rows {
        buf.push_str("\x1b[K");
        buf.push('\n');
    }

    buf
}

/// Guard that restores terminal state on drop.
struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        terminal::enable_raw_mode()?;
        io::stdout().execute(EnterAlternateScreen)?;
        io::stdout().execute(cursor::Hide)?;
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = io::stdout().execute(cursor::Show);
        let _ = io::stdout().execute(LeaveAlternateScreen);
        let _ = terminal::disable_raw_mode();
    }
}

pub async fn run(
    mut client: Client,
    namespace_id: &str,
    mut event_stream: Streaming<NamespaceEvent>,
) -> anyhow::Result<()> {
    let _guard = TerminalGuard::enter()?;

    let mut state = WatchState::new();
    let mut term_events = EventStream::new();

    // Initial status fetch
    let resp = client
        .get_namespace_status(GetNamespaceStatusRequest {
            namespace_id: namespace_id.to_string(),
        })
        .await
        .map_err(client::handle_grpc_error)?;
    state.report = resp.into_inner().status;

    // Initial render
    let (mut cols, mut rows) = terminal::size()?;
    state.update_max_events(
        rows,
        state
            .report
            .as_ref()
            .map(|r| format::render_namespace_overview(r).lines().count())
            .unwrap_or(1),
    );
    redraw(&state, cols, rows)?;

    // Debounce state
    let mut refetch_pending = false;
    let mut debounce_deadline: Option<time::Instant> = None;

    loop {
        // Compute sleep future for debounce
        let debounce_sleep = match debounce_deadline {
            Some(deadline) => tokio::time::sleep_until(deadline),
            None => {
                // Sleep forever (won't fire)
                tokio::time::sleep(Duration::from_secs(86400))
            }
        };
        let debounce_active = debounce_deadline.is_some();

        tokio::select! {
            // gRPC event stream
            msg = event_stream.message() => {
                match msg {
                    Ok(Some(event)) => {
                        let line = format::render_event_line(&event);
                        state.push_event(line);

                        // Start debounce if not already pending
                        if !refetch_pending {
                            refetch_pending = true;
                            debounce_deadline = Some(time::Instant::now() + Duration::from_millis(DEBOUNCE_MS));
                        }

                        redraw(&state, cols, rows)?;
                    }
                    Ok(None) => {
                        // Stream ended
                        state.push_event("(event stream ended)".to_string());
                        redraw(&state, cols, rows)?;
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
                        return Err(client::handle_grpc_error(status));
                    }
                }
            }

            // Debounce timer fired — re-fetch status
            _ = debounce_sleep, if debounce_active => {
                refetch_pending = false;
                debounce_deadline = None;

                match client
                    .get_namespace_status(GetNamespaceStatusRequest {
                        namespace_id: namespace_id.to_string(),
                    })
                    .await
                {
                    Ok(resp) => {
                        state.report = resp.into_inner().status;
                        let status_lines = state
                            .report
                            .as_ref()
                            .map(|r| format::render_namespace_overview(r).lines().count())
                            .unwrap_or(1);
                        state.update_max_events(rows, status_lines);
                        redraw(&state, cols, rows)?;
                    }
                    Err(status) => {
                        state.push_event(format!("(status fetch error: {})", status.message()));
                        redraw(&state, cols, rows)?;
                    }
                }
            }

            // Terminal events (key presses, resize)
            ev = term_events.next() => {
                match ev {
                    Some(Ok(Event::Key(key))) => {
                        if should_quit(&key) {
                            return Ok(());
                        }
                    }
                    Some(Ok(Event::Resize(new_cols, new_rows))) => {
                        cols = new_cols;
                        rows = new_rows;
                        let status_lines = state
                            .report
                            .as_ref()
                            .map(|r| format::render_namespace_overview(r).lines().count())
                            .unwrap_or(1);
                        state.update_max_events(rows, status_lines);
                        redraw(&state, cols, rows)?;
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

fn redraw(state: &WatchState, cols: u16, rows: u16) -> io::Result<()> {
    let buf = render(state, cols, rows);
    let mut stdout = io::stdout();
    // Move cursor to top-left and write the whole buffer at once
    stdout.execute(cursor::MoveTo(0, 0))?;
    stdout.write_all(buf.as_bytes())?;
    stdout.flush()?;
    Ok(())
}

fn should_quit(key: &KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char('q'))
        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
}
