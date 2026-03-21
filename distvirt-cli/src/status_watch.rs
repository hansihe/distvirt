use std::collections::VecDeque;
use std::io::{self, Write};

use crossterm::{
    cursor,
    event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers},
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use futures::StreamExt;

use distvirt_client::format::render_event_line;
use distvirt_client::model::NamespaceModel;
use distvirt_client::watcher::NamespaceWatcher;

const SEPARATOR: &str = "── Recent Events ";

struct WatchState {
    events: VecDeque<String>,
    max_events: usize,
}

impl WatchState {
    fn new() -> Self {
        Self {
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
fn render(model: &NamespaceModel, watch: &WatchState, width: u16, height: u16) -> String {
    let mut buf = String::new();

    // Status section
    let status_text = render_model_overview(model);

    let status_lines: Vec<&str> = status_text.lines().collect();
    let n_status = status_lines.len();

    for line in &status_lines {
        buf.push_str(line);
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
    let used = n_status + 1;
    let event_rows = (height as usize).saturating_sub(used);

    let start = watch.events.len().saturating_sub(event_rows);
    let mut printed = 0;
    for line in watch.events.iter().skip(start) {
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

/// Render a namespace overview from the model.
fn render_model_overview(model: &NamespaceModel) -> String {
    use std::fmt::Write;
    let mut buf = String::new();

    writeln!(
        &mut buf,
        "Namespace: {}  State: {}",
        model.namespace_id,
        model.state.label()
    )
    .unwrap();
    writeln!(&mut buf).unwrap();

    if model.workloads.is_empty() && model.services.is_empty() {
        writeln!(&mut buf, "  (no workloads)").unwrap();
        return buf;
    }

    let mut sorted_workloads: Vec<_> = model.workloads.iter().collect();
    sorted_workloads.sort_by_key(|(id, _)| id.as_str());
    let mut sorted_services: Vec<_> = model.services.iter().collect();
    sorted_services.sort_by_key(|(id, _)| id.as_str());

    for (workload_id, workload) in &sorted_workloads {
        let state = workload.state.label();
        let spliced = if workload.spliced { " [spliced]" } else { "" };
        let ip = workload.ip.as_deref().unwrap_or("");
        if ip.is_empty() {
            writeln!(&mut buf, "  workload/{:<20} {}{}", workload_id, state, spliced).unwrap();
        } else {
            writeln!(
                &mut buf,
                "  workload/{:<20} {}  {}{}",
                workload_id, state, ip, spliced
            )
            .unwrap();
        }

        for (svc_id, svc) in &sorted_services {
            if svc.workload_id.as_str() == workload_id.as_str() {
                let svc_state = svc.state.label();
                let activation = if svc.activation_enabled {
                    " (activation)"
                } else {
                    ""
                };
                writeln!(
                    &mut buf,
                    "    service/{:<18} {}{}",
                    svc_id, svc_state, activation
                )
                .unwrap();
            }
        }
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
    watcher: NamespaceWatcher,
) -> anyhow::Result<()> {
    let _guard = TerminalGuard::enter()?;

    // Split watcher into model + stream so we can use them independently
    // in the select loop.
    let (mut model, mut event_stream) = watcher.into_parts();

    let mut watch = WatchState::new();
    let mut term_events = EventStream::new();

    // Initial render
    let (mut cols, mut rows) = terminal::size()?;
    let status_text = render_model_overview(&model);
    watch.update_max_events(rows, status_text.lines().count());
    redraw(&model, &watch, cols, rows)?;

    loop {
        tokio::select! {
            // gRPC event stream
            msg = event_stream.message() => {
                match msg {
                    Ok(Some(event)) => {
                        let line = render_event_line(&event);
                        watch.push_event(line);

                        // Apply event to model
                        model.apply_event(&event);

                        let status_lines = render_model_overview(&model).lines().count();
                        watch.update_max_events(rows, status_lines);
                        redraw(&model, &watch, cols, rows)?;
                    }
                    Ok(None) => {
                        watch.push_event("(event stream ended)".to_string());
                        redraw(&model, &watch, cols, rows)?;
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
                        return Err(distvirt_client::connection::handle_grpc_error(status));
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
                        let status_lines = render_model_overview(&model).lines().count();
                        watch.update_max_events(rows, status_lines);
                        redraw(&model, &watch, cols, rows)?;
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

fn redraw(model: &NamespaceModel, watch: &WatchState, cols: u16, rows: u16) -> io::Result<()> {
    let buf = render(model, watch, cols, rows);
    let mut stdout = io::stdout();
    stdout.execute(cursor::MoveTo(0, 0))?;
    stdout.write_all(buf.as_bytes())?;
    stdout.flush()?;
    Ok(())
}

fn should_quit(key: &KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char('q'))
        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
}
