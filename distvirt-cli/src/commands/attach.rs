use std::io::Write;

use anyhow::{bail, Context};
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal;
use distvirt_client::connection::{handle_grpc_error, Client};
use distvirt_client_protocol::*;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

const DEFAULT_DETACH_KEYS: (KeyEvent, KeyEvent) = (
    KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL),
    KeyEvent::new(KeyCode::Char('q'), KeyModifiers::CONTROL),
);

pub async fn attach(
    mut client: Client,
    namespace_id: &str,
    workload_id: &str,
) -> anyhow::Result<()> {
    // Set up the input channel. First message is AttachStart.
    let (input_tx, input_rx) = mpsc::channel::<AttachWorkloadInput>(32);

    input_tx
        .send(AttachWorkloadInput {
            input: Some(attach_workload_input::Input::Start(AttachStart {
                namespace_id: namespace_id.to_string(),
                workload_id: workload_id.to_string(),
            })),
        })
        .await?;

    // Open the bidirectional stream.
    let response = client
        .attach_workload(ReceiverStream::new(input_rx))
        .await
        .map_err(handle_grpc_error)?;
    let mut output_stream = response.into_inner();

    // Wait for AttachStarted to know if this is a TTY session.
    let first_msg = output_stream
        .message()
        .await
        .map_err(handle_grpc_error)?
        .context("stream closed before AttachStarted")?;

    let is_tty = match first_msg.output {
        Some(attach_workload_output::Output::Started(started)) => started.tty,
        Some(attach_workload_output::Output::Exited(exited)) => {
            bail!("process already exited with code {}", exited.exit_code);
        }
        _ => bail!("unexpected first message from server"),
    };

    // Print detach instructions before entering raw mode.
    eprintln!(
        "Attached to {}/{}{}. Detach with Ctrl-P Ctrl-Q.",
        namespace_id,
        workload_id,
        if is_tty { " (TTY)" } else { "" },
    );

    // Enter raw mode if TTY session and we have a terminal.
    let raw_mode = is_tty && terminal::is_raw_mode_enabled().unwrap_or(false) == false;
    if raw_mode {
        terminal::enable_raw_mode().context("failed to enable raw mode")?;
    }

    let result = run_attach_loop(is_tty, input_tx, &mut output_stream).await;

    // Restore terminal state.
    if raw_mode {
        terminal::disable_raw_mode().ok();
    }

    result
}

async fn run_attach_loop(
    is_tty: bool,
    input_tx: mpsc::Sender<AttachWorkloadInput>,
    output_stream: &mut tonic::Streaming<AttachWorkloadOutput>,
) -> anyhow::Result<()> {
    let mut stdout = std::io::stdout();
    let mut stderr = std::io::stderr();

    // For detach key sequence detection.
    let (detach_first, detach_second) = DEFAULT_DETACH_KEYS;
    let mut saw_first_detach_key = false;

    // Crossterm event stream for terminal input and resize events.
    let mut terminal_events = EventStream::new();

    loop {
        tokio::select! {
            // Terminal input (stdin + resize).
            event = terminal_events.next() => {
                let Some(event) = event else { break };
                let event = event.context("terminal event error")?;

                match event {
                    Event::Key(key) => {
                        // Detach sequence detection.
                        if is_tty {
                            if saw_first_detach_key {
                                if key == detach_second {
                                    eprintln!("\r\nDetached.");
                                    break;
                                }
                                saw_first_detach_key = false;
                                // Send the buffered first key that wasn't part of a detach.
                                if let Some(bytes) = key_event_to_bytes(&detach_first) {
                                    send_stdin(&input_tx, bytes).await?;
                                }
                            }
                            if key == detach_first {
                                saw_first_detach_key = true;
                                continue;
                            }
                        }

                        // Forward key as stdin bytes.
                        if let Some(bytes) = key_event_to_bytes(&key) {
                            send_stdin(&input_tx, bytes).await?;
                        }
                    }
                    Event::Resize(cols, rows) => {
                        if is_tty {
                            input_tx
                                .send(AttachWorkloadInput {
                                    input: Some(attach_workload_input::Input::Resize(
                                        AttachResize {
                                            cols: cols as u32,
                                            rows: rows as u32,
                                        },
                                    )),
                                })
                                .await
                                .ok();
                        }
                    }
                    _ => {}
                }
            }

            // Server output.
            msg = output_stream.message() => {
                let msg = msg.map_err(handle_grpc_error)?;
                let Some(msg) = msg else { break };

                match msg.output {
                    Some(attach_workload_output::Output::Stdout(data)) => {
                        stdout.write_all(&data.data)?;
                        stdout.flush()?;
                    }
                    Some(attach_workload_output::Output::Stderr(data)) => {
                        stderr.write_all(&data.data)?;
                        stderr.flush()?;
                    }
                    Some(attach_workload_output::Output::Exited(exited)) => {
                        if is_tty {
                            eprintln!("\r\nProcess exited with code {}.", exited.exit_code);
                        } else {
                            eprintln!("Process exited with code {}.", exited.exit_code);
                        }
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

async fn send_stdin(
    tx: &mpsc::Sender<AttachWorkloadInput>,
    data: Vec<u8>,
) -> anyhow::Result<()> {
    tx.send(AttachWorkloadInput {
        input: Some(attach_workload_input::Input::Stdin(AttachStdin { data })),
    })
    .await
    .context("failed to send stdin")?;
    Ok(())
}

fn key_event_to_bytes(key: &KeyEvent) -> Option<Vec<u8>> {
    match key.code {
        KeyCode::Char(c) => {
            if key.modifiers.contains(KeyModifiers::CONTROL) {
                // Ctrl+A = 0x01, Ctrl+Z = 0x1A
                let byte = (c as u8).wrapping_sub(b'a').wrapping_add(1);
                if byte <= 26 {
                    Some(vec![byte])
                } else {
                    None
                }
            } else {
                let mut buf = [0u8; 4];
                let s = c.encode_utf8(&mut buf);
                Some(s.as_bytes().to_vec())
            }
        }
        KeyCode::Enter => Some(vec![b'\r']),
        KeyCode::Backspace => Some(vec![0x7f]),
        KeyCode::Tab => Some(vec![b'\t']),
        KeyCode::Esc => Some(vec![0x1b]),
        KeyCode::Up => Some(b"\x1b[A".to_vec()),
        KeyCode::Down => Some(b"\x1b[B".to_vec()),
        KeyCode::Right => Some(b"\x1b[C".to_vec()),
        KeyCode::Left => Some(b"\x1b[D".to_vec()),
        KeyCode::Home => Some(b"\x1b[H".to_vec()),
        KeyCode::End => Some(b"\x1b[F".to_vec()),
        KeyCode::Delete => Some(b"\x1b[3~".to_vec()),
        KeyCode::Insert => Some(b"\x1b[2~".to_vec()),
        KeyCode::PageUp => Some(b"\x1b[5~".to_vec()),
        KeyCode::PageDown => Some(b"\x1b[6~".to_vec()),
        KeyCode::F(n) => {
            let seq = match n {
                1 => "\x1b[11~",
                2 => "\x1b[12~",
                3 => "\x1b[13~",
                4 => "\x1b[14~",
                5 => "\x1b[15~",
                6 => "\x1b[17~",
                7 => "\x1b[18~",
                8 => "\x1b[19~",
                9 => "\x1b[20~",
                10 => "\x1b[21~",
                11 => "\x1b[23~",
                12 => "\x1b[24~",
                _ => return None,
            };
            Some(seq.as_bytes().to_vec())
        }
        _ => None,
    }
}
