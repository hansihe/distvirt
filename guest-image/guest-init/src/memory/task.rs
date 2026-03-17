use std::cell::RefCell;
use std::rc::Rc;

use futures::FutureExt;

use distvirt_guest_protocol::GuestEvent;

use super::MemoryManager;
use super::monitor::BalloonChange;
use crate::cgroup;

/// Run the balloon management task: monitors PSI events, memory.events changes,
/// and balloon sysfs confirmations, sending `GuestEvent::BalloonSet` through
/// the channel.
///
/// Returns when the event channel closes (connection dropped). The
/// `mem_events` monitor is borrowed at task start and returned on exit so it
/// survives across connection reconnects.
pub async fn run(
    mm: Rc<RefCell<MemoryManager>>,
    psi: Rc<cgroup::AsyncPsiMonitor>,
    event_tx: async_channel::Sender<GuestEvent>,
    mem_events_holder: Rc<RefCell<Option<cgroup::AsyncMemoryEventsMonitor>>>,
    balloon_rx: async_channel::Receiver<BalloonChange>,
) {
    // Take the monitor for the duration of this task.
    let mut mem_events = mem_events_holder.borrow_mut().take();

    let result = run_inner(&mm, &psi, &event_tx, &mut mem_events, &balloon_rx).await;

    // Put the monitor back so the next connection can reuse it.
    *mem_events_holder.borrow_mut() = mem_events;

    result
}

async fn run_inner(
    mm: &Rc<RefCell<MemoryManager>>,
    psi: &Rc<cgroup::AsyncPsiMonitor>,
    event_tx: &async_channel::Sender<GuestEvent>,
    mem_events: &mut Option<cgroup::AsyncMemoryEventsMonitor>,
    balloon_rx: &async_channel::Receiver<BalloonChange>,
) {
    let mut inflation_timer = async_io::Timer::after(std::time::Duration::from_secs(5));

    loop {
        let psi_ready = psi.wait();
        let inflation_tick = async {
            (&mut inflation_timer).await;
        };
        let mem_events_change = async {
            match mem_events.as_mut() {
                Some(monitor) => monitor.wait_for_change().await,
                None => futures::future::pending().await,
            }
        };
        let balloon_change = async {
            match balloon_rx.recv().await {
                Ok(change) => Some(change),
                Err(_) => None,
            }
        };

        futures::select! {
            level = psi_ready.fuse() => {
                let mut mm = mm.borrow_mut();
                if let Some(event) = mm.handle_psi_event(level, cgroup::CGROUP_ROOT) {
                    if event_tx.send(event).await.is_err() {
                        return;
                    }
                }
            }
            _ = inflation_tick.fuse() => {
                inflation_timer = async_io::Timer::after(std::time::Duration::from_secs(5));
                let mut mm = mm.borrow_mut();
                if let Some(event) = mm.tick_inflation(cgroup::CGROUP_ROOT) {
                    if event_tx.send(event).await.is_err() {
                        return;
                    }
                }
            }
            (diff, _absolute) = mem_events_change.fuse() => {
                log::info!(
                    "[memory.events] delta: low=+{} high=+{} max=+{} oom=+{} oom_kill=+{} oom_group_kill=+{}",
                    diff.low, diff.high, diff.max, diff.oom, diff.oom_kill, diff.oom_group_kill,
                );

                // Primary deflation trigger: memory.high breaches cause PSI some
                // (kernel throttling) but not PSI full, so we deflate here.
                if diff.high > 0 {
                    let mut mm = mm.borrow_mut();
                    if let Some(event) = mm.handle_pressure() {
                        if event_tx.send(event).await.is_err() {
                            return;
                        }
                    }
                }
            }
            change = balloon_change.fuse() => {
                match change {
                    Some(BalloonChange { old_pages, new_pages }) => {
                        let mut mm = mm.borrow_mut();
                        mm.on_balloon_pages_changed(old_pages, new_pages, cgroup::CGROUP_ROOT);
                    }
                    None => {
                        // Balloon monitor channel closed — monitor exited.
                        // Must return to avoid busy-looping (recv returns Err immediately).
                        log::warn!("[balloon_task] balloon monitor channel closed");
                        return;
                    }
                }
            }
        }
    }
}
