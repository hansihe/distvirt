#!/usr/bin/env bash
# Build and run the memory management testbench.
#
# Requires root for /dev/kvm and containerd access.
# Builds as the current user to avoid root-owned build artifacts.
#
# Usage: ./distvirt-worker/examples/run-memory-testbench.sh [extra args for memory_testbench...]

set -euo pipefail

CARGO="$(which cargo)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_IMAGE="$SCRIPT_DIR/../../guest-image/result-test-containers"

# Try to find a graphical askpass program for GUI-based sudo approval.
if [[ -z "${SUDO_ASKPASS:-}" ]]; then
    for askpass in ksshaskpass ssh-askpass lxqt-openssh-askpass gnome-ssh-askpass; do
        if command -v "$askpass" &>/dev/null; then
            export SUDO_ASKPASS="$(command -v "$askpass")"
            break
        fi
    done
fi

SUDO_FLAGS=(-E)
if [[ -n "${SUDO_ASKPASS:-}" ]]; then
    SUDO_FLAGS+=(-A)
fi

# Import the test container image into containerd.
if [[ -f "$TEST_IMAGE" ]]; then
    echo "==> Loading test container image into containerd..."
    sudo "${SUDO_FLAGS[@]}" ctr image import "$TEST_IMAGE"
else
    echo "error: test container image not found at $TEST_IMAGE" >&2
    echo "       run guest-image/build.sh first" >&2
    exit 1
fi

# Build as the current (non-root) user.
echo "==> Building memory_testbench..."
"$CARGO" build --example memory_testbench --package distvirt-worker

# Find the built binary. Run the binary directly instead of `sudo cargo run`
# so that SIGINT (Ctrl+C) reaches the process properly.
TESTBENCH_BIN="$("$CARGO" build --example memory_testbench --package distvirt-worker --message-format=json 2>/dev/null \
    | jq -r 'select(.executable != null and .target.name == "memory_testbench") | .executable')"

if [[ -z "$TESTBENCH_BIN" ]]; then
    echo "error: could not determine binary path" >&2
    exit 1
fi

# Run with sudo, forwarding Ctrl+C (SIGINT/SIGTERM) to the child.
# We cannot use `exec` here because we need the shell alive to trap signals
# and forward them — sudo doesn't reliably relay SIGINT to the child process group.
echo "==> Running $TESTBENCH_BIN..."
sudo "${SUDO_FLAGS[@]}" "$TESTBENCH_BIN" \
    --container-image docker.io/library/distvirt-test-containers:latest \
    --serial-console \
    "$@" &
SUDO_PID=$!

trap 'kill -INT $SUDO_PID 2>/dev/null; wait $SUDO_PID 2>/dev/null; exit' INT TERM
wait $SUDO_PID
