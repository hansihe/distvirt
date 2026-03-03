#!/usr/bin/env bash
# Full-stack E2E test: orchestrator + worker + CLI.
#
# Requires root (TUN devices, KVM, containerd).
# Build happens as the current (non-root) user, then re-execs under sudo.
# Prerequisites: same as worker E2E tests plus pre-pulled alpine image in containerd.
#
# Usage: ./tests/full-stack-e2e.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CARGO="$(which cargo)"

# --- Build as current user (before sudo) ---

if [[ "${DISTVIRT_E2E_RUNNING:-}" != "1" ]]; then
    log() { echo "==> $*"; }

    log "building binaries (as $(whoami))..."
    "$CARGO" build --manifest-path "$REPO_ROOT/Cargo.toml" \
        -p distvirt-orchestrator --features bin \
        -p distvirt-worker \
        -p distvirt-cli

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

    log "re-executing under sudo..."
    exec sudo "${SUDO_FLAGS[@]}" env DISTVIRT_E2E_RUNNING=1 "$0" "$@"
fi

# --- Everything below runs as root ---

# --- Configuration ---

KERNEL="${KERNEL:-$REPO_ROOT/guest-image/result-kernel/vmlinux}"
ROOTFS="${ROOTFS:-$REPO_ROOT/guest-image/result-rootfs}"
CONTAINERD_SOCKET="${CONTAINERD_SOCKET:-/run/containerd/containerd.sock}"
FIRECRACKER_BIN="${FIRECRACKER_BIN:-firecracker}"
COMPOSE_FILE="$SCRIPT_DIR/e2e-compose.yaml"

TMPDIR_BASE="$(mktemp -d /tmp/distvirt-e2e.XXXXXX)"
ORCH_LOG="$TMPDIR_BASE/orchestrator.log"
WORKER_LOG="$TMPDIR_BASE/worker.log"
CONFIG_FILE="$TMPDIR_BASE/orchestrator.toml"

ORCH_PID=""
WORKER_PID=""

TARGET_DIR="$REPO_ROOT/target/debug"
DV="$TARGET_DIR/dv"
ORCH_BIN="$TARGET_DIR/distvirt-orchestrator"
WORKER_BIN="$TARGET_DIR/distvirt-worker"

# --- Helpers ---

log() {
    echo "==> $*"
}

log "logs directory: $TMPDIR_BASE"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

# Find a free TCP port.
find_free_port() {
    python3 -c '
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
'
}

# Wait for a TCP port to be listening (up to $2 seconds).
wait_for_port() {
    local port="$1"
    local timeout="${2:-30}"
    local deadline=$((SECONDS + timeout))
    while ! ss -tlnp 2>/dev/null | grep -q ":${port} " ; do
        if (( SECONDS >= deadline )); then
            fail "port $port not ready after ${timeout}s"
        fi
        sleep 0.5
    done
}

CONNECT_PID=""

cleanup() {
    log "cleaning up..."
    if [[ -n "$CONNECT_PID" ]] && kill -0 "$CONNECT_PID" 2>/dev/null; then
        kill "$CONNECT_PID" 2>/dev/null || true
        wait "$CONNECT_PID" 2>/dev/null || true
    fi
    if [[ -n "$WORKER_PID" ]] && kill -0 "$WORKER_PID" 2>/dev/null; then
        kill "$WORKER_PID" 2>/dev/null || true
        wait "$WORKER_PID" 2>/dev/null || true
    fi
    if [[ -n "$ORCH_PID" ]] && kill -0 "$ORCH_PID" 2>/dev/null; then
        kill "$ORCH_PID" 2>/dev/null || true
        wait "$ORCH_PID" 2>/dev/null || true
    fi
    if [[ -d "$TMPDIR_BASE" ]]; then
        log "logs available at: $TMPDIR_BASE"
    fi
}

trap cleanup EXIT

# --- Prerequisite checks ---

log "checking prerequisites..."
[[ -x "$DV" ]]        || fail "dv binary not found at $DV"
[[ -x "$ORCH_BIN" ]]  || fail "orchestrator binary not found at $ORCH_BIN"
[[ -x "$WORKER_BIN" ]] || fail "worker binary not found at $WORKER_BIN"
[[ -f "$KERNEL" ]]           || fail "kernel not found at $KERNEL"
[[ -d "$ROOTFS" ]] || [[ -f "$ROOTFS" ]] || fail "rootfs not found at $ROOTFS"
[[ -S "$CONTAINERD_SOCKET" ]] || fail "containerd socket not found at $CONTAINERD_SOCKET"
command -v "$FIRECRACKER_BIN" &>/dev/null || fail "firecracker not found (set FIRECRACKER_BIN)"

# --- Generate config ---

GRPC_PORT=$(find_free_port)
WORKER_PORT=$(find_free_port)
WG_PORT=$(find_free_port)

log "gRPC port: $GRPC_PORT, worker listener port: $WORKER_PORT"

cat > "$CONFIG_FILE" <<EOF
[grpc]
listen = "127.0.0.1:${GRPC_PORT}"

[workers]
listen = "127.0.0.1:${WORKER_PORT}"

[wireguard]
listen_port = ${WG_PORT}
EOF

# --- Start orchestrator ---

log "starting orchestrator..."
RUST_LOG="${RUST_LOG:-info}" "$ORCH_BIN" --config "$CONFIG_FILE" > "$ORCH_LOG" 2>&1 &
ORCH_PID=$!
wait_for_port "$GRPC_PORT" 30
log "orchestrator ready (pid $ORCH_PID)"

# --- Start worker ---

log "starting worker..."
RUST_LOG="${RUST_LOG:-info,distvirt_worker::adapter=trace,distvirt_worker::fabric::forwarding=trace}" "$WORKER_BIN" \
    --kernel "$KERNEL" \
    --rootfs-image "$ROOTFS" \
    --containerd-socket "$CONTAINERD_SOCKET" \
    --orchestrator "127.0.0.1:${WORKER_PORT}" \
    --public-endpoint "127.0.0.1" \
    > "$WORKER_LOG" 2>&1 &
WORKER_PID=$!
log "worker started (pid $WORKER_PID)"

SERVER="http://127.0.0.1:${GRPC_PORT}"

# --- Wait for worker registration ---

log "waiting for worker to register..."
DEADLINE=$((SECONDS + 60))
while true; do
    WORKERS=$("$DV" get workers --server "$SERVER" -o json 2>/dev/null || echo "")
    if echo "$WORKERS" | grep -q '"worker_id"'; then
        break
    fi
    if (( SECONDS >= DEADLINE )); then
        fail "worker did not register within 60s"
    fi
    sleep 2
done
log "worker registered"

# --- Deploy workload ---

NS="e2e-test-ns"

log "deploying workload (dv up $NS)..."
"$DV" up "$NS" --file "$COMPOSE_FILE" --server "$SERVER" 2>&1
log "deploy submitted"

# --- Wait for workloads running ---

log "waiting for workloads to become active..."
DEADLINE=$((SECONDS + 120))
LAST_STATUS=""
while true; do
    STATUS=$("$DV" status "$NS" --server "$SERVER" 2>/dev/null || echo "")
    # Log state changes for debugging
    if [[ "$STATUS" != "$LAST_STATUS" ]]; then
        echo "  status: $(echo "$STATUS" | grep 'workload/' || echo '(no workloads yet)')"
        LAST_STATUS="$STATUS"
    fi
    # Match "workload/... running" in status output
    if echo "$STATUS" | grep -q 'workload/.*running'; then
        break
    fi
    if (( SECONDS >= DEADLINE )); then
        echo "Last status output:"
        echo "$STATUS"
        echo "--- pod logs ---"
        timeout 5 "$DV" logs "$NS" --server "$SERVER" 2>&1 | tail -100 || true
        echo "--- namespace events ---"
        timeout 5 "$DV" events "$NS" --server "$SERVER" 2>&1 | tail -50 || true
        fail "workloads did not reach running state within 120s"
    fi
    sleep 3
done
log "workloads running"

# --- Dump pod logs for debugging ---

log "fetching pod logs..."
timeout 5 "$DV" logs "$NS" --server "$SERVER" 2>&1 | head -50 || true
echo ""

# --- Connect and verify network reachability ---

log "getting pod IP..."
POD_JSON=$("$DV" get pods --namespace "$NS" --server "$SERVER" -o json 2>/dev/null) || true
POD_IP=$(echo "$POD_JSON" | python3 -c "
import sys, json
pods = json.load(sys.stdin)
ip = pods[0]['ip']
if not ip:
    print('ERROR: pod IP is empty', file=sys.stderr)
    sys.exit(1)
print(ip)
" 2>&1) || true
[[ -n "$POD_IP" && "$POD_IP" != ERROR* ]] || fail "could not determine pod IP from: $POD_JSON"
log "pod IP: $POD_IP"

log "starting dv connect..."
CONNECT_LOG="$TMPDIR_BASE/connect.log"
RUST_LOG="${RUST_LOG:-info,dv::commands::connect=trace}" "$DV" connect "$NS" --server "$SERVER" > "$CONNECT_LOG" 2>&1 &
CONNECT_PID=$!

# Wait for TUN device and route to be set up.
# dv connect sets up a subnet route (e.g. 172.16.0.0/24), not a per-pod route,
# so we check for the subnet rather than the specific pod IP.
POD_SUBNET=$(echo "$POD_IP" | sed 's/\.[0-9]*$/.0\/24/')
DEADLINE=$((SECONDS + 30))
while ! ip route | grep -q "$POD_SUBNET"; do
    if ! kill -0 "$CONNECT_PID" 2>/dev/null; then
        echo "dv connect output:"
        cat "$CONNECT_LOG"
        fail "dv connect exited unexpectedly"
    fi
    if (( SECONDS >= DEADLINE )); then
        echo "dv connect output:"
        cat "$CONNECT_LOG"
        fail "dv connect did not set up routes within 30s"
    fi
    sleep 1
done
log "tunnel established"

# Give WireGuard handshake a moment to complete.
sleep 2

log "testing connectivity to pod at $POD_IP:8080..."
HTTP_RESP=$(curl -s --connect-timeout 10 --max-time 15 "http://${POD_IP}:8080/" 2>&1) || true
if echo "$HTTP_RESP" | grep -q "ok"; then
    log "connectivity test PASSED"
else
    echo "HTTP response: $HTTP_RESP"
    echo "--- pod logs ---"
    timeout 5 "$DV" logs "$NS" --server "$SERVER" 2>&1 | tail -100 || true
    echo "--- dv connect output ---"
    cat "$CONNECT_LOG"
    fail "could not reach pod via dv connect tunnel"
fi

# Tear down the tunnel.
log "disconnecting..."
kill "$CONNECT_PID" 2>/dev/null || true
wait "$CONNECT_PID" 2>/dev/null || true
CONNECT_PID=""
log "disconnected"

# --- Inspect ---

log "inspecting resources..."

echo "--- get namespaces ---"
"$DV" get namespaces --server "$SERVER" 2>&1
echo ""

echo "--- get workers ---"
"$DV" get workers --server "$SERVER" 2>&1
echo ""

echo "--- get pods --namespace $NS ---"
"$DV" get pods --namespace "$NS" --server "$SERVER" 2>&1
echo ""

echo "--- describe namespace $NS ---"
"$DV" describe namespaces "$NS" --server "$SERVER" 2>&1
echo ""

# --- Teardown ---

log "tearing down (dv down $NS)..."
"$DV" down "$NS" --server "$SERVER" 2>&1
log "teardown submitted"

# --- Verify teardown ---

log "verifying namespace removed..."
DEADLINE=$((SECONDS + 30))
while true; do
    NS_LIST=$("$DV" get namespaces --server "$SERVER" 2>/dev/null || echo "")
    if ! echo "$NS_LIST" | grep -q "$NS"; then
        break
    fi
    if (( SECONDS >= DEADLINE )); then
        fail "namespace $NS still present after 30s"
    fi
    sleep 2
done
log "namespace removed"

# --- Done ---

log "full-stack E2E test PASSED"
exit 0
