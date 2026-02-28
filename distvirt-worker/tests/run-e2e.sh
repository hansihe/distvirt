#!/usr/bin/env bash
# Run distvirt-worker E2E tests under sudo.
#
# These tests require root for TUN devices, bind mounts, and KVM access.
# Uses a graphical askpass dialog when available (e.g. ksshaskpass),
# so privilege escalation can be approved via a GUI popup.
#
# Usage: ./distvirt-worker/tests/run-e2e.sh [extra cargo test args...]

set -euo pipefail

CARGO="$(which cargo)"

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

exec sudo "${SUDO_FLAGS[@]}" env DISTVIRT_E2E=1 "$CARGO" test --package distvirt-worker --test e2e "$@"
