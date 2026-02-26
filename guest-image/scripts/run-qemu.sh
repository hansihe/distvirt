#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Building kernel and rootfs..."
nix build "$ROOT_DIR#guestKernel" -o "$ROOT_DIR/result-kernel"
nix build "$ROOT_DIR#guestRootfsImage" -o "$ROOT_DIR/result-rootfs"

KERNEL="$ROOT_DIR/result-kernel/bzImage"
ROOTFS_ORIG="$ROOT_DIR/result-rootfs"

# Copy rootfs to temp file (nix store is read-only)
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT
cp "$ROOTFS_ORIG" "$TMPDIR/rootfs.ext4"
chmod 644 "$TMPDIR/rootfs.ext4"

echo "Starting QEMU..."
ACCEL_ARGS=()
if [ -w /dev/kvm ]; then
    ACCEL_ARGS+=(-accel kvm -cpu host)
    echo "  Using KVM acceleration"
else
    ACCEL_ARGS+=(-accel tcg)
    echo "  KVM not available, using TCG (slower)"
fi

qemu-system-x86_64 \
    -M q35 \
    "${ACCEL_ARGS[@]}" \
    -m 128M \
    -smp 1 \
    -kernel "$KERNEL" \
    -append "console=ttyS0 reboot=k panic=1 root=/dev/vda ro init=/sbin/init" \
    -drive file="$TMPDIR/rootfs.ext4",format=raw,if=virtio \
    -device vhost-vsock-pci,guest-cid=3 \
    -serial stdio \
    -no-reboot \
    -nographic \
    -nodefaults \
    -display none
