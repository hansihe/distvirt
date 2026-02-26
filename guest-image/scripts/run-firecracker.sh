#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Building kernel and rootfs..."
nix build "$ROOT_DIR#guestKernel" -o "$ROOT_DIR/result-kernel"
nix build "$ROOT_DIR#guestRootfsImage" -o "$ROOT_DIR/result-rootfs"

KERNEL="$ROOT_DIR/result-kernel/vmlinux"
ROOTFS_ORIG="$ROOT_DIR/result-rootfs"

# Firecracker needs writable rootfs and its own API socket
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT
cp "$ROOTFS_ORIG" "$TMPDIR/rootfs.ext4"
chmod 644 "$TMPDIR/rootfs.ext4"

SOCKET="$TMPDIR/firecracker.sock"

if [ ! -w /dev/kvm ]; then
    echo "Error: Firecracker requires KVM. Use scripts/run-qemu.sh instead."
    exit 1
fi

echo "Starting Firecracker..."
firecracker --api-sock "$SOCKET" &
FC_PID=$!
trap "kill $FC_PID 2>/dev/null; rm -rf $TMPDIR" EXIT

# Wait for socket
for i in $(seq 1 20); do
    [ -e "$SOCKET" ] && break
    sleep 0.1
done

# Configure boot source
curl -s --unix-socket "$SOCKET" -X PUT "http://localhost/boot-source" \
    -H "Content-Type: application/json" \
    -d "{
        \"kernel_image_path\": \"$KERNEL\",
        \"boot_args\": \"console=ttyS0 reboot=k panic=1 pci=off init=/sbin/init\"
    }"

# Configure root drive
curl -s --unix-socket "$SOCKET" -X PUT "http://localhost/drives/rootfs" \
    -H "Content-Type: application/json" \
    -d "{
        \"drive_id\": \"rootfs\",
        \"path_on_host\": \"$TMPDIR/rootfs.ext4\",
        \"is_root_device\": true,
        \"is_read_only\": false
    }"

# Configure machine
curl -s --unix-socket "$SOCKET" -X PUT "http://localhost/machine-config" \
    -H "Content-Type: application/json" \
    -d "{
        \"vcpu_count\": 1,
        \"mem_size_mib\": 128
    }"

# Start the VM
curl -s --unix-socket "$SOCKET" -X PUT "http://localhost/actions" \
    -H "Content-Type: application/json" \
    -d "{
        \"action_type\": \"InstanceStart\"
    }"

# Wait for Firecracker to exit
wait $FC_PID || true
