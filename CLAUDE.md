# distvirt

## Testing

### E2E tests (distvirt-worker)

E2E tests require root privileges (TUN devices, bind mounts, KVM access).

Run via the wrapper script which handles sudo with a GUI askpass dialog:

```sh
./distvirt-worker/tests/run-e2e.sh
```

Extra args are forwarded to `cargo test`:

```sh
./distvirt-worker/tests/run-e2e.sh -- test_launch_pod_echo
```

Prerequisites:
- `firecracker` binary (or `FIRECRACKER_BIN` env var)
- Running containerd (or `CONTAINERD_SOCKET` env var)
- Built kernel at `guest-image/result-kernel/bzImage`
- Built rootfs at `guest-image/result-rootfs`
