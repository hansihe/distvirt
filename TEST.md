# 1. Build guest kernel + rootfs (via nix)
cd guest-image
nix build .#guestKernel -o result-kernel
nix build .#guestRootfsImage -o result-rootfs

# 2. Get a container rootfs (Alpine minirootfs)
mkdir -p /tmp/alpine-rootfs
curl -L https://dl-cdn.alpinelinux.org/alpine/v3.21/releases/x86_64/alpine-minirootfs-3.21.3-x86_64.tar.gz | tar xz -C /tmp/alpine-rootfs

# 3. Build the container ext4 image
cargo run -p distvirt-cli -- build-image --rootfs /tmp/alpine-rootfs --output /tmp/container.ext4

# 4. Run it (needs KVM + firecracker in PATH)
cargo run -p distvirt-cli -- run \
  --kernel guest-image/result-kernel/vmlinux \
  --rootfs-image guest-image/result-rootfs \
  --container-rootfs /tmp/alpine-rootfs \
  --entrypoint /bin/sh \
  --args -c --args "echo hello from container"

Regarding containerd — yes, you could use it to pull and unpack OCI images. A few options:

- ctr image pull + ctr image mount — pulls an image and mounts it as an overlay/snapshot you can point at as a rootfs directory
- nerdctl export — can export a container rootfs as a tarball
- containerd's snapshotter API — for devmapper snapshots, containerd can prepare thin volumes directly, skipping the ext4 build entirely (the thin volume is the block device you pass to the VM)

The simplest path for now would be something like:

# Pull and unpack with ctr
sudo ctr image pull docker.io/library/alpine:latest
sudo ctr image mount docker.io/library/alpine:latest /tmp/alpine-rootfs

# Then use that as --container-rootfs

The devmapper snapshotter path is more interesting long-term since it avoids building ext4 images entirely — containerd gives you a block device directly, which you'd pass straight to Firecracker as the second virtio-blk drive.
