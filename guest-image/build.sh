#!/bin/bash
set -xe

nix build ".#guestRootfsImage" -o result-rootfs
nix build ".#guestKernel" -o result-kernel
nix build ".#testContainerImage" -o result-test-containers
