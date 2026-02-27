#!/bin/bash
set -xe

nix run ".#kernel-olddefconfig"
