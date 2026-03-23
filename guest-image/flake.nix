{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    root.url = "path:../.";
    root.flake = false;
  };

  outputs = { self, nixpkgs, root }:
    let
      forSystems = nixpkgs.lib.genAttrs [ "x86_64-linux" "aarch64-linux" ];

      mkPackages = system:
        let
          pkgs = nixpkgs.legacyPackages.${system};

          kernelConfigFile = {
            x86_64-linux = ./guest-kernel.config;
            aarch64-linux = ./guest-kernel-aarch64.config;
          }.${system};

          # Firecracker expects vmlinux on x86_64, Image on aarch64
          kernelImagePath = {
            x86_64-linux = "vmlinux";
            aarch64-linux = "arch/arm64/boot/Image";
          }.${system};

          guestKernel = (pkgs.linuxManualConfig {
            src = pkgs.linux_latest.src;
            version = pkgs.linux_latest.version;
            configfile = kernelConfigFile;
            allowImportFromDerivation = true;
          }).overrideAttrs (old: {
            patches = (old.patches or []) ++ [
              ./patches/virtio-balloon-sysfs-notify.patch
            ];
            postInstall = (old.postInstall or "") + ''
              cp ${kernelImagePath} $out/
            '';
          });

          guestInit = pkgs.pkgsStatic.rustPlatform.buildRustPackage {
            pname = "guest-init";
            version = "0.1.0";
            src = pkgs.lib.cleanSource root;
            cargoLock.lockFile = root + "/Cargo.lock";
            cargoLock.outputHashes = {
              "containerd-client-0.8.0" = "sha256-b1qEaf35FQQNCxGKAv3AB5Mc8FGLoxCotVx0yFItYxk=";
            };
            buildAndTestSubdir = "crates/guest-init";
            cargoBuildProfileFlag = "--profile guest";
          };

          kernelSrc = pkgs.runCommand "linux-src" {} ''
            mkdir $out
            tar -xf ${pkgs.linux_latest.src} --strip-components=1 -C $out
          '';

          # Script to run `make olddefconfig` on the guest kernel config using the
          # same kernel source as the build.  Usage: just run `nix run .#kernel-olddefconfig`
          # from the guest-image directory (or pass the config path as $1).
          kernel-olddefconfig = pkgs.writeShellScriptBin "kernel-olddefconfig" ''
            set -euo pipefail
            export PATH="${pkgs.lib.makeBinPath [ pkgs.gnumake pkgs.gcc pkgs.flex pkgs.bison pkgs.pkg-config pkgs.bc ]}:$PATH"
            CONFIG="''${1:-$(pwd)/${if system == "aarch64-linux" then "guest-kernel-aarch64.config" else "guest-kernel.config"}}"
            if [ ! -f "$CONFIG" ]; then
              echo "error: config not found: $CONFIG" >&2
              exit 1
            fi
            TMPDIR=$(mktemp -d)
            trap 'rm -rf "$TMPDIR"' EXIT
            cp "$CONFIG" "$TMPDIR/.config"
            make -C ${kernelSrc} O="$TMPDIR" olddefconfig
            cp "$TMPDIR/.config" "$CONFIG"
            echo "updated $CONFIG"
          '';

          testContainers = pkgs.pkgsStatic.rustPlatform.buildRustPackage {
            pname = "test-containers";
            version = "0.1.0";
            src = pkgs.lib.cleanSource root + "/guest-image/test-containers";
            cargoLock.lockFile = root + "/guest-image/test-containers/Cargo.lock";
            cargoBuildProfileFlag = "--profile guest";
          };

          testContainerImage = pkgs.dockerTools.buildImage {
            name = "distvirt-test-containers";
            tag = "latest";
            copyToRoot = pkgs.buildEnv {
              name = "test-containers-root";
              paths = [
                testContainers
                (pkgs.runCommand "base-dirs" {} "mkdir -p $out/tmp")
              ];
              pathsToLink = [ "/bin" "/tmp" ];
            };
          };

          guestRootfsImage = pkgs.runCommand "guest-rootfs.ext4" {
            nativeBuildInputs = [ pkgs.e2fsprogs ];
          } ''
            mkdir -p rootfs/sbin rootfs/dev rootfs/proc rootfs/sys rootfs/tmp

            cp ${guestInit}/bin/init rootfs/sbin/init

            truncate -s 64M $out
            mkfs.ext4 -d rootfs $out
            resize2fs -M $out
          '';
        in {
          inherit guestKernel guestInit guestRootfsImage kernel-olddefconfig testContainers testContainerImage;
        };
    in
    {
      packages = forSystems mkPackages;
    };
}
