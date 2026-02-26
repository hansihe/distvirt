{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    root.url = "path:../.";
    root.flake = false;
  };

  outputs = { self, nixpkgs, root }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};

      guestKernel = (pkgs.linuxManualConfig {
        src = pkgs.linux_latest.src;
        version = pkgs.linux_latest.version;
        configfile = ./guest-kernel.config;
        allowImportFromDerivation = true;
      }).overrideAttrs (old: {
        postInstall = (old.postInstall or "") + ''
          cp vmlinux $out/
        '';
      });

      guestInit = pkgs.pkgsStatic.rustPlatform.buildRustPackage {
        pname = "guest-init";
        version = "0.1.0";
        src = pkgs.lib.cleanSource root;
        cargoLock.lockFile = root + "/Cargo.lock";
        buildAndTestSubdir = "guest-image/guest-init";
        cargoBuildProfileFlag = "--profile guest";
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
    in
    {
      packages.${system} = {
        inherit guestKernel guestInit guestRootfsImage;
      };
    };
}
