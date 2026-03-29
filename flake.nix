{
  description = "btrfs-progrs — Rust reimplementation of the btrfs CLI tool";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs = { self, nixpkgs, rust-overlay, crane }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f system);
    in
    {
      packages = forAllSystems (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };

          toolchainToml = builtins.fromTOML (builtins.readFile ./rust-toolchain.toml);
          channel = toolchainToml.toolchain.channel;

          # rust-overlay expects "stable", "beta", or "nightly" as the
          # top-level key. A pinned version like "1.94.1" lives under
          # pkgs.rust-bin.stable."1.94.1".
          rustToolchain =
            let
              isVersion = builtins.match "[0-9]+\\.[0-9]+\\.[0-9]+" channel != null;
              base = if isVersion
                then pkgs.rust-bin.stable.${channel}.default
                else pkgs.rust-bin.${channel}.latest.default;
            in base.override {
              extensions = toolchainToml.toolchain.components or [];
              targets = toolchainToml.toolchain.targets or [];
            };

          craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

          # Common source filtering — keep Rust files, C headers (for bindgen),
          # snapshot files (for insta tests), and fixture images.
          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter = path: type:
              let base = builtins.baseNameOf path; in
              (craneLib.filterCargoSources path type)
              || builtins.match ".*\\.(h|c)$" base != null
              || builtins.match ".*\\.snap$" base != null
              || builtins.match ".*\\.snap\\.new$" base != null
              || builtins.match ".*\\.img\\.gz$" base != null;
          };

          commonArgs = {
            inherit src;
            pname = "btrfs";
            strictDeps = true;

            nativeBuildInputs = [
              pkgs.llvmPackages.libclang
              pkgs.clang
              pkgs.gzip
              pkgs.installShellFiles
            ];

            buildInputs = [
              pkgs.linuxHeaders
            ];

            LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
            BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${pkgs.linuxHeaders}/include";
          };

          # Build dependencies separately so they're cached across rebuilds
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;

          btrfs = craneLib.buildPackage (commonArgs // {
            inherit cargoArtifacts;

            # After building, generate man pages and install them
            postInstall = ''
              # mangen is publish=false and not in default-members, so the
              # binary won't be in $out/bin. Build and run it manually.
              cargo run --package btrfs-mangen -- man-pages
              installManPage man-pages/*.1
            '';
          });
        in
        {
          default = btrfs;
          inherit btrfs;
        });

      devShells = forAllSystems (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };

          toolchainToml = builtins.fromTOML (builtins.readFile ./rust-toolchain.toml);
          channel = toolchainToml.toolchain.channel;

          rustToolchain =
            let
              isVersion = builtins.match "[0-9]+\\.[0-9]+\\.[0-9]+" channel != null;
              base = if isVersion
                then pkgs.rust-bin.stable.${channel}.default
                else pkgs.rust-bin.${channel}.latest.default;
            in base.override {
              extensions = toolchainToml.toolchain.components or [];
              targets = toolchainToml.toolchain.targets or [];
            };

          # Nightly just for cargo fmt (Justfile runs `cargo +nightly fmt`)
          rustNightly = pkgs.rust-bin.nightly.latest.default.override {
            extensions = [ "rustfmt" ];
          };
        in
        {
          default = pkgs.mkShell {
            buildInputs = [
              rustToolchain
              rustNightly

              # bindgen needs libclang
              pkgs.llvmPackages.libclang
              pkgs.clang

              # build/dev tools
              pkgs.just
              pkgs.jq
              pkgs.cargo-insta
              pkgs.cargo-llvm-cov
            ];

            env = {
              # Tell bindgen where to find libclang
              LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
            };
          };
        });
    };
}
