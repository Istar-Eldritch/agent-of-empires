{
  description = "Terminal session manager for AI coding agents";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    crane.url = "github:ipetkov/crane";
  };

  outputs = inputs @ { flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ];

      perSystem = { config, self', inputs', pkgs, system, ... }:
        let
          craneLib = inputs.crane.mkLib pkgs;

          # git2 uses vendored-openssl (needs perl to build OpenSSL)
          # and libgit2-sys vendors libgit2 (needs cmake to build it)
          nativeBuildInputs = with pkgs; [
            pkg-config
            perl
            cmake
            installShellFiles
          ];

          buildInputs = with pkgs; [
            zlib # required by vendored libgit2
          ];

          commonArgs = {
            # Cargo source filtering keeps only *.rs, *.toml and Cargo.lock, so
            # every other compile-time embedded asset has to be unioned in
            # explicitly: the acp-worker/adapters manifests that
            # src/acp/adapters.rs reads with include_bytes! (#3204), and
            # docker/Dockerfile, which the agent_compat test embeds to pin the
            # sandbox npm floor (the aoe-test and aoe-clippy checks compile test
            # code, so they need it even though the packages do not).
            # `scripts/check-nix-embedded-assets.py` fails CI if a new embedded
            # asset lands without being added here.
            src = pkgs.lib.fileset.toSource {
              root = ./.;
              fileset = pkgs.lib.fileset.unions [
                (craneLib.fileset.commonCargoSources ./.)
                ./acp-worker/adapters
                ./docker
              ];
            };
            strictDeps = true;
            inherit nativeBuildInputs buildInputs;
            # build.rs always builds and embeds the web bundle (#2170), and the
            # Nix sandbox has no network for npm, so every derivation that
            # compiles the crate is pointed at the prebuilt `webFrontend`
            # instead. That includes the clippy and test checks, which used to
            # compile the dashboard out and so skipped the frontend step
            # entirely. `buildDepsOnly` uses a dummy crate source, so this is a
            # no-op there.
            AOE_WEB_DIST = "${webFrontend}/dist";
          };

          # Build only workspace dependencies first (for caching)
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;

          aoe = craneLib.buildPackage (commonArgs // {
            inherit cargoArtifacts;
            cargoExtraArgs = "--package agent-of-empires";
            doCheck = false;
            # Expose npmDeps so `nix-update` can automatically recompute the
            # npmDepsHash in webFrontend when web/package-lock.json changes.
            passthru.npmDeps = webFrontend.npmDeps;
            postInstall = ''
              installShellCompletion --cmd aoe \
                --bash <($out/bin/aoe completion bash) \
                --fish <($out/bin/aoe completion fish) \
                --zsh <($out/bin/aoe completion zsh)
            '';

            meta = with pkgs.lib; {
              description = "Terminal session manager for AI coding agents";
              longDescription = ''
                Agent of Empires (AoE) is a terminal session manager for AI coding
                agents on Linux and macOS. Built on tmux, it allows running multiple
                AI agents in parallel across different branches of your codebase,
                each in its own isolated session with optional Docker sandboxing.

                Supports Claude Code, OpenCode, Mistral Vibe, Codex CLI, and Gemini CLI.
              '';
              homepage = "https://github.com/agent-of-empires/agent-of-empires";
              license = licenses.mit;
              platforms = platforms.unix;
              mainProgram = "aoe";
            };
          });

          # Build the React frontend as a standalone derivation (Forgejo/ntfy-sh pattern).
          # This separates the npm build from the Rust build cleanly.
          #
          # Update npmDepsHash whenever web/package-lock.json changes:
          #   nix-update default
          # or manually: set npmDepsHash to lib.fakeHash, build, copy the got: hash.
          webFrontend = pkgs.buildNpmPackage {
            pname = "agent-of-empires-web";
            version = "0";
            src = ./web;
            npmDepsHash = "sha256-2yEWUP278jgQaqeFuBxQ81xs+5KXsIHnPMfnHpGNSVc=";
            # tsc -b && vite build; output goes to web/dist
            installPhase = ''
              mkdir $out
              cp -r dist $out/
            '';
          };
        in
        {
          packages.default = aoe;
          # Just the npm + vite build. Exposed so the PR-CI Nix Build
          # Web job can validate npmDepsHash + frontend build in ~1-2
          # min instead of rebuilding the full Rust workspace.
          packages.aoe-web-frontend = webFrontend;
          # Exposed so the nix-npm-hash bots and the local manual
          # update procedure use the same nixpkgs revision as
          # `buildNpmPackage` above.
          packages.prefetch-npm-deps = pkgs.prefetch-npm-deps;

          checks = {
            # Build the package as a check too
            inherit aoe;

            aoe-clippy = craneLib.cargoClippy (commonArgs // {
              inherit cargoArtifacts;
              # e2e-tests keeps the gated e2e target inside the --all-targets
              # sweep; without it the required-features gate would silently drop
              # e2e from clippy's --deny warnings coverage.
              cargoClippyExtraArgs = "--package agent-of-empires --all-targets --features e2e-tests -- --deny warnings";
            });

            aoe-fmt = craneLib.cargoFmt {
              inherit (commonArgs) src;
            };

            aoe-test = craneLib.cargoTest (commonArgs // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--package agent-of-empires";
              # Some git:: unit tests invoke the git binary directly
              nativeBuildInputs = commonArgs.nativeBuildInputs ++ [ pkgs.git ];
            });
          };

          devShells.default = craneLib.devShell {
            checks = self'.checks;
            packages = with pkgs; [
              rust-analyzer
              tmux
              nodejs # for web frontend development
            ];
          };
        };
    };
}
