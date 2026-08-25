# Installation

## Prerequisites

- [tmux](https://github.com/tmux/tmux/wiki) (required)
- [Docker](https://www.docker.com/) (optional, for sandboxing agents in containers)
- [Node.js](https://nodejs.org/) (required only when building from source: every binary embeds the web dashboard, so the frontend is built during compilation)

## Install Agent of Empires

### Quick Install (Recommended)

Run the install script:

```bash
curl -fsSL \
  https://raw.githubusercontent.com/agent-of-empires/agent-of-empires/main/scripts/install.sh \
  | bash
```

### Homebrew

```bash
brew install aoe
```

### Build from Source

```bash
git clone https://github.com/agent-of-empires/agent-of-empires
cd agent-of-empires
cargo build --release
```

The binary will be at `target/release/aoe`.

Building from source requires Node.js and npm: every binary embeds the web
dashboard, and the frontend is built automatically during compilation. If you
package aoe and cannot run npm, set `AOE_WEB_DIST` to a directory holding a
prebuilt dashboard and the build will copy that instead.

## Verify Installation

```bash
aoe --version
```

## Updating

```bash
aoe update
```

The `aoe update` command detects how aoe was installed (Homebrew, the curl install script, Nix, or Cargo) and dispatches to the right upgrade mechanism. For Nix and Cargo it prints the manual upgrade command instead of attempting an automatic update, since those cases need external tooling.

Inside the TUI, press `u` when the update bar is visible to run the same flow without leaving the app. Press `Ctrl+x` to dismiss the bar for the current session.

If you installed shell completions as a static file, regenerate it after an update so it picks up new commands and flags. See [Shell Completions](guides/shell-completions.md) for both the static and the always-fresh eval-on-startup setup.

## Uninstall

```bash
aoe uninstall
```

Prompts to remove the binary, configuration (the app data dir), and tmux settings.
