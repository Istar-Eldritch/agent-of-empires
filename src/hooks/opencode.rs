//! opencode status hooks, installed as an opencode plugin.
//!
//! opencode exposes no shell-hook surface at all: its config schema has no
//! `hook` key, and the only way to observe its internal state is the JS plugin
//! system. AoE therefore ships a generated plugin that translates opencode's
//! own events into the same sidecar status file every other agent's hooks
//! write (`/tmp/aoe-hooks-<euid>/$AOE_INSTANCE_ID/status`).
//!
//! **The plugin does not write the status file itself.** It shells out to the
//! canonical hardened writer ([`super::hook_command`]) that Claude, Codex,
//! hermes, kiro and kimi all use, baked into the generated source as string
//! literals. One implementation of the instance-id validation, the
//! host-vs-sandbox base, the squatted-directory checks and the never-exit-nonzero
//! contract, and the `# aoe-hooks` marker rides along for free. The alternative,
//! reimplementing that writer in JS, would put security-sensitive filesystem
//! code in a second language inside an asset this repo has no test harness for.
//!
//! Discovery: opencode globs `{plugin,plugins}/*.{ts,js}` in every config
//! directory it resolves. `plugin/` (singular) is the backward-compatible
//! spelling; opencode 1.0 globbed only that name. Nothing needs to be added to
//! the user's `opencode.json`.

use std::path::{Path, PathBuf};

use anyhow::Result;

use super::{hook_command, resolve_config_dir_override, with_config_lock, HookInstallTarget};

/// Host fallback for `opencode_plugin_path_in`, and the declared
/// `AgentStatusIntegration::host_config_subpath` for opencode. Only used when
/// `XDG_CONFIG_HOME` is unset.
pub const OPENCODE_PLUGIN_SUBPATH: &str = ".config/opencode/plugin/aoe-status.js";

/// Sandbox staging path for the plugin. Lands inside the `sandbox` staging
/// directory of opencode's `AgentConfigMount` (`.config/opencode`), which is
/// bind-mounted at `~/.config/opencode` in the container, so the in-container
/// opencode discovers it at `~/.config/opencode/plugin/aoe-status.js`.
/// Deliberately not XDG-aware: the staging location is fixed by the mount, and
/// the container has no `XDG_CONFIG_HOME`.
pub const OPENCODE_PLUGIN_SANDBOX_SUBPATH: &str = ".config/opencode/sandbox/plugin/aoe-status.js";

/// Ownership marker. Install refuses to clobber a file that lacks it, and
/// uninstall deletes only a file that carries it. Distinct from the
/// `# aoe-hooks` sentinel inside the embedded shell commands: ownership of a
/// whole file AoE generates should not hinge on a substring buried in a
/// generated string literal.
const AOE_PLUGIN_MARKER: &str = "aoe-opencode-status-plugin:v1";

/// Resolve the host plugin path, honoring `XDG_CONFIG_HOME` the way opencode
/// itself does (`xdg-basedir`: the variable when set and non-empty, else
/// `~/.config`).
pub(crate) fn opencode_plugin_path_in(home: &Path, host_env: &[String]) -> PathBuf {
    opencode_config_dir_in(home, host_env)
        .join("plugin")
        .join("aoe-status.js")
}

fn opencode_config_dir_in(home: &Path, host_env: &[String]) -> PathBuf {
    match resolve_config_dir_override("XDG_CONFIG_HOME", host_env) {
        Some(xdg) => PathBuf::from(xdg).join("opencode"),
        None => home.join(".config").join("opencode"),
    }
}

/// Marker-presence gate for [`super::has_aoe_marker`].
pub(crate) fn opencode_plugin_has_aoe_marker(path: &Path) -> bool {
    std::fs::read_to_string(path).is_ok_and(|content| content.contains(AOE_PLUGIN_MARKER))
}

/// Write the AoE status plugin, replacing any prior AoE-generated copy.
///
/// The whole file is AoE's, so this is a replace rather than a merge. Refuses
/// to overwrite a file without `AOE_PLUGIN_MARKER`: that path belongs to
/// someone else, and clobbering a user's plugin to report status would be a bad
/// trade. Skips the write when the rendered bytes already match, so a
/// re-launch does not churn the file's mtime.
pub fn install_opencode_plugin_with_events(
    path: &Path,
    target: HookInstallTarget,
    events: &[crate::agents::ResolvedHookEvent],
) -> Result<()> {
    with_config_lock(path, "js.lock", || {
        if path.exists() && !opencode_plugin_has_aoe_marker(path) {
            anyhow::bail!(
                "refusing to overwrite {}: it is not an AoE-generated plugin",
                path.display()
            );
        }

        let rendered = render_opencode_plugin(target, events)?;
        if std::fs::read(path).is_ok_and(|existing| existing == rendered.as_bytes()) {
            tracing::debug!(target: "hooks.install",
                "AoE opencode plugin at {} already up to date; skipping write",
                path.display());
            return Ok(());
        }

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        crate::session::atomic_write(path, rendered.as_bytes())?;
        tracing::info!(target: "hooks.install", "Installed AoE opencode plugin at {}", path.display());
        Ok(())
    })
}

/// Remove the AoE status plugin. Leaves a file that is not ours in place.
///
/// Symlink handling mirrors the install side, which writes through a link
/// because `crate::session::atomic_write` resolves the chain first. Deleting
/// only the link would strand AoE's generated content at the target; deleting
/// only the target would leave a dangling link that opencode still globs, so it
/// would log a plugin load failure on every launch. Both go.
pub fn uninstall_opencode_plugin(path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }

    with_config_lock(path, "js.lock", || {
        if !opencode_plugin_has_aoe_marker(path) {
            return Ok(false);
        }
        let target = crate::session::resolve_symlink_chain(path)?;
        std::fs::remove_file(&target)?;
        if target != *path {
            // Best effort: the plugin content is already gone, and a failure
            // here leaves a dangling link that is noisy but not harmful.
            if let Err(e) = std::fs::remove_file(path) {
                tracing::warn!(target: "hooks.uninstall",
                    "Removed {} but could not remove the symlink at {}: {}",
                    target.display(), path.display(), e);
            }
        }
        tracing::info!(target: "hooks.uninstall", "Removed AoE opencode plugin at {}", path.display());
        Ok(true)
    })
}

/// Render the plugin source: a fixed body plus the generated event-to-command
/// map. Commands are emitted through `serde_json` so no JS escaping is hand
/// rolled.
fn render_opencode_plugin(
    target: HookInstallTarget,
    events: &[crate::agents::ResolvedHookEvent],
) -> Result<String> {
    let mut running = Vec::new();
    let mut idle = Vec::new();
    let mut other = serde_json::Map::new();

    for event in events {
        let Some(status) = event.status else {
            continue;
        };
        let command = serde_json::Value::String(hook_command(status.as_str(), target));
        match status {
            // Running and Idle drive the active-session bookkeeping in the
            // plugin body, so they are grouped rather than looked up by key.
            crate::agents::HookStatus::Running => running.push((event.name.clone(), command)),
            crate::agents::HookStatus::Idle => idle.push((event.name.clone(), command)),
            _ => {
                other.insert(event.name.clone(), command);
            }
        }
    }

    let running_json = serde_json::to_string(&serde_json::Map::from_iter(running))?;
    let idle_json = serde_json::to_string(&serde_json::Map::from_iter(idle))?;
    let other_json = serde_json::to_string(&other)?;

    Ok(format!(
        r#"// Generated by Agent of Empires. Do not edit; changes are overwritten.
// {AOE_PLUGIN_MARKER}
//
// Reports this opencode session's activity status to AoE. Outside an AoE
// session AOE_INSTANCE_ID is unset, so this registers no handlers at all and
// costs nothing on your own opencode runs.
//
// Delete this file to remove it, or run `aoe uninstall`.

const RUNNING = {running_json};
const IDLE = {idle_json};
const OTHER = {other_json};

// Serialized on purpose. Each command is a separate process, and unawaited
// spawns can exit out of order, which would let a stale `running` land after
// the `idle` that followed it and pin the session as busy forever.
let queue = Promise.resolve();
function write(command) {{
  queue = queue
    .catch(() => {{}})
    .then(() =>
      Bun.spawn(["/bin/sh", "-c", command], {{
        stdin: "ignore",
        stdout: "ignore",
        stderr: "ignore",
      }}).exited,
    );
  return queue;
}}

// Session ids with a turn in flight. opencode runs subagents (the task tool)
// as their own sessions that emit their own idle, so idle is only reported once
// every tracked session has gone quiet; otherwise a finished subagent would
// flash the whole session Idle mid-turn.
const active = new Set();

function sessionIdOf(event) {{
  return event && event.properties && event.properties.sessionID;
}}

// `session.status` carries the status in the payload, so it fans out to one
// key per variant (`session.status:busy` and so on) and each is separately
// overridable through a profile status_map.
function keysFor(event) {{
  if (event.type === "session.status") {{
    const type = event.properties && event.properties.status && event.properties.status.type;
    return type ? [`session.status:${{type}}`, event.type] : [event.type];
  }}
  return [event.type];
}}

function report(keys, sessionId) {{
  for (const key of keys) {{
    if (key in RUNNING) {{
      if (sessionId) active.add(sessionId);
      return write(RUNNING[key]);
    }}
    if (key in IDLE) {{
      if (sessionId) active.delete(sessionId);
      // A still-busy sibling session means the turn is not over.
      if (active.size > 0) return undefined;
      return write(IDLE[key]);
    }}
    if (key in OTHER) return write(OTHER[key]);
  }}
  return undefined;
}}

// Register nothing outside an AoE session. The writer command bails on an
// unset AOE_INSTANCE_ID too, but that guard is inside the spawned shell, so
// without this early-out every non-AoE opencode run on the machine would still
// fork a process per status transition. AoE injects the variable for opencode
// (status_hook_env_prefix), so its absence means this is not our session.
export const AoeStatus = async () => {{
  if (!process.env.AOE_INSTANCE_ID) return {{}};
  return {{
  event: async ({{ event }}) => {{
    try {{
      await report(keysFor(event), sessionIdOf(event));
    }} catch {{
      // Status reporting must never break an opencode event handler.
    }}
  }},
  "tool.execute.before": async (input) => {{
    try {{
      await report(["tool.execute.before"], input && input.sessionID);
    }} catch {{
      // As above.
    }}
  }},
  }};
}};
"#
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::test_support::EnvGuard;

    fn opencode_events() -> Vec<crate::agents::ResolvedHookEvent> {
        let agent = crate::agents::get_agent("opencode").unwrap();
        crate::agents::resolved_status_integration_events(
            agent,
            &crate::session::config::Config::default(),
        )
        .unwrap()
    }

    #[test]
    #[serial_test::serial(shell_env)]
    fn test_opencode_plugin_path_honors_xdg_config_home() {
        let _guard = EnvGuard::unset(&["XDG_CONFIG_HOME"]);
        let home = Path::new("/home/tester");
        let cases: &[(&[&str], &str)] = &[
            // No override anywhere: opencode's own `~/.config` default.
            (&[], "/home/tester/.config/opencode/plugin/aoe-status.js"),
            // Session `environment` entry wins.
            (
                &["XDG_CONFIG_HOME=/xdg"],
                "/xdg/opencode/plugin/aoe-status.js",
            ),
            // Empty is unset, matching `resolve_config_dir_override`.
            (
                &["XDG_CONFIG_HOME="],
                "/home/tester/.config/opencode/plugin/aoe-status.js",
            ),
            // An unrelated entry does not disturb resolution.
            (
                &["FOO=bar"],
                "/home/tester/.config/opencode/plugin/aoe-status.js",
            ),
        ];
        for (env, expected) in cases {
            let env: Vec<String> = env.iter().map(|e| e.to_string()).collect();
            assert_eq!(
                opencode_plugin_path_in(home, &env),
                PathBuf::from(expected),
                "env {env:?}"
            );
        }

        // The declared home-relative subpath is exactly the no-override answer,
        // so a caller that ignores the resolver still lands in the right place.
        assert_eq!(
            opencode_plugin_path_in(home, &[]),
            home.join(OPENCODE_PLUGIN_SUBPATH)
        );
    }

    #[test]
    fn test_render_covers_every_declared_event() {
        let events = opencode_events();
        let rendered =
            render_opencode_plugin(HookInstallTarget::Host, &events).expect("render succeeds");
        assert!(rendered.contains(AOE_PLUGIN_MARKER));
        for event in &events {
            assert!(
                rendered.contains(&format!("\"{}\"", event.name)),
                "event {} missing from rendered plugin",
                event.name
            );
        }
        // Every embedded command must be the canonical writer, marker included,
        // so uninstall and the hook-rewrite migrations still recognise it.
        assert!(rendered.contains("# aoe-hooks"));
    }

    #[test]
    fn test_render_bakes_the_target_specific_status_base() {
        let events = opencode_events();
        let host = render_opencode_plugin(HookInstallTarget::Host, &events).unwrap();
        let sandbox = render_opencode_plugin(HookInstallTarget::Sandbox, &events).unwrap();
        assert!(host.contains(
            &super::super::dir_guard::hook_base_path()
                .display()
                .to_string()
        ));
        assert!(sandbox.contains(super::super::HOOK_STATUS_BASE_IN_CONTAINER));
        assert_ne!(host, sandbox);
    }

    #[test]
    fn test_render_applies_status_map_override() {
        let mut config = crate::session::config::Config::default();
        config
            .agents
            .entry("opencode".to_string())
            .or_default()
            .status_map
            .insert(
                "permission.asked".to_string(),
                crate::agents::HookStatus::Idle,
            );
        let agent = crate::agents::get_agent("opencode").unwrap();
        let events = crate::agents::resolved_status_integration_events(agent, &config).unwrap();
        let rendered = render_opencode_plugin(HookInstallTarget::Host, &events).unwrap();

        // The overridden event moved from the waiting map into the idle group,
        // which is what changes the plugin's runtime behavior.
        let idle_line = rendered
            .lines()
            .find(|l| l.starts_with("const IDLE = "))
            .expect("IDLE map rendered");
        assert!(idle_line.contains("permission.asked"), "{idle_line}");
        let other_line = rendered
            .lines()
            .find(|l| l.starts_with("const OTHER = "))
            .expect("OTHER map rendered");
        assert!(!other_line.contains("permission.asked"), "{other_line}");
    }

    /// The plugin is generated from a `format!` template, and opencode
    /// swallows a load failure: a syntax error would silently disable status
    /// reporting with nothing to show for it. Parse the real output rather
    /// than trusting review. Skips where `node` is unavailable, the way the
    /// tmux-dependent tests do.
    #[test]
    fn test_rendered_plugin_is_syntactically_valid_javascript() {
        let Ok(node) = which::which("node") else {
            eprintln!("skipping: node not on PATH");
            return;
        };
        let dir = tempfile::TempDir::new().unwrap();
        // `--check` parses as a module only for `.mjs`; the plugin uses
        // `export`, which opencode loads as ESM regardless of extension.
        let path = dir.path().join("aoe-status.mjs");
        std::fs::write(
            &path,
            render_opencode_plugin(HookInstallTarget::Host, &opencode_events()).unwrap(),
        )
        .unwrap();

        let out = std::process::Command::new(node)
            .arg("--check")
            .arg(&path)
            .output()
            .expect("run node --check");
        assert!(
            out.status.success(),
            "generated plugin does not parse:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Drive the generated plugin under `node` with `Bun.spawn` stubbed, and
    /// assert the sequence of statuses it would have written.
    ///
    /// The subagent rule is the subtle part: opencode runs the task tool as its
    /// own session that emits its own idle, so a finished subagent must not
    /// report the whole session Idle while the parent turn is still going. That
    /// is a behavior of the emitted JS, which no Rust-side assertion on the
    /// rendered text can reach.
    #[test]
    fn test_rendered_plugin_suppresses_subagent_idle() {
        let Ok(node) = which::which("node") else {
            eprintln!("skipping: node not on PATH");
            return;
        };
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("aoe-status.mjs"),
            render_opencode_plugin(HookInstallTarget::Host, &opencode_events()).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("driver.mjs"),
            r#"
const recorded = []
globalThis.Bun = {
  spawn(argv) {
    // Recover the status from the canonical writer's `printf <status>`.
    const m = /printf ([a-z]+) >/.exec(argv[2] ?? "")
    if (m) recorded.push(m[1])
    return { exited: Promise.resolve(0) }
  },
}
process.env.AOE_INSTANCE_ID = "drivertest"
const plugin = await (await import("./aoe-status.mjs")).AoeStatus({})
if (!plugin.event) throw new Error("no handlers registered with AOE_INSTANCE_ID set")
const status = (sessionID, type) =>
  plugin.event({ event: { type: "session.status", properties: { sessionID, status: { type } } } })

await status("A", "busy")                                // parent turn starts
await plugin["tool.execute.before"]({ sessionID: "B" })  // subagent starts working
await status("B", "idle")                                // subagent done, parent is not
await plugin.event({ event: { type: "permission.asked", properties: { sessionID: "A" } } })
await status("A", "idle")                                // parent done
console.log(JSON.stringify(recorded))
"#,
        )
        .unwrap();

        let out = std::process::Command::new(node)
            .arg("driver.mjs")
            .current_dir(dir.path())
            .output()
            .expect("run the plugin driver");
        assert!(
            out.status.success(),
            "driver failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            r#"["running","running","waiting","idle"]"#,
            "stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// With `AOE_INSTANCE_ID` unset the plugin must register no handlers at
    /// all, so a non-AoE opencode run on the same machine does not fork a
    /// shell per status transition. The writer command bails on the unset
    /// variable as well, but that guard lives inside the spawned process.
    #[test]
    fn test_rendered_plugin_registers_nothing_outside_an_aoe_session() {
        let Ok(node) = which::which("node") else {
            eprintln!("skipping: node not on PATH");
            return;
        };
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("aoe-status.mjs"),
            render_opencode_plugin(HookInstallTarget::Host, &opencode_events()).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("driver.mjs"),
            r#"
delete process.env.AOE_INSTANCE_ID
globalThis.Bun = {
  spawn() {
    throw new Error("spawned a shell outside an AoE session")
  },
}
const hooks = await (await import("./aoe-status.mjs")).AoeStatus({})
console.log(JSON.stringify(Object.keys(hooks)))
"#,
        )
        .unwrap();

        let out = std::process::Command::new(node)
            .arg("driver.mjs")
            .current_dir(dir.path())
            .env_remove("AOE_INSTANCE_ID")
            .output()
            .expect("run the plugin driver");
        assert!(
            out.status.success(),
            "driver failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "[]");
    }

    #[test]
    fn test_install_then_uninstall_roundtrip() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("plugin").join("aoe-status.js");
        let events = opencode_events();

        install_opencode_plugin_with_events(&path, HookInstallTarget::Host, &events).unwrap();
        assert!(path.exists());
        assert!(opencode_plugin_has_aoe_marker(&path));

        // Re-install is idempotent down to the bytes, so the file's mtime is
        // not churned on every session launch.
        let first = std::fs::read(&path).unwrap();
        install_opencode_plugin_with_events(&path, HookInstallTarget::Host, &events).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), first);

        assert!(uninstall_opencode_plugin(&path).unwrap());
        assert!(!path.exists());
        // Nothing left to remove the second time.
        assert!(!uninstall_opencode_plugin(&path).unwrap());
    }

    /// A file symlink at the plugin path: install writes through it (that is
    /// `atomic_write`'s single write behavior, see #2784 / #3186), and uninstall
    /// clears both ends. Leaving the target behind would strand generated
    /// content in whatever the link points at; leaving the link behind would
    /// give opencode a dangling plugin to fail on at every launch.
    #[cfg(unix)]
    #[test]
    fn test_install_and_uninstall_through_a_symlinked_plugin_path() {
        let dir = tempfile::TempDir::new().unwrap();
        let target = dir.path().join("dotfiles").join("aoe-status.js");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        let link = dir.path().join("plugin").join("aoe-status.js");
        std::fs::create_dir_all(link.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        install_opencode_plugin_with_events(&link, HookInstallTarget::Host, &opencode_events())
            .unwrap();
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink(),
            "install must write through the link, not replace it"
        );
        assert!(std::fs::read_to_string(&target)
            .unwrap()
            .contains(AOE_PLUGIN_MARKER));

        assert!(uninstall_opencode_plugin(&link).unwrap());
        assert!(!target.exists(), "generated content must not be stranded");
        assert!(
            std::fs::symlink_metadata(&link).is_err(),
            "a dangling link would make opencode log a plugin load failure on every launch"
        );
    }

    #[test]
    fn test_a_foreign_plugin_at_our_path_is_left_alone() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("aoe-status.js");
        let foreign = "export const Mine = async () => ({})\n";
        std::fs::write(&path, foreign).unwrap();

        let err =
            install_opencode_plugin_with_events(&path, HookInstallTarget::Host, &opencode_events())
                .expect_err("install must refuse a file it does not own");
        assert!(err.to_string().contains("refusing to overwrite"), "{err}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), foreign);

        assert!(!uninstall_opencode_plugin(&path).unwrap());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), foreign);
    }
}
