// Environment isolation for the live harness.
//
// `spawnAoeServe` starts from `process.env` so the daemon inherits PATH,
// locale, and proxy settings. Anything inherited that names a config, data,
// or credential location escapes the temporary HOME, because the daemon
// resolves agent state from its own environment (`resolve_agent_home` in
// `src/session/capture.rs`, the opencode readers, the XDG bases). A developer
// or CI shell exporting one of them points a live spec at real agent state.
//
// Listing every such name does not hold on its own: `src/` reads more than a
// dozen and gains one per agent integration, and a missed name leaks
// silently. So anything shaped like a path override is dropped, and the few
// the child genuinely needs are named instead. Dropping one of those breaks
// the run loudly, which is the safe direction to fail in.
//
// The shape rule alone is not enough either: `GIT_CONFIG_GLOBAL`,
// `AOE_ACP_NODE` and `AGENT_OF_EMPIRES_PROFILE` name host state under no
// suffix at all (#3657). Those are dropped by name, or pinned where dropping
// would only fall back to another host location. `isolatedEnv.test.ts` holds
// the resulting contract and fails on a variable `src/` reads that neither
// rule covers.

import { join } from "node:path";

/** Directories the harness owns, all inside the temporary test HOME. */
export interface IsolatedPaths {
  home: string;
  xdgConfig: string;
  xdgData: string;
  tmp: string;
  tmuxTmp: string;
}

/** Shape of a variable naming a path: `CODEX_HOME`, `OPENCODE_DB`, `PI_CONFIG_DIR`. */
const PATH_VAR = /^[A-Z][A-Z0-9_]*_(HOME|DIR|DB|PATH|CREDENTIALS)$/;

/**
 * Git's whole namespace is host state: config files, work trees, object
 * stores, and the helper commands git shells out to. Most of those names
 * carry no path suffix, so the family is dropped by prefix instead.
 */
const GIT_VAR = /^GIT_/;

/**
 * Host state the daemon reads under a name neither rule above can see. Each
 * entry points `aoe serve`, or an `aoe` call the harness makes with this
 * environment, at host configuration, a host executable, a host repository,
 * or the host session the test runner was launched from.
 */
export const HOST_STATE_VARS = new Set([
  // Both move the app dir, port, and tmux prefix off the ones `appDirFor`
  // and `tmuxSocketPath` compute for a spec.
  "AGENT_OF_EMPIRES_DEBUG",
  "AGENT_OF_EMPIRES_PROFILE",
  "AOE_ACP_NODE", // an arbitrary host Node executable for the ACP runner
  "AOE_CITYHALL_MODE", // serves the daemon as a client of a host CityHall
  // `discovery::discover()` prefers these over the local daemon, so every
  // `aoe` call the harness makes with this env, teardown's `acp stop --all`
  // included, would hit the developer's own daemon and kill its workers.
  "AOE_DAEMON_TOKEN",
  "AOE_DAEMON_URL",
  "AOE_GITHUB_CLONE_BASE", // redirects plugin clones at a host path or tree
  "AOE_SERVE_PASSPHRASE", // host credential for the daemon's own auth
  // Host endpoints for the daemon's outbound calls.
  "AOE_TELEMETRY_ENDPOINT",
  "AOE_UPDATE_API_BASE",
  "AOE_UPDATE_BASE_URL",
  // The session the test runner was launched from: `aoe` resolves "the
  // current session" from `TMUX_PANE`, and `AOE_INSTANCE_ID` names a host
  // session directly.
  "AOE_INSTANCE_ID",
  "TMUX",
  "TMUX_PANE",
]);

/**
 * Path variables the child keeps: toolchain and system locations, never agent
 * state. `XDG_RUNTIME_DIR` is the one XDG base not redirected, because it
 * names the host's session sockets rather than a data tree.
 */
export const INHERITED_PATH_VARS = new Set([
  "CARGO_HOME",
  "DYLD_FALLBACK_LIBRARY_PATH",
  "DYLD_LIBRARY_PATH",
  "GIT_EXEC_PATH",
  "LD_LIBRARY_PATH",
  "RUSTUP_HOME",
  "SSL_CERT_DIR",
  "XDG_RUNTIME_DIR",
]);

/**
 * Variables pinned rather than dropped, because dropping them only falls back
 * to another host location: git reads `/etc/gitconfig` for the system file,
 * and `$HOME/.gitconfig` for the global one. Pinning the global file inside
 * the test home keeps a daemon-side `git config --global` write in the tree
 * the harness deletes. `gitFixture.ts` pins the same pair for the fixture
 * subprocesses; this covers the daemon's own git calls.
 */
export function pinnedVars(paths: IsolatedPaths): Record<string, string> {
  return {
    GIT_CONFIG_GLOBAL: join(paths.home, ".gitconfig"),
    GIT_CONFIG_SYSTEM: "/dev/null",
  };
}

/**
 * Copy of `parentEnv` with every agent path pointed inside the test HOME.
 *
 * The bases the harness owns are redirected; the rest are dropped, which
 * leaves the daemon on its `$HOME`-relative fallback (`XDG_STATE_HOME` ->
 * `$HOME/.local/state`, and so on), already inside the test home.
 */
export function isolateEnv(parentEnv: NodeJS.ProcessEnv, paths: IsolatedPaths): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const [name, value] of Object.entries(parentEnv)) {
    if (INHERITED_PATH_VARS.has(name)) {
      env[name] = value;
      continue;
    }
    if (HOST_STATE_VARS.has(name)) continue;
    if (PATH_VAR.test(name) || GIT_VAR.test(name)) continue;
    env[name] = value;
  }
  return {
    ...env,
    HOME: paths.home,
    XDG_CONFIG_HOME: paths.xdgConfig,
    XDG_DATA_HOME: paths.xdgData,
    TMPDIR: paths.tmp,
    TMUX_TMPDIR: paths.tmuxTmp,
    ...pinnedVars(paths),
  };
}
