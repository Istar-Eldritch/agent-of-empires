//! Background async sub-agent transcript tailer.
//!
//! Claude's `Task` tool, when launched with `isAsync`, completes
//! immediately on the parent ACP stream and runs off-protocol. The
//! parent stream never reports the sub-agent's progress or completion;
//! that lives only in an on-disk JSONL transcript (the launch payload's
//! `outputFile`, a symlink into `~/.claude/projects/<proj>/subagents/`).
//!
//! For each launch the daemon spawns one [`spawn_tailer`] task that
//! follows that transcript and emits `BackgroundAgentProgress` /
//! `BackgroundAgentCompleted` events so the web "Background agents" panel
//! and the inline Task card can show live status, activity, and result.
//!
//! Design (see the design debate on this feature):
//!
//! - One task per agent, keyed by the launch. It self-terminates on
//!   completion, on a hard-idle cap, or when `event_tx` closes (the
//!   session went away), so it can never outlive its session.
//! - Completion is set on a terminal `end_turn` assistant message, or, at
//!   the idle timeout, inferred from a substantial final text block with
//!   no dangling tool call (Claude Code doesn't always tag the true final
//!   record `end_turn`; see `infer_idle_outcome`, #3232). A genuine hang
//!   (no final text, or a tool call never resolved) still reports
//!   `Stalled`, never faked as done.
//! - Progress is a throttled, coalesced snapshot (tool count + last
//!   action), not one event per transcript line, so the SQLite event log
//!   stays bounded while a mid-run reload still sees in-flight agents.
//! - Parsing is fully defensive: the transcript is an undocumented Claude
//!   SDK format. Malformed lines are counted and skipped; a format we
//!   cannot read at all degrades to a visible warning, never a panic.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::mpsc::Sender;

use crate::acp::state::{BackgroundAgentStatus, Event};

/// How often to poll the transcript for new bytes (no inotify).
const POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Minimum gap between two persisted `BackgroundAgentProgress` snapshots.
const PROGRESS_THROTTLE: Duration = Duration::from_millis(1500);
/// No transcript growth for this long flips the agent to `Stalled`.
const STALL_AFTER: Duration = Duration::from_secs(90);
/// No transcript growth for this long stops tracking entirely.
const ABORT_AFTER: Duration = Duration::from_secs(300);
/// Give the transcript file this long to appear after launch.
const WAIT_FILE_FOR: Duration = Duration::from_secs(30);
/// Bound on a single `docker`/`podman exec` used to read the transcript.
///
/// Both reads are awaited outside the tailer's `tokio::select!`, so a wedged
/// container runtime would block the loop before it could observe
/// `event_tx.closed()` or its own idle timeout, holding the task's
/// `ActiveGuard` (and therefore the off-protocol work grace) indefinitely.
/// Paired with `kill_on_drop`, so a timed-out exec is reaped rather than left
/// behind. Comfortably above a healthy exec while still well under
/// `POLL_INTERVAL * a few`, so a slow runtime degrades to fewer reads rather
/// than a stuck tailer.
const CONTAINER_EXEC_TIMEOUT: Duration = Duration::from_secs(10);
/// Cap on the assistant-text preview carried in progress/result.
const TEXT_PREVIEW_CHARS: usize = 240;

/// Where a sub-agent transcript lives, and how to read it.
///
/// A host session's transcript is an ordinary file the daemon reads
/// directly. A sandboxed session's transcript lives inside the container's
/// home directory (`~/.claude/.../subagents/`), which is NOT one of the
/// session's mounted volumes, so there is no host path to read and no mount
/// to translate through. It must be read across the container boundary via
/// the runtime's `exec`, exactly as `terminal_handler` runs sandboxed
/// commands. Before this existed the tailer always used host `tokio::fs`, so
/// a sandboxed sub-agent's transcript "never appeared" and every async Task
/// in a sandbox reported a spurious error. See the module docs and #(sandbox
/// sub-agent transcript).
#[derive(Clone)]
pub enum TranscriptSource {
    /// Read the transcript directly from the host filesystem.
    Host,
    /// Read the transcript from inside the session's container via
    /// `<runtime> exec <container> …` (`docker` / `podman`).
    Container {
        /// Container runtime binary, e.g. `docker`.
        runtime: &'static str,
        /// The session's container name for `<runtime> exec`.
        container: String,
    },
}

impl TranscriptSource {
    /// Whether the transcript file exists yet. The launch payload's
    /// `outputFile` is a symlink into the subagents dir; `test -e` and the
    /// host `metadata` call both follow it to the real target.
    async fn exists(&self, path: &str) -> bool {
        match self {
            TranscriptSource::Host => tokio::fs::metadata(path).await.is_ok(),
            TranscriptSource::Container { runtime, container } => {
                let mut cmd = tokio::process::Command::new(runtime);
                cmd.args(["exec", container, "test", "-e", path])
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .kill_on_drop(true);
                matches!(
                    tokio::time::timeout(CONTAINER_EXEC_TIMEOUT, cmd.status()).await,
                    Ok(Ok(s)) if s.success()
                )
            }
        }
    }

    /// Read the bytes appended since `offset` (0-based). Returns an empty
    /// vec when nothing is new or the file is momentarily unreadable, so the
    /// poll loop keeps waiting rather than aborting; mirrors the host
    /// open-failure path.
    async fn read_from(&self, path: &str, offset: u64) -> Vec<u8> {
        match self {
            TranscriptSource::Host => {
                let Ok(mut file) = tokio::fs::File::open(path).await else {
                    return Vec::new();
                };
                if file.seek(SeekFrom::Start(offset)).await.is_err() {
                    return Vec::new();
                }
                let mut chunk = Vec::new();
                if file.read_to_end(&mut chunk).await.is_err() {
                    return Vec::new();
                }
                chunk
            }
            TranscriptSource::Container { runtime, container } => {
                // `tail -c +N` prints bytes from the 1-based byte offset N to
                // EOF, so a 0-based `offset` maps to `+(offset + 1)`. On the
                // first read (offset 0) this is `+1`, i.e. the whole file.
                let start = format!("+{}", offset.saturating_add(1));
                let mut cmd = tokio::process::Command::new(runtime);
                cmd.args(["exec", container, "tail", "-c", &start, path])
                    .stdin(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .kill_on_drop(true);
                match tokio::time::timeout(CONTAINER_EXEC_TIMEOUT, cmd.output()).await {
                    Ok(Ok(out)) if out.status.success() => out.stdout,
                    _ => Vec::new(),
                }
            }
        }
    }
}

/// Removes an agent from the shared in-flight set on any tailer exit
/// (terminal event, hard-idle abort, or `event_tx` close). The
/// between-prompt idle watchdog treats a non-empty set as work in flight,
/// so this drop guard is what lets the session end once the sub-agent is
/// done, on every exit path. See #2573.
struct ActiveGuard {
    active: Arc<Mutex<HashSet<String>>>,
    agent_id: String,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        if let Ok(mut set) = self.active.lock() {
            set.remove(&self.agent_id);
        }
    }
}

/// Spawn the tailer for one async sub-agent. Returns immediately; the
/// task runs until the agent reaches a terminal state or `event_tx`
/// closes. `output_file` is the launch payload's transcript path.
/// `active` is the connection's in-flight background-agent set: the id is
/// inserted here and removed when the tailer task exits (see `ActiveGuard`).
/// Transcript on-disk format. Claude Code's async-Task transcripts and
/// pi-subagents' async run `events.jsonl` carry the same information in
/// different record shapes, so the tailer folds lines through the parser
/// matching the launch's declared format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TranscriptFormat {
    #[default]
    Claude,
    /// pi-subagents async run `events.jsonl` (pi session event stream).
    PiEvents,
}

impl TranscriptFormat {
    fn from_opt_str(value: Option<&str>) -> Self {
        match value {
            Some("pi-events") => Self::PiEvents,
            _ => Self::Claude,
        }
    }
}

pub fn spawn_tailer(
    agent_id: String,
    output_file: String,
    output_format: Option<String>,
    source: TranscriptSource,
    event_tx: Sender<Event>,
    active: Arc<Mutex<HashSet<String>>>,
) {
    active
        .lock()
        .expect("bg-agent active set mutex poisoned")
        .insert(agent_id.clone());
    if output_file.is_empty() {
        // No transcript path: we can never tail it. Mark it so the panel
        // doesn't show a forever-running agent.
        tokio::spawn(async move {
            let _guard = ActiveGuard {
                active,
                agent_id: agent_id.clone(),
            };
            let _ = event_tx
                .send(completed(
                    agent_id,
                    BackgroundAgentStatus::Error,
                    Vec::new(),
                    None,
                    Some("no transcript path reported for this sub-agent".into()),
                ))
                .await;
        });
        return;
    }
    tokio::spawn(async move {
        let _guard = ActiveGuard {
            active,
            agent_id: agent_id.clone(),
        };
        let format = TranscriptFormat::from_opt_str(output_format.as_deref());
        run_tailer(
            agent_id,
            tool_call_id,
            output_file,
            format,
            source,
            event_tx,
        )
        .await;
    });
}

/// One tool call parsed from the transcript, tracked by its tool_use id
/// so a later `tool_result` can fill in the outcome.
struct ToolEntry {
    id: String,
    name: String,
    title: Option<String>,
    ok: Option<bool>,
}

/// Hard cap on per-agent tool entries carried in events, so a runaway
/// sub-agent can't bloat the snapshot payload. Excess keeps the count
/// accurate (`tool_count`) but stops growing the detailed list.
const MAX_TOOLS: usize = 250;

/// Running accumulator for one agent's parsed transcript state.
#[derive(Default)]
struct Snapshot {
    tool_count: u32,
    /// Individual tool calls in order, with outcomes filled in from
    /// matching tool_result records. Capped at `MAX_TOOLS`.
    tools: Vec<ToolEntry>,
    last_tool: Option<String>,
    last_text: Option<String>,
    /// Final assistant text seen alongside an `end_turn` stop reason.
    result: Option<String>,
    /// Set once a terminal `end_turn` assistant message is parsed.
    done: bool,
    parse_errors: u32,
    parsed_any: bool,
    /// True when the most recently folded content block was `text`, false
    /// when it was `tool_use`. Claude Code's async-Task transcripts don't
    /// always tag the final assistant record `stop_reason: "end_turn"`, so
    /// this is the fallback signal an idle-timeout uses to tell "the
    /// sub-agent finished speaking" from "it's mid tool-call". See
    /// `infer_idle_outcome`.
    last_was_text: bool,
    /// Tool-use ids with no matching `tool_result` yet. Tracked separately
    /// from `tools`, which stops growing at `MAX_TOOLS` to bound the event
    /// payload; completion state must stay accurate past that cap, so it
    /// cannot be derived from the truncated list. Never sent on the wire.
    unresolved_tools: HashSet<String>,
    /// Declared transcript format; selects the line parser.
    format: TranscriptFormat,
    /// pi-format only: terminal status from `subagent.run.completed` /
    /// `subagent.workflow.completed` / status.json ("complete" vs "failed"
    /// vs "stopped"), so a failed run does not report Completed.
    pi_terminal_status: Option<String>,
    /// pi-format only: signature of the last status.json state folded in
    /// (state + per-step statuses). Lets the poll loop emit progress when
    /// the runner's canonical state changes even with no new transcript
    /// lines. Never sent on the wire.
    pi_sig: Option<String>,
}

async fn run_tailer(
    agent_id: String,
    tool_call_id: String,
    output_file: String,
    format: TranscriptFormat,
    source: TranscriptSource,
    event_tx: Sender<Event>,
) {
    // Wait for the transcript to appear (the SDK writes it shortly after
    // the launch event). Bail to Error if it never shows. For a sandboxed
    // session this checks inside the container, not the host.
    let mut waited = Duration::ZERO;
    while !source.exists(&output_file).await {
        if waited >= WAIT_FILE_FOR {
            let _ = event_tx
                .send(completed(
                    agent_id,
                    BackgroundAgentStatus::Error,
                    Vec::new(),
                    None,
                    Some("sub-agent transcript never appeared".into()),
                ))
                .await;
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(POLL_INTERVAL) => waited += POLL_INTERVAL,
            _ = event_tx.closed() => return, // session gone
        }
    }

    let mut offset: u64 = 0;
    let mut line_buf = String::new();
    let mut snap = Snapshot::default();
    snap.format = format;
    let mut last_progress = Utc::now() - chrono::Duration::seconds(10);
    let mut last_growth = Utc::now();
    let mut stalled_emitted = false;

    // pi-format only: per-child fan-out state. Workflow runs surface one
    // panel row per child step; single-child runs fold into the umbrella.
    let mut pi_children: HashMap<String, PiChildTrack> = HashMap::new();
    loop {
        let mut grew =
            read_new_lines(&source, &output_file, &mut offset, &mut line_buf, &mut snap).await;
        let now = Utc::now();
        if format == TranscriptFormat::PiEvents {
            if let Some(status) = read_pi_status(&source, &output_file).await {
                let own: Vec<&PiStep> = status
                    .steps
                    .iter()
                    .filter(|s| s.parent_workflow_run_id.as_deref() == Some(agent_id.as_str()))
                    .collect();
                if fold_umbrella_pi(&status, &own, &mut snap) {
                    grew = true;
                }
                if !own.is_empty() {
                    let now_ms = now.timestamp_millis();
                    for step in &own {
                        match emit_pi_child_event(
                            &mut pi_children,
                            step,
                            &tool_call_id,
                            now_ms,
                            &event_tx,
                        )
                        .await
                        {
                            Ok(true) => grew = true,
                            Ok(false) => {}
                            // Session event channel closed; stop tracking.
                            Err(()) => return,
                        }
                    }
                    if snap.done
                        && flush_pi_children(&mut pi_children, &event_tx)
                            .await
                            .is_err()
                    {
                        return; // session gone
                    }
                }
            }
        }
        if grew {
            last_growth = now;
            stalled_emitted = false;
        }

        if snap.done {
            // The explicit completion path. For pi-format transcripts the
            // runner's `subagent.run.completed` record carries the true
            // status ("complete" vs "failed"); Claude transcripts only end
            // via end_turn, which always means success here. The idle
            // timeout below can also infer completion; see
            // infer_idle_outcome.
            let status = match snap.pi_terminal_status.as_deref() {
                Some("failed") => BackgroundAgentStatus::Failed,
                // The run was stopped or its parent session went away
                // before finishing; not a success, not a failure.
                Some("stopped") => BackgroundAgentStatus::Detached,
                _ => BackgroundAgentStatus::Completed,
            };
            let warning = format_warning(&snap);
            let _ = event_tx
                .send(completed(
                    agent_id,
                    status,
                    snapshot_tools(&snap),
                    snap.result.clone(),
                    warning,
                ))
                .await;
            return;
        }

        let idle = (now - last_growth).to_std().unwrap_or(Duration::ZERO);
        if idle >= ABORT_AFTER {
            // Stopped tracking. No end_turn marker was ever seen, but the
            // transcript may still show the sub-agent actually finished;
            // see infer_idle_outcome.
            let (status, result, warning) = infer_idle_outcome(&snap);
            let _ = event_tx
                .send(completed(
                    agent_id,
                    status,
                    snapshot_tools(&snap),
                    result,
                    warning,
                ))
                .await;
            return;
        }

        let status = if idle >= STALL_AFTER {
            BackgroundAgentStatus::Stalled
        } else {
            BackgroundAgentStatus::Running
        };

        // Emit a throttled snapshot on real growth, or once when the
        // agent first transitions to Stalled so the panel reflects it.
        let throttle_ok = (now - last_progress)
            .to_std()
            .map(|d| d >= PROGRESS_THROTTLE)
            .unwrap_or(true);
        let stall_edge = status == BackgroundAgentStatus::Stalled && !stalled_emitted;
        if (grew && throttle_ok) || stall_edge {
            if event_tx
                .send(progress(agent_id.clone(), status, &snap))
                .await
                .is_err()
            {
                return; // session gone
            }
            last_progress = now;
            if stall_edge {
                stalled_emitted = true;
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
            _ = event_tx.closed() => return,
        }
    }
}

/// One child run parsed from the async runner's `status.json`.
struct PiStep {
    run_id: String,
    parent_workflow_run_id: Option<String>,
    agent: String,
    label: String,
    status: String,
    model: Option<String>,
    tool_count: Option<u32>,
    /// `tool args` of the most recent recorded tool call, for the
    /// activity line.
    last_tool: Option<String>,
    /// Most recent output line, for the activity line and result preview.
    last_text: Option<String>,
    /// The child's task prompt, as recorded by the runner.
    session_name: String,
    last_activity_ms: Option<i64>,
}

/// Parsed `status.json`: the async runner's canonical live state for a run.
struct PiStatus {
    /// "running" | "complete" | "failed" | "stopped".
    state: String,
    last_update_ms: Option<i64>,
    steps: Vec<PiStep>,
}

/// pi-format only: read the run's `status.json` (sibling of the
/// transcript's events.jsonl). The runner rewrites this small snapshot in
/// place as children start, make tool calls, and finish, so polling it
/// gives per-child liveness and detail that the sparse
/// `subagent.workflow.*` transcript records never carry. Returns None
/// when the file is missing or unreadable (the poll loop keeps waiting).
async fn read_pi_status(source: &TranscriptSource, output_file: &str) -> Option<PiStatus> {
    let dir = std::path::Path::new(output_file).parent()?;
    let status_path = dir.join("status.json").to_string_lossy().into_owned();
    // Whole-file read from offset 0; status.json is a snapshot, not a log.
    let bytes = source.read_from(&status_path, 0).await;
    if bytes.is_empty() {
        return None;
    }
    let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let state = v
        .get("state")
        .and_then(|s| s.as_str())
        .unwrap_or("running")
        .to_string();
    let last_update_ms = v.get("lastUpdate").and_then(|m| m.as_i64());
    let mut steps = Vec::new();
    if let Some(records) = v.get("steps").and_then(|s| s.as_array()) {
        for record in records {
            let agent = record
                .get("agent")
                .and_then(|a| a.as_str())
                .unwrap_or("agent")
                .to_string();
            let label = record
                .get("label")
                .and_then(|l| l.as_str())
                .filter(|l| !l.is_empty())
                .unwrap_or(&agent)
                .to_string();
            // recentTools entries are { tool, args, endMs }, most recent
            // last; recentOutput is a list of output lines.
            let last_tool = record
                .get("recentTools")
                .and_then(|t| t.as_array())
                .and_then(|t| t.last())
                .map(|t| {
                    let tool = t.get("tool").and_then(|x| x.as_str()).unwrap_or("tool");
                    let args = t.get("args").and_then(|x| x.as_str()).unwrap_or("");
                    preview(&format!("{tool} {args}"))
                })
                .filter(|t| !t.is_empty());
            let last_text = record
                .get("recentOutput")
                .and_then(|o| o.as_array())
                .and_then(|o| o.last())
                .and_then(|o| o.as_str())
                .filter(|o| !o.is_empty())
                .map(preview);
            steps.push(PiStep {
                run_id: record
                    .get("runId")
                    .and_then(|r| r.as_str())
                    .unwrap_or_default()
                    .to_string(),
                parent_workflow_run_id: record
                    .get("parentWorkflowRunId")
                    .and_then(|r| r.as_str())
                    .map(str::to_string),
                agent,
                label,
                status: record
                    .get("status")
                    .and_then(|s| s.as_str())
                    .unwrap_or("running")
                    .to_string(),
                model: record
                    .get("model")
                    .and_then(|m| m.as_str())
                    .map(str::to_string),
                tool_count: record
                    .get("toolCount")
                    .and_then(|c| c.as_u64())
                    .map(|c| c as u32),
                last_tool,
                last_text,
                session_name: record
                    .get("sessionName")
                    .and_then(|s| s.as_str())
                    .unwrap_or_default()
                    .to_string(),
                last_activity_ms: record.get("lastActivityAt").and_then(|m| m.as_i64()),
            });
        }
    }
    Some(PiStatus {
        state,
        last_update_ms,
        steps,
    })
}

/// pi-format only: fold a parsed status.json into the umbrella row's
/// snapshot. Runs whose steps belong to this run's workflow fan out to
/// their own panel rows (the umbrella keeps just the child count);
/// single-child runs fold the one step's live detail (real tool count,
/// recent tools) into the umbrella row itself. Returns true when the
/// folded state changed materially.
fn fold_umbrella_pi(status: &PiStatus, own: &[&PiStep], snap: &mut Snapshot) -> bool {
    let mut sig = String::with_capacity(32);
    sig.push_str(&status.state);
    if own.is_empty() {
        // Single-child run (or workflow before children spawn): the first
        // step's live detail belongs to this row. recentTools only holds
        // the runner's recent-window snapshot, so entries carry no ok
        // state; the total count comes from the authoritative toolCount.
        if let Some(step) = status.steps.first() {
            sig.push_str(&format!("|{}|{:?}", step.status, step.tool_count));
            if let Some(count) = step.tool_count {
                snap.tool_count = count;
            }
            if let Some(tool) = &step.last_tool {
                snap.last_tool = Some(tool.clone());
                snap.last_was_text = false;
            }
            if let Some(text) = &step.last_text {
                snap.last_text = Some(text.clone());
                snap.last_was_text = true;
            }
        }
    } else {
        sig.push_str(&format!(
            "|{}|{}",
            own.len(),
            own.first().map_or("", |s| s.status.as_str())
        ));
        snap.tool_count = own.len() as u32;
        snap.tools.clear();
    }
    match status.state.as_str() {
        "running" => {}
        // Terminal states from the runner's own bookkeeping; the tailer's
        // transcript-based completion may fire first, but this catches
        // workflow runs whose events.jsonl ends with sparse records.
        "complete" | "failed" | "stopped" => {
            snap.done = true;
            snap.pi_terminal_status = Some(status.state.clone());
        }
        _ => {}
    }
    let changed = snap.pi_sig.as_deref() != Some(sig.as_str());
    if changed {
        snap.pi_sig = Some(sig);
    }
    changed
}

/// Per-child fan-out state for one tailer. Keys are child run ids from
/// status.json steps; the umbrella's `BackgroundAgentLaunched` rows stay
/// as the workflow aggregate, and each child gets its own row carrying
/// its own model, tool count, and terminal status.
struct PiChildTrack {
    sig: String,
    status: String,
    done: bool,
}

/// Map a status.json terminal step status to a panel status.
fn pi_step_status(status: &str) -> BackgroundAgentStatus {
    match status {
        "failed" => BackgroundAgentStatus::Failed,
        // The child was stopped or its session ended before finishing.
        "stopped" => BackgroundAgentStatus::Detached,
        _ => BackgroundAgentStatus::Completed,
    }
}

fn pi_child_tool(step: &PiStep) -> crate::acp::state::BackgroundAgentTool {
    crate::acp::state::BackgroundAgentTool {
        name: step.label.clone(),
        title: step.last_tool.clone(),
        ok: match step.status.as_str() {
            "complete" => Some(true),
            "failed" => Some(false),
            _ => None,
        },
    }
}

fn pi_child_active(step: &PiStep, now_ms: i64) -> BackgroundAgentStatus {
    match step.last_activity_ms {
        Some(ms) if now_ms.saturating_sub(ms) >= STALL_AFTER.as_millis() as i64 => {
            BackgroundAgentStatus::Stalled
        }
        _ => BackgroundAgentStatus::Running,
    }
}

/// Emit the launch/progress/completion events for one workflow child step.
/// Returns true when the step state changed materially (counts as poll
/// activity), Err when the session event channel closed.
async fn emit_pi_child_event(
    children: &mut HashMap<String, PiChildTrack>,
    step: &PiStep,
    tool_call_id: &str,
    now_ms: i64,
    event_tx: &Sender<Event>,
) -> Result<bool, ()> {
    let sig = format!("{}|{:?}", step.status, step.tool_count);
    if !children.contains_key(&step.run_id) {
        children.insert(
            step.run_id.clone(),
            PiChildTrack {
                sig: sig.clone(),
                status: step.status.clone(),
                done: step.status != "running",
            },
        );
        let description = format!("{} ({})", step.label, step.agent);
        event_tx
            .send(Event::BackgroundAgentLaunched {
                agent_id: step.run_id.clone(),
                tool_call_id: tool_call_id.to_string(),
                description,
                prompt: step.session_name.chars().take(2000).collect(),
                model: step.model.clone().unwrap_or_default(),
                output_file: String::new(),
                output_format: None,
                started_at: Utc::now(),
            })
            .await
            .map_err(|_| ())?;
        let _ = event_tx
            .send(Event::BackgroundAgentProgress {
                agent_id: step.run_id.clone(),
                status: pi_child_active(step, now_ms),
                tool_count: step.tool_count.unwrap_or(0),
                tools: vec![pi_child_tool(step)],
                last_tool: step.last_tool.clone(),
                last_text: step.last_text.clone(),
                at: Utc::now(),
            })
            .await
            .map_err(|_| ())?;
        return Ok(true);
    }
    let track = children.get_mut(&step.run_id).expect("just checked");
    if track.done {
        return Ok(false);
    }
    if step.status != "running" {
        track.done = true;
        track.status = step.status.clone();
        let _ = event_tx
            .send(completed(
                step.run_id.clone(),
                pi_step_status(&step.status),
                vec![pi_child_tool(step)],
                step.last_text.clone(),
                None,
            ))
            .await
            .map_err(|_| ())?;
        return Ok(true);
    }
    if sig != track.sig {
        track.sig = sig;
        let _ = event_tx
            .send(Event::BackgroundAgentProgress {
                agent_id: step.run_id.clone(),
                status: pi_child_active(step, now_ms),
                tool_count: step.tool_count.unwrap_or(0),
                tools: vec![pi_child_tool(step)],
                last_tool: step.last_tool.clone(),
                last_text: step.last_text.clone(),
                at: Utc::now(),
            })
            .await
            .map_err(|_| ())?;
        return Ok(true);
    }
    Ok(false)
}

/// Flush children that never reported a terminal status when the workflow
/// itself finished (the runner may race the last step's record).
async fn flush_pi_children(
    children: &mut HashMap<String, PiChildTrack>,
    event_tx: &Sender<Event>,
) -> Result<(), ()> {
    let pending: Vec<(String, String)> = children
        .iter()
        .filter(|(_, t)| !t.done)
        .map(|(id, t)| (id.clone(), t.status.clone()))
        .collect();
    for (id, status) in pending {
        if let Some(track) = children.get_mut(&id) {
            track.done = true;
        }
        let _ = event_tx
            .send(completed(
                id,
                pi_step_status(&status),
                Vec::new(),
                None,
                None,
            ))
            .await
            .map_err(|_| ())?;
    }
    Ok(())
}

/// Read any bytes appended since `offset`, splitting on newlines and
/// folding complete JSONL records into `snap`. Returns true if the file
/// grew. A partial trailing line stays in `line_buf` for the next poll.
async fn read_new_lines(
    source: &TranscriptSource,
    path: &str,
    offset: &mut u64,
    line_buf: &mut String,
    snap: &mut Snapshot,
) -> bool {
    let chunk = source.read_from(path, *offset).await;
    if chunk.is_empty() {
        return false;
    }
    *offset += chunk.len() as u64;
    // Transcript is UTF-8 JSONL; lossy is fine for our previews and never
    // splits a record (we only act on whole, newline-terminated lines).
    line_buf.push_str(&String::from_utf8_lossy(&chunk));
    while let Some(nl) = line_buf.find('\n') {
        let line: String = line_buf.drain(..=nl).collect();
        let line = line.trim();
        if !line.is_empty() {
            fold_line(line, snap);
        }
    }
    true
}

/// Parse one JSONL transcript line and fold it into the snapshot. Fully
/// defensive: any shape we don't recognize is ignored, not fatal.
/// Assistant lines carry `tool_use` (a tool call) and `text`; user lines
/// carry `tool_result` (the outcome), matched back by `tool_use_id`.
fn fold_line(line: &str, snap: &mut Snapshot) {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
        snap.parse_errors += 1;
        return;
    };
    if snap.format == TranscriptFormat::PiEvents {
        fold_pi_line(&v, snap);
        return;
    }
    let kind = v.get("type").and_then(|t| t.as_str());
    let Some(msg) = v.get("message") else {
        return;
    };
    let Some(blocks) = msg.get("content").and_then(|c| c.as_array()) else {
        return;
    };
    match kind {
        Some("assistant") => {
            snap.parsed_any = true;
            let end_turn = msg.get("stop_reason").and_then(|s| s.as_str()) == Some("end_turn");
            for block in blocks {
                match block.get("type").and_then(|t| t.as_str()) {
                    Some("tool_use") => fold_tool_use(block, snap),
                    Some("text") => {
                        if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                            let preview = preview(text);
                            if !preview.is_empty() {
                                snap.last_text = Some(preview.clone());
                                snap.last_was_text = true;
                                if end_turn {
                                    snap.result = Some(preview);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            if end_turn {
                snap.done = true;
            }
        }
        Some("user") => {
            for block in blocks {
                if block.get("type").and_then(|t| t.as_str()) == Some("tool_result") {
                    fold_tool_result(block, snap);
                }
            }
        }
        // attachment / system bookkeeping lines: ignore.
        _ => {}
    }
}

/// Fold one pi-subagents async-run `events.jsonl` record into the snapshot.
/// Same information as the Claude parser, different record shapes:
/// `tool_execution_start/end` carry the tool call lifecycle (with
/// `toolCallId`/`toolName` at the top level), `message_end` carries full
/// messages, and `subagent.run.completed` is the definitive terminal marker.
fn fold_pi_line(v: &serde_json::Value, snap: &mut Snapshot) {
    let kind = v.get("type").and_then(|t| t.as_str());
    let Some(kind) = kind else { return };
    match kind {
        "tool_execution_start" => {
            snap.parsed_any = true;
            snap.tool_count += 1;
            let name = v
                .get("toolName")
                .and_then(|n| n.as_str())
                .unwrap_or("tool")
                .to_string();
            snap.last_tool = Some(name.clone());
            snap.last_was_text = false;
            let id = v
                .get("toolCallId")
                .and_then(|i| i.as_str())
                .unwrap_or_default()
                .to_string();
            snap.unresolved_tools.insert(id.clone());
            if snap.tools.len() >= MAX_TOOLS {
                return;
            }
            let title = v
                .get("args")
                .and_then(|a| a.as_object())
                .map(|obj| {
                    for key in [
                        "command",
                        "file_path",
                        "path",
                        "pattern",
                        "url",
                        "query",
                        "description",
                    ] {
                        if let Some(val) = obj.get(key).and_then(|v| v.as_str()) {
                            if !val.is_empty() {
                                return Some(preview(val));
                            }
                        }
                    }
                    None
                })
                .unwrap_or(None);
            snap.tools.push(ToolEntry {
                id,
                name,
                title,
                ok: None,
            });
        }
        "tool_execution_end" => {
            let id = v
                .get("toolCallId")
                .and_then(|i| i.as_str())
                .unwrap_or_default()
                .to_string();
            let is_error = v.get("isError").and_then(|e| e.as_bool()).unwrap_or(false);
            snap.unresolved_tools.remove(&id);
            if let Some(entry) = snap.tools.iter_mut().find(|t| t.id == id) {
                entry.ok = Some(!is_error);
            }
        }
        "message_end" => {
            let Some(message) = v.get("message") else {
                return;
            };
            let role = message.get("role").and_then(|r| r.as_str()).unwrap_or("");
            if role != "assistant" {
                return;
            }
            let end_turn = message.get("stopReason").and_then(|s| s.as_str()) == Some("end_turn");
            let mut saw_text = false;
            if let Some(blocks) = message.get("content").and_then(|c| c.as_array()) {
                for block in blocks {
                    match block.get("type").and_then(|t| t.as_str()) {
                        Some("text") => {
                            if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                                let text = preview(text);
                                if !text.is_empty() {
                                    saw_text = true;
                                    snap.last_text = Some(text.clone());
                                    if end_turn {
                                        snap.result = Some(text);
                                    }
                                }
                            }
                        }
                        Some("toolCall") | Some("tool_use") => {
                            snap.last_was_text = false;
                        }
                        _ => {}
                    }
                }
            }
            if saw_text {
                snap.last_was_text = true;
            }
            if end_turn {
                snap.done = true;
            }
        }
        // Definitive lifecycle markers written by the async runner.
        // Workflows end with `subagent.workflow.completed` (state:
        // complete/failed/stopped); plain runs with `subagent.run.completed`
        // (status: complete/failed).
        "subagent.workflow.completed" => {
            snap.parsed_any = true;
            snap.done = true;
            if let Some(state) = v.get("state").and_then(|s| s.as_str()) {
                snap.pi_terminal_status = Some(state.to_string());
            }
        }
        "subagent.run.completed" => {
            snap.parsed_any = true;
            snap.done = true;
            if let Some(status) = v.get("status").and_then(|s| s.as_str()) {
                snap.pi_terminal_status = Some(status.to_string());
            }
        }
        _ => {}
    }
}

/// Record a tool call. Bumps the count and the unresolved set always;
/// appends a detailed entry only until the cap, so a huge sub-agent can't
/// bloat the event payload.
fn fold_tool_use(block: &serde_json::Value, snap: &mut Snapshot) {
    snap.tool_count += 1;
    snap.last_was_text = false;
    let name = block
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("tool")
        .to_string();
    snap.last_tool = Some(name.clone());
    let id = block
        .get("id")
        .and_then(|i| i.as_str())
        .unwrap_or_default()
        .to_string();
    // Tracked past the cap: `tools` is truncated to bound the event
    // payload, but completion state must stay accurate for every call.
    snap.unresolved_tools.insert(id.clone());
    if snap.tools.len() >= MAX_TOOLS {
        return;
    }
    let title = block.get("input").and_then(tool_title);
    snap.tools.push(ToolEntry {
        id,
        name,
        title,
        ok: None,
    });
}

/// Fill in a tool's outcome from its `tool_result`, matched by id, and
/// clear it from the unresolved set (which, unlike `tools`, is uncapped).
fn fold_tool_result(block: &serde_json::Value, snap: &mut Snapshot) {
    let Some(id) = block.get("tool_use_id").and_then(|i| i.as_str()) else {
        return;
    };
    let is_error = block
        .get("is_error")
        .and_then(|e| e.as_bool())
        .unwrap_or(false);
    snap.unresolved_tools.remove(id);
    if let Some(entry) = snap.tools.iter_mut().find(|t| t.id == id) {
        entry.ok = Some(!is_error);
    }
}

/// Pick a short label from a tool's input: the command, file path,
/// pattern, url, or description, whichever is present first.
fn tool_title(input: &serde_json::Value) -> Option<String> {
    for key in [
        "command",
        "file_path",
        "path",
        "pattern",
        "url",
        "query",
        "description",
    ] {
        if let Some(s) = input.get(key).and_then(|v| v.as_str()) {
            if !s.is_empty() {
                return Some(preview(s));
            }
        }
    }
    None
}

/// Convert the tracked tool entries into the wire shape (drops the
/// internal id used only for result matching).
fn snapshot_tools(snap: &Snapshot) -> Vec<crate::acp::state::BackgroundAgentTool> {
    snap.tools
        .iter()
        .map(|t| crate::acp::state::BackgroundAgentTool {
            name: t.name.clone(),
            title: t.title.clone(),
            ok: t.ok,
        })
        .collect()
}

/// First `TEXT_PREVIEW_CHARS` characters of `text`, trimmed, with an
/// ellipsis if truncated.
fn preview(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= TEXT_PREVIEW_CHARS {
        return trimmed.to_string();
    }
    let head: String = trimmed.chars().take(TEXT_PREVIEW_CHARS).collect();
    format!("{}…", head.trim_end())
}

/// A non-fatal note when the transcript was readable but we never parsed
/// a usable assistant record (likely an SDK format change).
fn format_warning(snap: &Snapshot) -> Option<String> {
    if !snap.parsed_any && snap.parse_errors > 0 {
        Some("sub-agent transcript format not recognized; details unavailable".into())
    } else {
        None
    }
}

/// Decide what an idle-timeout (`ABORT_AFTER`, no `end_turn` ever seen)
/// really means: a genuine hang, or a sub-agent that finished speaking and
/// simply stopped writing. Claude Code's async-Task transcripts don't
/// always tag the final assistant record `stop_reason: "end_turn"` (see
/// #3232), so a substantial final text block with no dangling tool call is
/// treated as done, not stalled. A tool call still awaiting its result
/// (`ok: None`) means the sub-agent was mid-action, never done.
fn infer_idle_outcome(snap: &Snapshot) -> (BackgroundAgentStatus, Option<String>, Option<String>) {
    let dangling_tool = !snap.unresolved_tools.is_empty();
    if snap.last_was_text && snap.last_text.is_some() && !dangling_tool {
        (
            BackgroundAgentStatus::Completed,
            snap.last_text.clone(),
            Some("no explicit end_turn marker; completion inferred from final text".into()),
        )
    } else {
        (
            BackgroundAgentStatus::Stalled,
            snap.result.clone(),
            Some("no transcript activity; stopped tracking".into()),
        )
    }
}

fn progress(agent_id: String, status: BackgroundAgentStatus, snap: &Snapshot) -> Event {
    Event::BackgroundAgentProgress {
        agent_id,
        status,
        tool_count: snap.tool_count,
        tools: snapshot_tools(snap),
        last_tool: snap.last_tool.clone(),
        last_text: snap.last_text.clone(),
        at: Utc::now(),
    }
}

fn completed(
    agent_id: String,
    status: BackgroundAgentStatus,
    tools: Vec<crate::acp::state::BackgroundAgentTool>,
    result: Option<String>,
    warning: Option<String>,
) -> Event {
    Event::BackgroundAgentCompleted {
        agent_id,
        status,
        tools,
        result,
        warning,
        ended_at: Utc::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The host read path returns only bytes appended since `offset`, and
    /// `read_new_lines` folds each complete line while a partial trailing
    /// line waits for the next poll. This is the same offset contract the
    /// container `tail -c +N` path mirrors, so pinning it here guards both.
    #[tokio::test]
    async fn host_read_new_lines_reads_from_offset_and_buffers_partials() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("transcript.jsonl");
        let path_str = path.to_string_lossy().to_string();
        let source = TranscriptSource::Host;

        let line =
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash"}]}}"#;
        // First write ends mid-line (no trailing newline): nothing folds yet.
        tokio::fs::write(&path, line).await.unwrap();
        let mut offset = 0u64;
        let mut buf = String::new();
        let mut snap = Snapshot::default();
        assert!(read_new_lines(&source, &path_str, &mut offset, &mut buf, &mut snap).await);
        assert_eq!(snap.tool_count, 0, "a partial line must not fold");
        assert_eq!(offset, line.len() as u64);

        // Append the newline plus a second full line; the read starts at the
        // saved offset, so the first line completes and the second folds too.
        tokio::fs::write(&path, format!("{line}\n{line}\n"))
            .await
            .unwrap();
        assert!(read_new_lines(&source, &path_str, &mut offset, &mut buf, &mut snap).await);
        assert_eq!(snap.tool_count, 2);

        // No growth → no new bytes → false, offset unchanged.
        let before = offset;
        assert!(!read_new_lines(&source, &path_str, &mut offset, &mut buf, &mut snap).await);
        assert_eq!(offset, before);
    }

    #[test]
    fn fold_counts_tools_and_tracks_last_text() {
        let mut snap = Snapshot::default();
        fold_line(
            r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","name":"Bash"}]}}"#,
            &mut snap,
        );
        fold_line(
            r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"working on it"}]}}"#,
            &mut snap,
        );
        assert_eq!(snap.tool_count, 1);
        assert_eq!(snap.last_tool.as_deref(), Some("Bash"));
        assert_eq!(snap.last_text.as_deref(), Some("working on it"));
        assert!(!snap.done);
    }

    /// pi-subagents async runs announce their launch with the claudeCode
    /// envelope and `outputFormat: "pi-events"`; the tailer must fold the
    /// run's `events.jsonl` records (different shapes from Claude
    /// transcripts) into the same snapshot state.
    #[test]
    fn fold_pi_events_tracks_tools_and_terminal_status() {
        let mut snap = Snapshot::default();
        snap.format = TranscriptFormat::PiEvents;
        fold_line(
            r#"{"type":"tool_execution_start","toolCallId":"t1","toolName":"read","args":{"path":"AGENTS.md"},"observedAt":1000}"#,
            &mut snap,
        );
        fold_line(
            r#"{"type":"tool_execution_start","toolCallId":"t2","toolName":"bash","args":{"command":"ls -la"},"observedAt":1001}"#,
            &mut snap,
        );
        fold_line(
            r#"{"type":"tool_execution_end","toolCallId":"t1","toolName":"read","isError":false,"observedAt":1002}"#,
            &mut snap,
        );
        fold_line(
            r#"{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"working on it"}],"stopReason":"toolUse"}}"#,
            &mut snap,
        );
        assert_eq!(snap.tool_count, 2);
        assert_eq!(snap.last_tool.as_deref(), Some("bash"));
        assert_eq!(snap.last_text.as_deref(), Some("working on it"));
        assert!(!snap.done);
        let tools = snapshot_tools(&snap);
        assert_eq!(tools[0].name, "read");
        assert_eq!(tools[0].title.as_deref(), Some("AGENTS.md"));
        assert_eq!(tools[0].ok, Some(true));
        assert_eq!(tools[1].title.as_deref(), Some("ls -la"));
        assert_eq!(tools[1].ok, None);

        // End_turn final text completes with the result.
        fold_line(
            r#"{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"final report"}],"stopReason":"end_turn"}}"#,
            &mut snap,
        );
        assert!(snap.done);
        assert_eq!(snap.result.as_deref(), Some("final report"));
    }

    /// `subagent.run.completed` is the definitive terminal marker written
    /// by the async runner; its `status` distinguishes success from
    /// failure so a failed run does not report Completed.
    /// The async runner's `status.json` is the canonical live state.
    /// Single-child runs fold the one step's live detail (real tool count,
    /// recent tool) into the umbrella row; workflow children are excluded
    /// from the umbrella because they fan out to their own rows. A fresh
    /// read with unchanged content must not signal change.
    #[tokio::test]
    async fn read_pi_status_and_umbrella_fold() {
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path().join("run-1");
        tokio::fs::create_dir_all(&run_dir).await.unwrap();
        let transcript = run_dir.join("events.jsonl");
        tokio::fs::write(&transcript, "{}\n").await.unwrap();
        let source = TranscriptSource::Host;
        let output_file = transcript.to_string_lossy().to_string();

        let status = r#"{"runId":"run-1","mode":"single","state":"running","lastUpdate":1000,
            "steps":[{"agent":"worker","status":"running","runId":"","toolCount":41,
                      "recentTools":[{"tool":"read","args":"src/parser.rs"}],
                      "recentOutput":["mapping the language surface"]}]}"#;
        tokio::fs::write(run_dir.join("status.json"), status)
            .await
            .unwrap();

        let parsed = read_pi_status(&source, &output_file).await.unwrap();
        assert_eq!(parsed.state, "running");
        let mut snap = Snapshot::default();
        snap.format = TranscriptFormat::PiEvents;
        let own: Vec<&PiStep> = parsed
            .steps
            .iter()
            .filter(|s| s.parent_workflow_run_id.as_deref() == Some("run-1"))
            .collect();
        assert!(own.is_empty(), "single-mode steps must not fan out");
        assert!(fold_umbrella_pi(&parsed, &own, &mut snap));
        assert!(!snap.done);
        assert_eq!(snap.tool_count, 41);
        assert_eq!(snap.last_tool.as_deref(), Some("read src/parser.rs"));
        assert_eq!(
            snap.last_text.as_deref(),
            Some("mapping the language surface")
        );

        // Unchanged state → no change signal (no progress spam).
        let parsed = read_pi_status(&source, &output_file).await.unwrap();
        let own: Vec<&PiStep> = parsed
            .steps
            .iter()
            .filter(|s| s.parent_workflow_run_id.as_deref() == Some("run-1"))
            .collect();
        assert!(!fold_umbrella_pi(&parsed, &own, &mut snap));

        // Workflow children are excluded from the umbrella fold; the
        // umbrella counts them but keeps no tool entries.
        let status = r#"{"runId":"run-2","mode":"workflow","state":"running","lastUpdate":2000,
            "steps":[
                {"agent":"delegate","label":"syntax","status":"complete","runId":"s1","parentWorkflowRunId":"run-2","model":"m1"},
                {"agent":"delegate","label":"semantics","status":"running","runId":"s2","parentWorkflowRunId":"run-2","model":"m2"}
            ]}"#;
        tokio::fs::write(run_dir.join("status.json"), status)
            .await
            .unwrap();
        let parsed = read_pi_status(&source, &output_file).await.unwrap();
        assert_eq!(parsed.steps.len(), 2);
        let own: Vec<&PiStep> = parsed
            .steps
            .iter()
            .filter(|s| s.parent_workflow_run_id.as_deref() == Some("run-1"))
            .collect();
        assert!(own.is_empty(), "another workflow's children must not match");
        let own: Vec<&PiStep> = parsed
            .steps
            .iter()
            .filter(|s| s.parent_workflow_run_id.as_deref() == Some("run-2"))
            .collect();
        assert_eq!(own.len(), 2);
        let mut snap = Snapshot::default();
        snap.format = TranscriptFormat::PiEvents;
        assert!(fold_umbrella_pi(&parsed, &own, &mut snap));
        assert_eq!(snap.tool_count, 2);
        assert!(snapshot_tools(&snap).is_empty(), "children own their rows");

        // Terminal workflow state flips the snapshot to done, mappable.
        let status =
            r#"{"runId":"run-2","mode":"workflow","state":"failed","lastUpdate":3000,"steps":[]}"#;
        tokio::fs::write(run_dir.join("status.json"), status)
            .await
            .unwrap();
        let parsed = read_pi_status(&source, &output_file).await.unwrap();
        let own: Vec<&PiStep> = parsed
            .steps
            .iter()
            .filter(|s| s.parent_workflow_run_id.as_deref() == Some("run-2"))
            .collect();
        assert!(fold_umbrella_pi(&parsed, &own, &mut snap));
        assert!(snap.done);
        assert_eq!(snap.pi_terminal_status.as_deref(), Some("failed"));
    }

    /// Workflow children fan out to their own panel rows: a Launched with
    /// the child's own run id, model, and prompt, then Progress ticks, and
    /// a terminal step status maps to a terminal panel status.
    #[tokio::test]
    async fn pi_children_fan_out_to_own_rows() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let mut children: HashMap<String, PiChildTrack> = HashMap::new();
        let step = PiStep {
            run_id: "s1".into(),
            parent_workflow_run_id: Some("run-2".into()),
            agent: "delegate".into(),
            label: "syntax".into(),
            status: "running".into(),
            model: Some("syn-large-text".into()),
            tool_count: Some(7),
            last_tool: Some("read src/parser.rs".into()),
            last_text: Some("reading parser".into()),
            session_name: "Map the syntax".into(),
            last_activity_ms: Some(1000),
        };
        let now_ms = 1500;

        // First sighting: Launched + initial Progress.
        assert!(
            emit_pi_child_event(&mut children, &step, "tc1", now_ms, &tx)
                .await
                .unwrap()
        );
        let Event::BackgroundAgentLaunched {
            agent_id,
            tool_call_id,
            description,
            model,
            prompt,
            ..
        } = rx.recv().await.unwrap()
        else {
            panic!("expected Launched");
        };
        assert_eq!(agent_id, "s1");
        assert_eq!(tool_call_id, "tc1");
        assert_eq!(description, "syntax (delegate)");
        assert_eq!(model, "syn-large-text");
        assert_eq!(prompt, "Map the syntax");
        let Event::BackgroundAgentProgress {
            tool_count, tools, ..
        } = rx.recv().await.unwrap()
        else {
            panic!("expected Progress");
        };
        assert_eq!(tool_count, 7);
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "syntax");
        assert_eq!(tools[0].ok, None);

        // Unchanged → no events.
        assert!(
            !emit_pi_child_event(&mut children, &step, "tc1", now_ms, &tx)
                .await
                .unwrap()
        );

        // Tool count tick → Progress with the child's own entry.
        let mut step2 = step.clone();
        step2.tool_count = Some(9);
        assert!(
            emit_pi_child_event(&mut children, &step2, "tc1", now_ms, &tx)
                .await
                .unwrap()
        );
        let Event::BackgroundAgentProgress { tool_count, .. } = rx.recv().await.unwrap() else {
            panic!("expected Progress");
        };
        assert_eq!(tool_count, 9);

        // Terminal failure → Completed with Failed.
        step2.status = "failed".into();
        assert!(
            emit_pi_child_event(&mut children, &step2, "tc1", now_ms, &tx)
                .await
                .unwrap()
        );
        let Event::BackgroundAgentCompleted { status, .. } = rx.recv().await.unwrap() else {
            panic!("expected Completed");
        };
        assert_eq!(status, BackgroundAgentStatus::Failed);

        // Done children stay quiet.
        assert!(
            !emit_pi_child_event(&mut children, &step2, "tc1", now_ms, &tx)
                .await
                .unwrap()
        );
    }

    #[test]
    fn fold_pi_events_run_completed_marks_done_with_status() {
        let cases = [("complete", "complete"), ("failed", "failed")];
        for (record_status, expected) in cases {
            let mut snap = Snapshot::default();
            snap.format = TranscriptFormat::PiEvents;
            fold_line(
                &format!(
                    r#"{{"type":"subagent.run.completed","runId":"r1","status":"{record_status}","durationMs":1000}}"#
                ),
                &mut snap,
            );
            assert!(snap.done);
            assert_eq!(snap.pi_terminal_status.as_deref(), Some(expected));
        }
    }

    #[test]
    fn fold_captures_tool_entries_with_titles_and_results() {
        let mut snap = Snapshot::default();
        fold_line(
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls -la","description":"list"}}]}}"#,
            &mut snap,
        );
        fold_line(
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t2","name":"Read","input":{"file_path":"src/main.rs"}}]}}"#,
            &mut snap,
        );
        // tool_result for t1 (success) and t2 (error) arrive on user lines.
        fold_line(
            r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"t1","is_error":false}]}}"#,
            &mut snap,
        );
        fold_line(
            r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"t2","is_error":true}]}}"#,
            &mut snap,
        );
        let tools = snapshot_tools(&snap);
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0].name, "Bash");
        assert_eq!(tools[0].title.as_deref(), Some("ls -la"));
        assert_eq!(tools[0].ok, Some(true));
        assert_eq!(tools[1].name, "Read");
        assert_eq!(tools[1].title.as_deref(), Some("src/main.rs"));
        assert_eq!(tools[1].ok, Some(false));
        assert_eq!(snap.tool_count, 2);
    }

    #[test]
    fn fold_marks_done_and_result_on_end_turn() {
        let mut snap = Snapshot::default();
        fold_line(
            r#"{"type":"assistant","message":{"stop_reason":"end_turn","content":[{"type":"text","text":"final answer"}]}}"#,
            &mut snap,
        );
        assert!(snap.done);
        assert_eq!(snap.result.as_deref(), Some("final answer"));
    }

    #[test]
    fn fold_skips_non_assistant_and_attachment_lines() {
        let mut snap = Snapshot::default();
        fold_line(
            r#"{"type":"user","message":{"content":"prompt"}}"#,
            &mut snap,
        );
        fold_line(r#"{"attachment":{"type":"skill_listing"}}"#, &mut snap);
        assert_eq!(snap.tool_count, 0);
        assert!(!snap.done);
        assert!(!snap.parsed_any);
    }

    #[test]
    fn fold_counts_parse_errors_without_panicking() {
        let mut snap = Snapshot::default();
        fold_line("not json at all", &mut snap);
        assert_eq!(snap.parse_errors, 1);
        assert!(!snap.parsed_any);
        assert!(format_warning(&snap).is_some());
    }

    #[test]
    fn preview_truncates_long_text() {
        let long = "x".repeat(TEXT_PREVIEW_CHARS + 50);
        let p = preview(&long);
        assert!(p.ends_with('…'));
        assert!(p.chars().count() <= TEXT_PREVIEW_CHARS + 1);
    }

    /// #3232: Claude Code's async-Task transcripts don't always tag the
    /// final assistant record `stop_reason: "end_turn"`. `infer_idle_outcome`
    /// is what an `ABORT_AFTER` idle-timeout falls back on to tell a
    /// sub-agent that actually finished from one genuinely hung.
    #[test]
    fn infer_idle_outcome_distinguishes_finished_from_hung() {
        // (lines, expected status, expected result, warning substring, case)
        let cases = vec![
            (
                vec![
                    r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"Bash"}]}}"#,
                    r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"t1","is_error":false}]}}"#,
                    r#"{"type":"assistant","message":{"stop_reason":null,"content":[{"type":"text","text":"final report"}]}}"#,
                ],
                BackgroundAgentStatus::Completed,
                Some("final report"),
                "completion inferred from final text",
                "final text block, no dangling tool, no end_turn marker: genuinely done (#3232)",
            ),
            (
                vec![
                    r#"{"type":"assistant","message":{"content":[{"type":"text","text":"working on it"}]}}"#,
                    r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"Bash"}]}}"#,
                ],
                BackgroundAgentStatus::Stalled,
                None,
                "no transcript activity",
                "last content is a tool call still awaiting its result: genuinely hung",
            ),
            (
                vec![
                    r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"Bash"},{"type":"text","text":"spoke after the call"}]}}"#,
                ],
                BackgroundAgentStatus::Stalled,
                None,
                "no transcript activity",
                "text after an unresolved tool call: still mid-action, not done",
            ),
            (
                vec!["not json at all"],
                BackgroundAgentStatus::Stalled,
                None,
                "no transcript activity",
                "nothing parsed at all: genuinely hung",
            ),
        ];
        for (lines, expected_status, expected_result, warn_contains, desc) in cases {
            let mut snap = Snapshot::default();
            for line in lines {
                fold_line(line, &mut snap);
            }
            let (status, result, warning) = infer_idle_outcome(&snap);
            assert_eq!(status, expected_status, "{desc}");
            assert_eq!(result.as_deref(), expected_result, "result for: {desc}");
            let warning = warning.unwrap_or_default();
            assert!(
                warning.contains(warn_contains),
                "warning for {desc}: expected {warn_contains:?}, got {warning:?}"
            );
        }
    }

    /// `tools` stops growing at `MAX_TOOLS` to bound the event payload, so
    /// completion state cannot be read off it: a call issued past the cap
    /// would be invisible and a trailing text block would wrongly infer
    /// `Completed`. `unresolved_tools` is uncapped for exactly this reason.
    #[test]
    fn infer_idle_outcome_sees_unresolved_tool_past_the_display_cap() {
        let mut snap = Snapshot::default();
        for i in 0..=MAX_TOOLS {
            fold_line(
                &format!(
                    r#"{{"type":"assistant","message":{{"content":[{{"type":"tool_use","id":"t{i}","name":"Bash"}}]}}}}"#
                ),
                &mut snap,
            );
        }
        // Resolve every call the capped display list actually holds, so the
        // only unresolved one is the call past the cap.
        for i in 0..MAX_TOOLS {
            fold_line(
                &format!(
                    r#"{{"type":"user","message":{{"content":[{{"type":"tool_result","tool_use_id":"t{i}","is_error":false}}]}}}}"#
                ),
                &mut snap,
            );
        }
        fold_line(
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"all done"}]}}"#,
            &mut snap,
        );

        assert_eq!(snap.tools.len(), MAX_TOOLS, "display list stays capped");
        assert!(
            snap.tools.iter().all(|t| t.ok.is_some()),
            "every tool in the capped list is resolved, so the cap hides the dangling one"
        );
        assert_eq!(snap.tool_count as usize, MAX_TOOLS + 1);
        let (status, ..) = infer_idle_outcome(&snap);
        assert_eq!(
            status,
            BackgroundAgentStatus::Stalled,
            "a tool call past the display cap is still unresolved, so not done"
        );
    }
}
