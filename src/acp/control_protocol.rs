//! Typed control channel between the daemon and the `aoe __acp-runner`
//! shim, carried on `<id>.control.sock`. As of Phase C (#2977) this is
//! the ONLY channel: the raw ACP byte relay on `<id>.sock` is retired and
//! the runner is the sole ACP protocol terminator.
//!
//! Phase A of #1054 (runner-side ACP protocol termination): the runner
//! observes the agent's response to the daemon-issued `session/prompt`
//! request and reports a native turn-complete signal over this channel,
//! so the daemon fires `Stopped { reason: "prompt_complete" }`
//! deterministically instead of guessing with the 30s resume-idle
//! watchdog.
//!
//! Phase B (#2976): the runner now owns the ACP handshake and the turn
//! request/response. The daemon drives the handshake inputs over this
//! channel ([`ControlBody::Initialize`] then [`ControlBody::EstablishSession`]);
//! the runner runs `initialize` + `session/new|load|fork` against the
//! agent exactly once, caches the raw results, and returns them as
//! [`ControlBody::Initialized`] / [`ControlBody::SessionReady`]. On every
//! later attach it replays those from cache without touching the agent.
//! Prompts and cancels move here too ([`ControlBody::Prompt`] /
//! [`ControlBody::Cancel`]); the runner assigns the canonical
//! `session/prompt` JSON-RPC id and reports the typed
//! [`ControlBody::PromptCompleted`] outcome.
//!
//! Phase C (#2977) moves the remaining traffic here and retires the relay,
//! adding two generic correlated lanes carrying opaque JSON-RPC payloads:
//!
//! - **Reverse** (agent -> daemon): [`ControlBody::ServerCall`] for the
//!   nine agent-to-client requests (permission, elicitation, fs read/write,
//!   terminal create/output/wait/kill/release), answered by
//!   [`ControlBody::ServerResult`] / [`ControlBody::ServerError`]; and
//!   [`ControlBody::Notify`] for fire-and-forget agent notifications
//!   (`session/update`, the whole event stream).
//! - **Forward** (daemon -> agent): [`ControlBody::AgentCall`] for the
//!   client-to-agent requests the runner does not itself own (`session/set_mode`,
//!   `session/set_config_option`, `session/delete`, `_session/steering`, and
//!   a conversation-reset `session/new`), answered by
//!   [`ControlBody::AgentResult`] / [`ControlBody::AgentError`].
//!
//! Because notifications and turn completion now share one FIFO socket and
//! the runner enqueues them in agent-stdout order, a `PromptCompleted` can
//! no longer overtake the `session/update` frames that preceded it. That
//! closes the cross-socket watermark hazard Phase B had to defer.
//!
//! Wire format: each frame is a 4-byte big-endian length prefix followed
//! by that many bytes of JSON (a serialized [`ControlBody`]). Length
//! framing rather than newline delimiting, so an opaque nested payload
//! cannot be confused with a newline in the body.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Bumped when the frame set changes in a wire-incompatible way. The
/// runner announces it in [`ControlBody::Hello`]; a daemon that does not
/// recognize the version keeps the legacy resume-idle watchdog rather
/// than trusting the channel. v2 (#2976) added the runner-owned handshake
/// and typed prompt/cancel frames; the [`ControlBody::PromptCompleted`]
/// shape changed, so v1 and v2 are wire-incompatible and the version gate
/// is what keeps a mixed-version daemon/runner pair from misreading each
/// other. v3 (#2977) adds the reverse and forward call lanes; a v3 runner no
/// longer binds `<id>.sock` at all, so a pre-v3 daemon cannot drive it and
/// the gate is load-bearing rather than merely defensive.
pub const CONTROL_PROTOCOL_VERSION: u32 = 3;

/// Hard cap on a single control frame. Phase A frames are tiny; reject
/// anything larger as a framing error instead of allocating a huge
/// buffer for a corrupt length prefix.
pub const MAX_CONTROL_FRAME_BYTES: u32 = 16 * 1024 * 1024;

/// A single control frame. `kind` tags the variant so the wire form is
/// self-describing and forward-compatible: an unknown variant fails to
/// deserialize rather than being silently misread.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ControlBody {
    // ---- runner -> daemon ----
    /// First frame the runner sends on a fresh control connection. Lets
    /// the daemon confirm the protocol version and the session identity
    /// it dialed.
    Hello {
        control_protocol_version: u32,
        session_id: String,
    },
    /// Runner's answer to [`ControlBody::Initialize`]: the raw ACP
    /// `initialize` result (an `InitializeResponse` serialized to JSON).
    /// Produced by running `initialize` against the agent on the first
    /// attach and replayed verbatim from cache on every later attach. The
    /// daemon deserializes it into the crate `InitializeResponse` to drive
    /// its capability consumers.
    Initialized { result: serde_json::Value },
    /// Runner's answer to [`ControlBody::EstablishSession`]: the
    /// established ACP session id plus the raw session response result
    /// (a `NewSessionResponse` / `LoadSessionResponse` serialized to
    /// JSON) so the daemon can extract modes / config options. Replayed
    /// from cache on later attaches.
    SessionReady {
        acp_session_id: String,
        result: serde_json::Value,
    },
    /// The runner-owned handshake failed (agent incompatible, `session/new`
    /// error, transport failure). Carries the raw JSON-RPC error object
    /// (`{code, message, data?}`) so the daemon can reconstruct the crate
    /// error verbatim and surface the same `AgentStartupError` (including
    /// `data.details` remediation) it would have on the byte-relay path,
    /// instead of hanging on a handshake that will never complete. A
    /// transport failure with no agent error synthesizes a minimal object.
    HandshakeFailed { error: serde_json::Value },
    /// The runner observed the agent's response to the `session/prompt`
    /// request it issued. `prompt_req_id` is the JSON-RPC id the runner
    /// assigned. `outcome` is the typed turn result.
    PromptCompleted {
        prompt_req_id: i64,
        outcome: PromptOutcome,
    },
    /// An agent-to-client request the daemon must service (permission,
    /// elicitation, fs, terminal). `params` is the raw JSON-RPC params;
    /// the daemon deserializes it into the crate request type keyed by
    /// `method`. `call_id` is allocated by the runner and is the sole
    /// correlation handle: the agent's own JSON-RPC id stays inside the
    /// runner, which alone knows its JSON type (a string id from one
    /// adapter and a numeric id from another must both round-trip).
    ///
    /// Answered by exactly one [`ControlBody::ServerResult`] or
    /// [`ControlBody::ServerError`] carrying the same `call_id`.
    ServerCall {
        call_id: u64,
        method: String,
        params: serde_json::Value,
    },
    /// A fire-and-forget agent notification, forwarded verbatim. Today
    /// that is `session/update` (the entire event stream); an unrecognized
    /// notification method is forwarded too rather than dropped, so a
    /// newly-emitting adapter shows up in the daemon's logs instead of
    /// vanishing.
    Notify {
        method: String,
        params: serde_json::Value,
    },
    /// The runner's answer to a [`ControlBody::AgentCall`]: the agent's
    /// raw JSON-RPC `result`.
    AgentResult {
        call_id: u64,
        result: serde_json::Value,
    },
    /// The agent answered a [`ControlBody::AgentCall`] with an error
    /// envelope, or the runner could not complete it (agent gone, deadline
    /// expired).
    AgentError { call_id: u64, error: JsonRpcError },

    // ---- daemon -> runner ----
    /// First frame the daemon sends after [`ControlBody::Hello`],
    /// acknowledging the version it will speak.
    Attach { control_protocol_version: u32 },
    /// The ACP `initialize` request params (an `InitializeRequest`
    /// serialized to JSON). The runner injects the JSON-RPC envelope + id.
    /// On a runner that already handshook, the params are ignored and the
    /// cached [`ControlBody::Initialized`] is replayed.
    Initialize { request: serde_json::Value },
    /// The session-creation request the runner should issue: `method` is
    /// `session/new`, `session/load`, or `session/fork`, and `request` is
    /// the matching params. Ignored (cache replayed) once the runner has
    /// an established session.
    EstablishSession {
        method: String,
        request: serde_json::Value,
    },
    /// Run a turn. `request` is the ACP `session/prompt` params
    /// (`PromptRequest`); the runner assigns the canonical JSON-RPC id and
    /// tracks the response.
    Prompt { request: serde_json::Value },
    /// Cancel the in-flight turn (maps to a `session/cancel` notification).
    Cancel,
    /// A client-to-agent request the runner does not own: `session/set_mode`,
    /// `session/set_config_option`, `session/delete`, `_session/steering`,
    /// or a conversation-reset `session/new`. The runner injects its own
    /// JSON-RPC envelope and id, correlates the agent's response, and
    /// answers with [`ControlBody::AgentResult`] / [`ControlBody::AgentError`]
    /// carrying the same `call_id`. `call_id` is allocated by the daemon;
    /// the two lanes have independent id spaces and never collide because
    /// each is only ever matched against its own pending map.
    AgentCall {
        call_id: u64,
        method: String,
        params: serde_json::Value,
    },
    /// The daemon's answer to a [`ControlBody::ServerCall`]: the JSON-RPC
    /// `result` to hand back to the agent.
    ServerResult {
        call_id: u64,
        result: serde_json::Value,
    },
    /// The daemon could not service a [`ControlBody::ServerCall`]. The
    /// runner forwards this as the request's JSON-RPC error envelope.
    ServerError { call_id: u64, error: JsonRpcError },
}

/// A JSON-RPC error object. Typed rather than a bare `Value` so neither
/// side can emit a malformed envelope that the other has to guess at; the
/// agent-facing wire form is exactly these three fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// JSON-RPC "internal error". Used when the daemon produced an answer this
/// side could not make sense of. Distinct from JSON-RPC's -32601
/// "method not found", which the crate's own dispatch answers for an
/// unhandled method before the shim ever sees it.
pub const INTERNAL_ERROR: i64 = -32603;

/// Reserved-range code for "the daemon went away before answering". Shared
/// by the runner's disconnect sweep and its deadline expiry so an agent
/// sees one consistent code for an unanswerable reverse call. Matches the
/// code the pre-Phase-C relay sweep used, so adapter-side handling of it is
/// unchanged.
pub const DAEMON_GONE: i64 = -32001;

impl JsonRpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
}

/// Typed result of a runner-owned turn. Replaces Phase A's
/// `stop_reason`-only form so an agent error-envelope response is
/// surfaced as an error rather than collapsed into a silent stop.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PromptOutcome {
    /// Normal completion. `stop_reason` is the ACP `stopReason` from the
    /// response result when present.
    Completed { stop_reason: Option<String> },
    /// The agent answered the prompt with a JSON-RPC error envelope. The
    /// `data` object is preserved so the daemon can still classify a
    /// rate-limit error (which carries `errorKind` / `resets_at` there).
    Error {
        code: i64,
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        data: Option<serde_json::Value>,
    },
    /// The turn ended because the runner lost the agent (process exit,
    /// transport failure) before a response arrived.
    Aborted,
}

/// Encode a frame: 4-byte big-endian length prefix, then the JSON body.
pub fn encode_frame(body: &ControlBody) -> Result<Vec<u8>> {
    let json = serde_json::to_vec(body)?;
    let len = u32::try_from(json.len())
        .map_err(|_| anyhow::anyhow!("control frame exceeds u32 length"))?;
    if len > MAX_CONTROL_FRAME_BYTES {
        bail!("control frame {len} bytes exceeds cap {MAX_CONTROL_FRAME_BYTES}");
    }
    let mut buf = Vec::with_capacity(4 + json.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&json);
    Ok(buf)
}

/// Write one frame and flush.
pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, body: &ControlBody) -> Result<()> {
    let buf = encode_frame(body)?;
    w.write_all(&buf).await?;
    w.flush().await?;
    Ok(())
}

/// Read one frame. Returns `Ok(None)` on a clean EOF at a frame boundary
/// (the peer closed the socket), so callers can treat that as a normal
/// disconnect rather than an error.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<ControlBody>> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_CONTROL_FRAME_BYTES {
        bail!("control frame length {len} exceeds cap {MAX_CONTROL_FRAME_BYTES}");
    }
    let mut body = vec![0u8; len as usize];
    r.read_exact(&mut body).await?;
    let parsed: ControlBody = serde_json::from_slice(&body)?;
    Ok(Some(parsed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn roundtrip(body: ControlBody) -> ControlBody {
        let encoded = encode_frame(&body).expect("encode");
        // Length prefix plus a body that deserializes back to the same value.
        let len = u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert_eq!(len as usize, encoded.len() - 4);
        serde_json::from_slice(&encoded[4..]).expect("decode")
    }

    #[test]
    fn hello_roundtrips() {
        let body = ControlBody::Hello {
            control_protocol_version: CONTROL_PROTOCOL_VERSION,
            session_id: "abc-123".into(),
        };
        assert_eq!(roundtrip(body.clone()), body);
    }

    #[test]
    fn prompt_completed_roundtrips() {
        let body = ControlBody::PromptCompleted {
            prompt_req_id: 42,
            outcome: PromptOutcome::Completed {
                stop_reason: Some("end_turn".into()),
            },
        };
        assert_eq!(roundtrip(body.clone()), body);
    }

    #[test]
    fn prompt_outcome_variants_roundtrip() {
        for outcome in [
            PromptOutcome::Completed { stop_reason: None },
            PromptOutcome::Error {
                code: -32000,
                message: "boom".into(),
                data: Some(serde_json::json!({"errorKind": "rate_limit"})),
            },
            PromptOutcome::Aborted,
        ] {
            let body = ControlBody::PromptCompleted {
                prompt_req_id: 1,
                outcome: outcome.clone(),
            };
            assert_eq!(roundtrip(body.clone()), body);
        }
    }

    #[test]
    fn handshake_frames_roundtrip() {
        for body in [
            ControlBody::Initialize {
                request: serde_json::json!({"protocolVersion": 1}),
            },
            ControlBody::Initialized {
                result: serde_json::json!({"agentCapabilities": {}}),
            },
            ControlBody::EstablishSession {
                method: "session/new".into(),
                request: serde_json::json!({"cwd": "/tmp"}),
            },
            ControlBody::SessionReady {
                acp_session_id: "sess-1".into(),
                result: serde_json::json!({"sessionId": "sess-1"}),
            },
            ControlBody::HandshakeFailed {
                error: serde_json::json!({"code": -32603, "message": "incompatible"}),
            },
            ControlBody::Prompt {
                request: serde_json::json!({"sessionId": "sess-1", "prompt": []}),
            },
            ControlBody::Cancel,
        ] {
            assert_eq!(roundtrip(body.clone()), body);
        }
    }

    #[test]
    fn v3_lane_frames_roundtrip() {
        for body in [
            ControlBody::ServerCall {
                call_id: 1,
                method: "session/request_permission".into(),
                params: serde_json::json!({"sessionId": "s"}),
            },
            ControlBody::ServerResult {
                call_id: 1,
                result: serde_json::json!({"outcome": {"outcome": "cancelled"}}),
            },
            ControlBody::ServerError {
                call_id: 1,
                error: JsonRpcError::new(INTERNAL_ERROR, "no handler"),
            },
            ControlBody::Notify {
                method: "session/update".into(),
                params: serde_json::json!({"sessionId": "s"}),
            },
            ControlBody::AgentCall {
                call_id: 2,
                method: "session/set_mode".into(),
                params: serde_json::json!({"modeId": "default"}),
            },
            ControlBody::AgentResult {
                call_id: 2,
                result: serde_json::json!({}),
            },
            ControlBody::AgentError {
                call_id: 2,
                error: JsonRpcError {
                    code: DAEMON_GONE,
                    message: "gone".into(),
                    data: Some(serde_json::json!({"errorKind": "rate_limit"})),
                },
            },
        ] {
            assert_eq!(roundtrip(body.clone()), body);
        }
    }

    #[test]
    fn json_rpc_error_omits_absent_data() {
        // The agent-facing envelope must not carry `"data": null`; some
        // adapters treat a present-but-null data field as a payload.
        let encoded = serde_json::to_value(JsonRpcError::new(DAEMON_GONE, "gone")).unwrap();
        assert_eq!(
            encoded,
            serde_json::json!({"code": -32001, "message": "gone"})
        );
    }

    #[tokio::test]
    async fn write_then_read_frame() {
        let body = ControlBody::PromptCompleted {
            prompt_req_id: 7,
            outcome: PromptOutcome::Aborted,
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &body).await.expect("write");
        let mut cursor = Cursor::new(buf);
        let got = read_frame(&mut cursor).await.expect("read");
        assert_eq!(got, Some(body));
    }

    #[tokio::test]
    async fn multiple_frames_in_one_stream() {
        let a = ControlBody::Hello {
            control_protocol_version: CONTROL_PROTOCOL_VERSION,
            session_id: "s".into(),
        };
        let b = ControlBody::PromptCompleted {
            prompt_req_id: 1,
            outcome: PromptOutcome::Completed {
                stop_reason: Some("cancelled".into()),
            },
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &a).await.unwrap();
        write_frame(&mut buf, &b).await.unwrap();
        let mut cursor = Cursor::new(buf);
        assert_eq!(read_frame(&mut cursor).await.unwrap(), Some(a));
        assert_eq!(read_frame(&mut cursor).await.unwrap(), Some(b));
        assert_eq!(read_frame(&mut cursor).await.unwrap(), None);
    }

    #[tokio::test]
    async fn clean_eof_returns_none() {
        let mut cursor = Cursor::new(Vec::new());
        assert_eq!(read_frame(&mut cursor).await.unwrap(), None);
    }

    #[tokio::test]
    async fn oversized_length_prefix_is_rejected() {
        // Length prefix past the cap, with no body: must error, not
        // attempt a multi-gigabyte allocation.
        let mut buf = Vec::new();
        buf.extend_from_slice(&(MAX_CONTROL_FRAME_BYTES + 1).to_be_bytes());
        let mut cursor = Cursor::new(buf);
        assert!(read_frame(&mut cursor).await.is_err());
    }

    #[tokio::test]
    async fn truncated_body_is_error_not_eof() {
        // A full length prefix but a short body is a corrupt frame, not a
        // clean close.
        let mut buf = Vec::new();
        buf.extend_from_slice(&16u32.to_be_bytes());
        buf.extend_from_slice(b"only-4"); // fewer than 16 bytes
        let mut cursor = Cursor::new(buf);
        assert!(read_frame(&mut cursor).await.is_err());
    }
}
