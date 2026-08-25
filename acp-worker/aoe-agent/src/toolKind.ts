/**
 * Tool-name to ACP `ToolKind` mapping for the structured view's card
 * dispatch. Its own module so it is testable: `index.ts` calls `main()` at
 * module scope, so importing it from a test would connect to stdio.
 */

import type * as acp from "@agentclientprotocol/sdk";

export function classifyKind(toolName: string): acp.ToolKind {
  switch (toolName) {
    case "Read":
      return "read";
    case "Write":
      return "edit";
    case "Bash":
      return "execute";
    // `task` is not in `buildTools`, but models trained against harnesses
    // that do have a subagent tool call it anyway; two such calls landed in
    // the wild (#1904). The AI SDK cannot execute it, so no subagent runs.
    // This only fixes how the call renders: `think` matches what
    // claude-agent-acp and opencode >=1.16.0 report for the same name,
    // which puts it on the think card instead of the generic one.
    case "task":
    case "Task":
      return "think";
    default:
      return "other";
  }
}
