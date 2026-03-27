---
name: execution-turn-supervisor
description: Use this skill when a long-running Codex task must not stop at intermediate progress, especially when supervising subagents with a state.json-driven loop, JSON-only subagent outputs, partial-progress rejection, and DONE/HARD_BLOCKED-only user responses.
---

# Execution Turn Supervisor

## Overview

Use this skill to keep Codex in an execution-turn mindset for long-horizon work. It prevents the parent agent from treating intermediate summaries as completion, and it forces progress to flow through state files plus strict subagent result contracts.

## When To Use

Use this skill when all of the following are true:

- The real done condition is large and global, not "finish the current small batch".
- Subagents are involved.
- Partial progress must not be treated as a terminal result.
- The task needs repeated `dispatch -> verify -> dispatch next` supervision.
- Intermediate progress should be written to files, not said to the user.

Typical examples:

- Multi-episode subtitle generation with many segment/pass combinations.
- Long-running browser automation supervised through small subagent batches.
- Any workflow where the parent agent must keep reassigning work until every item in `.codex/state.json` is complete.

## Execution Contract

Treat the current turn as an execution turn, not a discussion turn.

- The parent agent may only produce a user-facing final answer when the global done condition is satisfied or the task is hard blocked.
- Intermediate progress belongs in `.codex/PROGRESS.md` and `.codex/state.json`, not in user chat.
- A subagent report is not completion unless it matches the JSON contract in [contracts.md](references/contracts.md).

## State Files

Use these files as the source of truth:

- `.codex/state.json`
- `.codex/PROGRESS.md`

Rules:

- Read `.codex/state.json` before dispatching each batch.
- After a successful batch, verify generated files on disk, then update `.codex/state.json`.
- Write short operational notes to `.codex/PROGRESS.md` only when useful for the next supervisor step.

## Subagent Contract

Subagents must return JSON only. The only valid terminal payloads are defined in [contracts.md](references/contracts.md).

Important:

- Natural-language progress such as "current page", "next queue", "still running", or "save pending" is invalid.
- If a subagent returns partial progress, do not answer the user. Immediately steer the same subagent to continue.

## Supervisor Loop

Follow this loop exactly:

1. Read `.codex/state.json`.
2. Claim the next bounded batch.
3. Dispatch the worker.
4. Verify files on disk.
5. Update `.codex/state.json`.
6. Dispatch the next batch.
7. Repeat until done or hard blocked.

The parent agent is a scheduler and verifier. Do not drift into status-explainer mode.

## Recovery Rules

- Preferred batch size: 3 items.
- Only increase to 5 after the browser generation loop is stable.
- If a subagent returns partial progress, steer it immediately and keep the same batch open.
- If Chrome automation is blocked by a browser- or OS-level permission dialog, try UI-based recovery first instead of terminating the run.
- UI-based recovery includes inspecting the current window state, capturing the visible prompt, and clicking the blocking approval control if it is required for the intended browser flow.
- For the specific Chrome remote-debugging popup (`원격 디버깅을 허용하시겠습니까?`), the browser worker should:
  1. activate the Chrome Gemini window,
  2. try `Enter` first if the default focus is on `허용`,
  3. otherwise explicitly click the `허용` button,
  4. then resume the same batch.
- If the same item fails 3 distinct recovery attempts, escalate that item to `hard_blocked`.

## Worker Roles

- Browser worker: exactly 1. This worker is the only agent allowed to touch Chrome.
- Comparison workers: file-only. They read artifacts from disk and must not touch Chrome.
- Parent supervisor: no browser work unless a true recovery intervention is required.
- Recovery intervention may include Windows UI interaction when Chrome DevTools MCP is alive but browser progress is blocked by a visible modal or permission prompt.

## Batch Policy

Prefer many small batches over one large batch.

- Start with size 3.
- Keep browser generation isolated to one worker.
- Add comparison workers only after generation is moving reliably.
- Never mix "generate everything" and "compare everything" into one request.

## References

- JSON contracts: [contracts.md](references/contracts.md)
- Operating loop and intervention rules: [operating-loop.md](references/operating-loop.md)
