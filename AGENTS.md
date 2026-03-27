# Long-horizon rollout rules

Use the `$subtitle-rollout-supervisor` skill for any request that means "keep supervising work until the whole grid is complete" or "read subagent results and keep dispatching the next batch without replying after partial progress".

## Execution turns versus discussion turns

An execution turn is any turn whose goal is to complete a grid of artifacts such as episode x pass x segment outputs.

During an execution turn:

- Do not send any user-facing answer until one of the two terminal states is true.
- Terminal state 1 is `DONE`: every target item in `.codex/state.json` has `generated=true`, the expected file exists on disk, and file size is greater than zero.
- Terminal state 2 is `HARD_BLOCKED`: the same target item failed three distinct recovery attempts and the blocker is recorded in both `PLAN.md` and `.codex/PROGRESS.md`.
- Never treat partial progress, "current page", "next queue", "I am waiting", or a subagent note as a terminal result.
- Write intermediate progress only to `.codex/PROGRESS.md` and the living sections of `PLAN.md`.

## Planning

For long-running execution turns, open `PLAN.md` before doing anything else and follow it exactly.

If `PLAN.md` conflicts with ad hoc wording in chat, `PLAN.md` wins for execution details, validation, and stop conditions.

Use milestones small enough to verify after each batch.

## State and batch selection

The source of truth is `.codex/state.json`, rebuilt from disk.

Rebuild state before the first batch, after every `batch_done`, and after every recovery attempt.

Prefer earlier work first in this order unless `PLAN.md` overrides it:

1. lower pass number
2. lower episode number
3. lower segment number

## Subagent rules

Use exactly one browser-writing worker at a time.

Quality or comparison workers may read generated files but must never control Chrome and must never write target SRT files.

Spawn workers only for bounded batches of 3 to 5 target items.

A browser worker must return JSON only in one of these forms:

    {"status":"batch_done","completed":[...],"blocked":[]}
    {"status":"hard_blocked","completed":[...],"blocked":[...],"recovery_attempts":[...]}

If a worker returns prose, a queue summary, or partial progress, immediately steer it back to the JSON contract. Do not answer the user.

## Validation and fallback rules

A generated item counts as complete only after the expected SRT file exists on disk and has non-zero size.

Primary extraction is DOM `code` or `pre`.

If DOM extraction is empty, use snapshot `StaticText` fallback.

Do not use the clipboard unless the active browser-writing worker is the only process touching the browser and clipboard is the only remaining recovery path for exact Gemini output preservation.

If Chrome browser automation is blocked by a visible browser- or OS-level permission dialog, perform UI recovery first:

- inspect the current window state
- identify the blocking dialog
- if the Chrome remote debugging permission popup is visible and `Allow` is focused, press `Enter`
- otherwise click the `Allow`/approval button directly
- resume the same batch

## Final reply contract

A `DONE` reply may include only the completed counts and any retry-later queue.

A `HARD_BLOCKED` reply must include the exact remaining items, the repeated blocker, and the failed recovery attempts.
