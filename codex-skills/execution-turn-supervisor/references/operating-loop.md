# Operating Loop

## Primary Done Condition

The task is done only when every item in `.codex/state.json` has:

- `"generated": true`
- `"verified_on_disk": true`

## Reporting Rules

- No user-facing progress summaries during rollout.
- No preambles, plan recaps, or conversational discussion during rollout.
- Intermediate progress belongs only in:
  - `.codex/PROGRESS.md`
  - `.codex/state.json`

## Intervention Rules

Only intervene when one of these is true:

- A real execution error occurred.
- The same batch shows no disk progress for a materially long interval.
- The same item has failed 3 distinct recovery attempts.

When the blocker is a visible browser or OS permission dialog, the first intervention is not termination. It is UI recovery:

- inspect the current window state
- identify the blocking dialog
- if the dialog is the Chrome remote-debugging permission popup and `허용` is the default-focused button, press `Enter`
- otherwise click the approval/continue control directly
- resume the same batch

## Comparison Policy

- Keep exactly 1 browser worker until generation is stable.
- Comparison workers must be file-only.
- Comparison results do not stop the generation loop; they only mark items as `usable`, `retry_later`, or `retry_required` for later passes.

## Browser Recovery Policy

- Chrome DevTools MCP failure does not automatically mean `hard_blocked`.
- If Chrome itself is still running, first try to restore progress by UI recovery before giving up.
- Escalate to `hard_blocked` only after MCP-level retries and UI-level recovery both fail.

## Final Response Policy

Only two user-facing finals are allowed:

- `DONE`
- `HARD_BLOCKED`

Everything else must stay inside the supervisor loop.
