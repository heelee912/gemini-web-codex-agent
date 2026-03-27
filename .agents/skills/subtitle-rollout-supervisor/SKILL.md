---
name: subtitle-rollout-supervisor
description: Use this skill when the task is a long-running subtitle or artifact rollout that must continue across many episode-pass-segment targets without replying after partial progress. This skill is for execution turns that must rebuild state from disk, dispatch one bounded browser batch at a time, verify files, and continue until DONE or HARD_BLOCKED.
---

# Subtitle Rollout Supervisor

## Purpose

This skill turns Codex into a strict rollout supervisor. It removes conversational progress as a completion signal and replaces it with a state-driven batch loop.

## Preconditions

- `AGENTS.md` and `PLAN.md` exist at the working root.
- `.codex/state.json` is present or can be rebuilt from disk.
- The task is to complete a whole rollout grid, not one ad hoc batch.

## Hard contract

1. This is an execution turn, not a discussion turn.
2. Do not send a user-facing answer after partial progress.
3. Partial progress from a subagent is not a result.
4. Only `DONE` or `HARD_BLOCKED` are terminal.
5. One browser-writing worker only.
6. Comparison workers are read-only.
7. Rebuild `.codex/state.json` before the first batch, after every `batch_done`, and after every recovery attempt.
8. The only thing that makes an item complete is a verified file on disk.

## Files used by this skill

- `PLAN.md`
- `.codex/state.json`
- `.codex/PROGRESS.md`
- `tools/build_state.py`
- `tools/next_batch.py`

## Supervisor loop

1. Rebuild `.codex/state.json`
2. Choose the next bounded batch
3. Dispatch exactly one browser-writing worker
4. Verify files on disk
5. Update `.codex/state.json`
6. Dispatch the next batch immediately

## Subagent rules

Browser workers must return JSON only:

    {"status":"batch_done","completed":[...],"blocked":[]}
    {"status":"hard_blocked","completed":[...],"blocked":[...],"recovery_attempts":[...]}

If a worker returns prose or partial progress, steer it to continue. Do not answer the user.

## Validation and fallback rules

- Primary extraction is DOM `code` or `pre`
- If DOM extraction is empty, use snapshot `StaticText` fallback
- If Chrome is blocked by a visible permission dialog, recover through UI before escalation:
  - inspect the window
  - press `Enter` if `Allow` is focused
  - otherwise click `Allow`
  - resume the same batch

## Final reply contract

The only user-facing terminal replies are:

- `DONE`
- `HARD_BLOCKED`
