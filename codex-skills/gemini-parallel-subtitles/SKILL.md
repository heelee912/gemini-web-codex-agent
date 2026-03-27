---
name: gemini-parallel-subtitles
description: Build high-recall Japanese subtitle drafts from long video episodes using Gemini Pro 3.1 with Chrome DevTools MCP, multiple parallel Gemini tabs, targeted retry clips, meaning-based Whisper comparison, and timeline-safe final merging.
---

# Gemini Parallel Subtitles

Use this skill when generating Japanese subtitle drafts from long episodes with Gemini, especially when:
- the source must be uploaded as video-with-audio
- Gemini wording is stronger than Whisper, but recall is unstable
- multiple Gemini tabs can accelerate the work
- weak regions need narrower retries or bridge clips
- final quality should be judged by meaning coverage, not cue count

## Core rules

1. Use Chrome DevTools MCP for Gemini browser control.
2. Keep every Gemini job on `Pro 3.1`.
3. Upload only video files that Gemini can read directly.
4. Include dialogue, narration, songs, and preview narration if audible.
5. Exclude onscreen text, credits, sound-effect descriptions, bracketed directions, and speaker labels.
6. Do not trust `code copy` alone; wait until the response is fully complete.
7. Compare Gemini against Whisper by meaning, not raw cue counts.
8. Prefer targeted retries over redoing every segment.

## Standard workflow

### 1. Prepare source segments

- Keep the episode split into Gemini-safe video chunks.
- If a chunk is weak, create either a smaller refined clip or a bridge clip with more context.
- Use `scripts/segment-episode-video.ps1` to create baseline chunks.
- Use `scripts/init-episode-workspace.ps1` to create episode folders and merge manifests.

### 2. Open a Gemini job queue

- Open multiple Gemini tabs in parallel.
- Treat each tab as one job tab.
- For each new tab:
  - open a fresh chat
  - switch to `Pro 3.1`
  - upload the assigned video
  - wait until send is enabled
  - fill the prompt
  - send and move to the next tab

### 3. Use a speech-only prompt

Use a strict prompt with these constraints:
- output exactly one SRT code block
- include dialogue, narration, songs, and short interjections if audible
- exclude onscreen text, credits, sound effects, stage directions, and speaker labels
- do not summarize or omit
- keep SRT timing aligned to actual spoken flow

Use `references/prompt-templates.md` for ready-to-paste base prompts.

### 4. Identify weak segments

After copying Gemini output:
- compare it to the corresponding Whisper interval
- decide whether to accept, retry with a stronger prompt, retry with a narrower clip, or retry with a bridge clip

### 5. Build multiple episode passes

For each episode, create at least three candidates:
- `original pass`
- `altwhole pass`
- `master pass`

### 6. Merge safely

- Shift each segment SRT by cumulative segment duration.
- Preserve text and cue order inside each segment.
- Write the merged episode SRT to a dedicated output path.
- Use `scripts/merge-segment-srts.ps1` when deterministic merging is needed.

### 7. Decide the winner

- Prefer the pass that preserves more real spoken content.
- Reject passes that gain recall by hallucinating content.
- Small manual corrections are acceptable when they clearly improve spoken-content fidelity.
