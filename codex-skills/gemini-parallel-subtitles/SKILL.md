---
name: gemini-parallel-subtitles
description: Build high-recall Japanese subtitle drafts from long video episodes using Gemini Pro 3.1 with Chrome DevTools MCP, multiple parallel Gemini tabs, targeted retry clips, meaning-based Whisper comparison, and timeline-safe final merging. Use when Codex must subtitle anime/video episodes from uploaded video-with-audio segments, especially when quality depends on retrying weak segments, comparing Gemini against Whisper semantically rather than by cue count, and merging segment outputs back to the original episode timeline.
---

# Gemini Parallel Subtitles

Use this skill when generating Japanese subtitle drafts from long episodes with Gemini, especially when:
- the source must be uploaded as video-with-audio, not split into audio-only and muted-video passes
- Gemini quality is higher than Whisper on wording, but recall is unstable
- the work must be accelerated with multiple Gemini tabs running in parallel
- weak segments must be retried with narrower clips or bridge clips
- final quality must be judged by meaning coverage, not raw cue counts

## Core rules

1. Use Chrome DevTools MCP for Gemini browser control.
2. Keep every Gemini job on `Pro 3.1`.
3. Upload only video files that Gemini can read directly.
4. For speech-only passes, include dialogue, narration body, songs, and preview narration.
5. Exclude onscreen text, credits, telops, sound-effect descriptions, bracketed stage directions, and speaker labels.
6. Do not trust `code copy` alone; wait until the response is fully finished before copying.
7. Do not use cue count as the primary validator.
8. Compare Gemini against Whisper by meaning:
   - if Whisper is noisy or truncated but Gemini is coherent, trust Gemini
   - if Gemini repeatedly drops a short but meaningful utterance, retry with a narrower clip
   - if Gemini returns the same result across retries, treat that as evidence the source may genuinely be sparse
9. Prefer targeted retries over redoing every segment.
10. When a route is proven bad, stop using it.

## Standard workflow

### 1. Prepare source segments

- Keep the episode split into Gemini-safe video chunks.
- If a chunk is weak, create one of:
  - `refined clip`: a smaller subsegment around the weak region
  - `bridge clip`: a short clip that gives leading and trailing context around a missing utterance

Use [segment-episode-video.ps1](E:\Media\신통기\codex-skills\gemini-parallel-subtitles\scripts\segment-episode-video.ps1) to create baseline Gemini-safe video chunks from a full episode.
Use [init-episode-workspace.ps1](E:\Media\신통기\codex-skills\gemini-parallel-subtitles\scripts\init-episode-workspace.ps1) to create episode folders and a merge manifest from prepared segments.

### 2. Open a Gemini job queue

- Open multiple Gemini tabs in parallel.
- Treat each tab as one `job tab`.
- For every new tab:
  - open a fresh chat
  - switch the model to `Pro 3.1`
  - upload the assigned video
  - wait until upload processing is complete and send is enabled
  - fill the prompt
  - send and move to the next tab

This queue should overlap Gemini thinking time across tabs instead of waiting on one tab at a time.

### 3. Use a speech-only prompt

Use a strict prompt with these constraints:
- output exactly one SRT code block
- include dialogue, narration body, songs, ending song lyrics, and preview narration if audible
- exclude onscreen text and credits
- exclude sound-effect descriptions, bracketed directions, and speaker labels
- include short interjections, hesitation sounds, and incomplete trailing clauses if actually spoken
- do not summarize or omit
- keep SRT timing aligned to actual spoken flow

Adapt the wording if a segment keeps failing, but preserve those rules.

Use [prompt-templates.md](E:\Media\신통기\codex-skills\gemini-parallel-subtitles\references\prompt-templates.md) for ready-to-paste base prompts:
- `speech-only base`
- `speech-only recall-max`
- `bridge clip`

### 4. Identify weak segments

After copying Gemini output:
- compare it to the corresponding Whisper interval
- ask:
  - Is Whisper merely split differently?
  - Is Whisper garbled while Gemini is cleaner?
  - Did Gemini actually lose a meaningful spoken line?
  - Did Gemini hallucinate labels or effect text?

Mark the segment as:
- `accept`
- `retry with stronger prompt`
- `retry with narrower clip`
- `retry with bridge clip`

### 5. Build multiple episode passes

For one episode, do not stop at a single merge.

Create at least three full-episode pass candidates:
- `original pass`: best currently accepted baseline
- `altwhole pass`: a whole weak-segment retry replacing the weak original segment
- `master pass`: a manually curated pass that keeps the strongest lines from each retry

Use the same segment offsets each time so comparisons are fair.

### 6. Merge safely

When merging:
- shift each segment SRT by the cumulative actual segment durations
- do not concatenate timestamps blindly
- preserve text and cue order inside each segment
- write the merged episode SRT to a dedicated output path

Use [merge-segment-srts.ps1](E:\Media\신통기\codex-skills\gemini-parallel-subtitles\scripts\merge-segment-srts.ps1) when a deterministic merge is needed.

### 7. Decide the winner

Choose the final episode pass by meaning coverage:
- prefer the pass that preserves more real spoken content
- reject passes that gain recall by introducing obvious hallucinations
- short local manual corrections are acceptable if they remove clear OCR/ASR-style mistakes while preserving the spoken content

## Retry heuristics

Use these heuristics when a segment is weak.

### Retry with stronger prompt

Use when:
- Gemini inserts effect descriptions
- Gemini omits brief interjections
- Gemini collapses distinct utterances into one line

### Retry with narrower clip

Use when:
- a small region is consistently weak inside a mostly good segment
- one story beat is under-detected because the larger segment has too much competing speech

### Retry with bridge clip

Use when:
- Gemini keeps dropping the setup line before a clearly detected main line
- Gemini misses a short lead-in such as `昔...`, `え？`, `うーん...`, or a trailing incomplete clause

## Review checklist

Before finalizing an episode, check:
- songs/lyrics that should be spoken are present
- narration was not removed
- short reactions are present if spoken
- no `(ナレーション)`-style labels remain
- no effect descriptions remain
- no obviously wrong proper nouns survived if better evidence exists in other retries
- the merged file lines up with segment offsets

## Current project expectations

For this project:
- prioritize speech-only Japanese subtitle generation first
- use Whisper only as a semantic cross-check
- when a weak region dominates quality, keep retrying that region instead of restarting the whole episode immediately
- after a stable episode workflow is proven, reuse the same method for the rest of the season
