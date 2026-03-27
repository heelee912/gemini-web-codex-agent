# Contracts

## Allowed Terminal Shapes

Subagents may finish with exactly one of these shapes.

### Batch Done

```json
{
  "status": "batch_done",
  "completed": [
    {
      "ep": 1,
      "pass": 1,
      "seg": 6,
      "path": "episode-01/raw_speech_only/pass1/segment-06.srt",
      "bytes": 4211
    }
  ],
  "blocked": []
}
```

### Hard Blocked

```json
{
  "status": "hard_blocked",
  "completed": [
    {
      "ep": 2,
      "pass": 1,
      "seg": 1,
      "path": "episode-02/raw_speech_only/pass1/segment-01.srt",
      "bytes": 3880
    }
  ],
  "blocked": [
    {
      "ep": 2,
      "pass": 1,
      "seg": 2,
      "reason": "DOM empty after 3 recovery attempts"
    }
  ],
  "recovery_attempts": [
    "fresh-chat-retry",
    "snapshot-fallback",
    "page-reopen"
  ]
}
```

## Forbidden Outputs

Treat these as failures:

- "current page is ..."
- "next queue is ..."
- "still running ..."
- "save pending ..."
- Any prose-only progress note
- Any response that is not valid JSON

If a subagent returns one of those, the parent must steer it to continue instead of replying to the user.

## Recovery Attempt Expectations

Before returning `hard_blocked`, a browser worker should try the relevant recovery path for the actual failure mode.

Examples:

- `fresh-chat-retry`
- `snapshot-fallback`
- `page-reopen`
- `ui-approval-click`
- `window-reactivate`
- `chrome-allow-enter`

If a browser or OS modal is visibly blocking Chrome automation, `ui-approval-click` must be attempted before `hard_blocked`.
For the specific Chrome remote-debugging permission popup, `chrome-allow-enter` is an acceptable first recovery attempt when the default focus is already on `허용`.
