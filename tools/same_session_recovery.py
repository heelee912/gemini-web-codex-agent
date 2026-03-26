from __future__ import annotations

import json
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable


MAX_STATE_HISTORY = 100
WAKE_RETRY_DELAY_SECONDS = 5
NON_PRO_LOOP_CATEGORIES = {"worker_interrupted", "automation_stalled", "supervisor_required"}
LOOP_GUARD_SINGLE_ITEM_STREAK_THRESHOLD = 3
LOOP_GUARD_MULTI_ITEM_STREAK_THRESHOLD = 5
LOOP_GUARD_BASE_DELAY_SECONDS = 60
LOOP_GUARD_MAX_DELAY_SECONDS = 300


def now_stamp() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z %Z")


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        delete=False,
        dir=path.parent,
        prefix=f"{path.name}.",
        suffix=".tmp",
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        temp_path = Path(handle.name)
    temp_path.replace(path)


def append_json_line(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False))
        handle.write("\n")


def append_limited_history(history: list[dict[str, Any]], payload: dict[str, Any], limit: int = MAX_STATE_HISTORY) -> None:
    history.append(payload)
    overflow = len(history) - limit
    if overflow > 0:
        del history[:overflow]


def truncate_text(text: str, limit: int = 600) -> str:
    if len(text) <= limit:
        return text
    return f"{text[:limit]}...(truncated)"


def issue_batch_key(issue: dict[str, Any]) -> str:
    batch = sorted(str(item) for item in issue.get("batch") or [])
    return ", ".join(batch)


def issue_category_family(issue: dict[str, Any]) -> str:
    category = str(issue.get("category") or "")
    if category in NON_PRO_LOOP_CATEGORIES:
        return "non_pro_worker_cycle"
    return category


def build_issue_record(issue: dict[str, Any]) -> dict[str, Any]:
    return {
        "at": now_stamp(),
        "category": str(issue.get("category") or ""),
        "category_family": issue_category_family(issue),
        "batch": list(issue.get("batch") or []),
        "batch_key": issue_batch_key(issue),
        "explicit_stop_requested": bool(issue.get("explicit_stop_requested")),
    }


def summarize_repeated_issue(issue_history: list[dict[str, Any]], issue: dict[str, Any]) -> dict[str, Any]:
    current_batch_key = issue_batch_key(issue)
    current_family = issue_category_family(issue)
    current_batch_size = len(issue.get("batch") or [])
    same_batch_family_streak = 0
    raw_categories: set[str] = set()
    explicit_stop_requested_seen = False

    for entry in reversed(issue_history):
        if str(entry.get("batch_key") or "") != current_batch_key:
            break
        if str(entry.get("category_family") or "") != current_family:
            break
        same_batch_family_streak += 1
        raw_categories.add(str(entry.get("category") or ""))
        explicit_stop_requested_seen = explicit_stop_requested_seen or bool(entry.get("explicit_stop_requested"))

    threshold = (
        LOOP_GUARD_SINGLE_ITEM_STREAK_THRESHOLD
        if current_batch_size <= 1
        else LOOP_GUARD_MULTI_ITEM_STREAK_THRESHOLD
    )
    repeat_loop_detected = explicit_stop_requested_seen or same_batch_family_streak >= threshold
    delay_multiplier = max(1, same_batch_family_streak - threshold + 1)
    recommended_delay_seconds = min(
        LOOP_GUARD_MAX_DELAY_SECONDS,
        LOOP_GUARD_BASE_DELAY_SECONDS * delay_multiplier,
    )

    return {
        "batch_key": current_batch_key,
        "batch_size": current_batch_size,
        "category_family": current_family,
        "same_batch_family_streak": same_batch_family_streak,
        "raw_categories": sorted(category for category in raw_categories if category),
        "explicit_stop_requested_seen": explicit_stop_requested_seen,
        "repeat_loop_detected": repeat_loop_detected,
        "recommended_delay_seconds": recommended_delay_seconds,
    }


def should_force_loop_guard_wait(
    issue: dict[str, Any],
    repeat_analysis: dict[str, Any],
    decision: "RecoveryDecision",
) -> bool:
    if decision.recovery_action.kind != "retry_now":
        return False
    if issue_category_family(issue) != "non_pro_worker_cycle":
        return False
    return bool(repeat_analysis.get("repeat_loop_detected"))


def forced_loop_guard_decision(
    decision: "RecoveryDecision",
    repeat_analysis: dict[str, Any],
) -> "RecoveryDecision":
    batch_key = str(repeat_analysis.get("batch_key") or "current batch")
    streak = int(repeat_analysis.get("same_batch_family_streak") or 0)
    delay_seconds = int(repeat_analysis.get("recommended_delay_seconds") or LOOP_GUARD_BASE_DELAY_SECONDS)
    diagnosis = (
        f"{decision.diagnosis} Python loop guard observed {streak} consecutive same-batch "
        f"non-Pro recoveries for {batch_key} and converted the immediate retry into a cooldown."
    )
    reason = (
        f"Repeated same-batch non-Pro recovery loop detected for {batch_key}; "
        "pause before the next supervisor inspection instead of relaunching immediately again."
    )
    return RecoveryDecision(
        decision_version=1,
        workflow_status="continue",
        diagnosis=diagnosis,
        recovery_action=RecoveryAction(kind="wait", reason=reason, delay_seconds=delay_seconds),
    )


def extract_json_object(text: str) -> dict[str, Any]:
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            payload, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    raise ValueError(f"Assistant reply did not contain a JSON object: {truncate_text(text)}")


class CodexResumeError(RuntimeError):
    pass


class CodexResumeBridge:
    def __init__(
        self,
        session_id: str,
        codex_command: str,
        workdir: Path,
        runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self._session_id = session_id
        self._codex_command = codex_command
        self._workdir = workdir
        self._runner = runner

    @property
    def session_id(self) -> str:
        return self._session_id

    def wake_same_session(self, prompt: str) -> str:
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=False) as last_message_file:
            last_message_path = Path(last_message_file.name)

        command = [
            self._codex_command,
            "exec",
            "resume",
            self._session_id,
            "-",
            "--skip-git-repo-check",
            "--json",
            "-o",
            str(last_message_path),
        ]
        completed = self._runner(
            command,
            cwd=self._workdir,
            input=prompt,
            text=True,
            capture_output=True,
        )
        try:
            if completed.returncode != 0:
                raise CodexResumeError(
                    f"codex resume failed with exit code {completed.returncode}: "
                    f"{truncate_text(completed.stderr or completed.stdout)}"
                )
            if not last_message_path.exists():
                raise CodexResumeError("codex resume finished without an output-last-message file")
            return last_message_path.read_text(encoding="utf-8").strip()
        finally:
            last_message_path.unlink(missing_ok=True)


@dataclass(frozen=True)
class RecoveryAction:
    kind: str
    reason: str
    delay_seconds: int = 0

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RecoveryAction":
        action = cls(
            kind=str(payload.get("kind", "")),
            reason=str(payload.get("reason", "")),
            delay_seconds=int(payload.get("delay_seconds") or 0),
        )
        action.validate()
        return action

    def validate(self) -> None:
        if self.kind not in {"retry_now", "wait"}:
            raise ValueError("recovery_action.kind must be retry_now or wait")
        if not self.reason:
            raise ValueError("recovery_action.reason is required")
        if self.kind == "wait" and self.delay_seconds <= 0:
            raise ValueError("wait requires delay_seconds > 0")

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "reason": self.reason,
            "delay_seconds": self.delay_seconds,
        }


@dataclass(frozen=True)
class RecoveryDecision:
    decision_version: int
    workflow_status: str
    diagnosis: str
    recovery_action: RecoveryAction

    @classmethod
    def from_text(cls, text: str) -> "RecoveryDecision":
        payload = extract_json_object(text)
        decision = cls(
            decision_version=int(payload.get("decision_version", 0)),
            workflow_status=str(payload.get("workflow_status", "")),
            diagnosis=str(payload.get("diagnosis", "")),
            recovery_action=RecoveryAction.from_dict(payload.get("recovery_action") or {}),
        )
        decision.validate()
        return decision

    def validate(self) -> None:
        if self.decision_version != 1:
            raise ValueError("decision_version must be 1")
        if self.workflow_status != "continue":
            raise ValueError("workflow_status must be continue")
        if not self.diagnosis:
            raise ValueError("diagnosis is required")
        self.recovery_action.validate()

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_version": self.decision_version,
            "workflow_status": self.workflow_status,
            "diagnosis": self.diagnosis,
            "recovery_action": self.recovery_action.to_dict(),
        }


class RecoveryStateStore:
    def __init__(self, state_path: Path, event_log_path: Path, session_id: str, codex_command: str) -> None:
        self._session_id = session_id
        self._codex_command = codex_command
        self.state_path = state_path
        self.event_log_path = event_log_path

    def _fresh_state(self) -> dict[str, Any]:
        return {
            "session_id": self._session_id,
            "codex_command": self._codex_command,
            "wake_count": 0,
            "wake_history": [],
            "issue_history": [],
            "last_decision": None,
            "last_wake": None,
            "last_issue": None,
            "last_issue_repeat_analysis": None,
            "session_rebind_history": [],
            "created_at": now_stamp(),
            "updated_at": now_stamp(),
        }

    @staticmethod
    def _apply_defaults(state: dict[str, Any]) -> None:
        state.setdefault("wake_count", 0)
        state.setdefault("wake_history", [])
        state.setdefault("issue_history", [])
        state.setdefault("last_decision", None)
        state.setdefault("last_wake", None)
        state.setdefault("last_issue", None)
        state.setdefault("last_issue_repeat_analysis", None)
        state.setdefault("session_rebind_history", [])

    def _rebind_state(self, state: dict[str, Any], previous_session_id: str) -> dict[str, Any]:
        rebind_record = {
            "from_session_id": previous_session_id,
            "to_session_id": self._session_id,
            "rebound_at": now_stamp(),
            "previous_last_issue": state.get("last_issue"),
            "previous_last_decision": state.get("last_decision"),
        }
        history = list(state.get("session_rebind_history") or [])
        append_limited_history(history, rebind_record)

        rebound = self._fresh_state()
        rebound["created_at"] = str(state.get("created_at") or rebound["created_at"])
        rebound["session_rebind_history"] = history
        rebound["rebound_from_session_id"] = previous_session_id
        rebound["rebound_at"] = rebind_record["rebound_at"]
        return rebound

    def load_or_initialize(self) -> dict[str, Any]:
        if self.state_path.exists():
            state = json.loads(self.state_path.read_text(encoding="utf-8"))
            session_id = state.get("session_id")
            if session_id and session_id != self._session_id:
                rebound = self._rebind_state(state, previous_session_id=str(session_id))
                self.save(rebound)
                self.record_event(
                    "recovery_state_rebound",
                    {
                        "from_session_id": session_id,
                        "to_session_id": self._session_id,
                    },
                )
                return rebound
            self._apply_defaults(state)
            return state

        state = self._fresh_state()
        self.save(state)
        return state

    def save(self, state: dict[str, Any]) -> None:
        state["updated_at"] = now_stamp()
        atomic_write_json(self.state_path, state)

    def record_event(self, event_type: str, payload: dict[str, Any]) -> None:
        append_json_line(
            self.event_log_path,
            {
                "timestamp": now_stamp(),
                "event_type": event_type,
                "session_id": self._session_id,
                "payload": payload,
            },
        )


def build_recovery_prompt(issue: dict[str, Any], recovery_state: dict[str, Any], rollout_snapshot: dict[str, Any]) -> str:
    repeat_analysis = recovery_state.get("last_issue_repeat_analysis") or {}
    return (
        "Resume the same Codex session that owns this Teogonia rollout.\n"
        "You are deciding how Python should recover and continue without human intervention.\n"
        "Constraints:\n"
        "- This same Codex session is the continuity anchor.\n"
        "- Never ask for manual re-invocation or manual intervention.\n"
        "- Never allow fallback to fast mode.\n"
        "- If the issue is a Pro limit or Pro model selection issue, you may choose wait.\n"
        "- If issue.resume_hint contains a concrete wait_seconds or resume_at_iso from the Gemini UI, treat that as the primary timing evidence.\n"
        "- For non-Pro issues, prefer retry_now unless waiting is clearly necessary.\n"
        "- If repeated_issue_analysis.repeat_loop_detected is true, treat immediate retry_now as a probable loop unless there is concrete new evidence that the state materially changed.\n"
        "- If repeated_issue_analysis.explicit_stop_requested_seen is true, do not snap back into an immediate relaunch without a short cooldown or a materially different recovery basis.\n"
        "- Reply with JSON only.\n\n"
        "Return exactly this schema:\n"
        "{\n"
        '  "decision_version": 1,\n'
        '  "workflow_status": "continue",\n'
        '  "diagnosis": "short diagnosis",\n'
        '  "recovery_action": {\n'
        '    "kind": "retry_now|wait",\n'
        '    "reason": "short reason",\n'
        '    "delay_seconds": 0\n'
        "  }\n"
        "}\n\n"
        f"Recovery state:\n{json.dumps(recovery_state, ensure_ascii=False, indent=2)}\n\n"
        f"Repeated issue analysis:\n{json.dumps(repeat_analysis, ensure_ascii=False, indent=2)}\n\n"
        f"Rollout snapshot:\n{json.dumps(rollout_snapshot, ensure_ascii=False, indent=2)}\n\n"
        f"Issue:\n{json.dumps(issue, ensure_ascii=False, indent=2)}\n"
    )


def build_invalid_reply_prompt(previous_reply: str, error: Exception) -> str:
    return (
        "Your previous recovery reply was invalid.\n"
        f"Validation error: {error}\n"
        f"Previous reply: {truncate_text(previous_reply)}\n"
        "Reply again with JSON only using decision_version, workflow_status, diagnosis, and recovery_action."
    )


def request_recovery_decision(
    bridge: CodexResumeBridge,
    store: RecoveryStateStore,
    issue: dict[str, Any],
    rollout_snapshot: dict[str, Any],
    sleep_fn: Callable[[float], None] = time.sleep,
) -> RecoveryDecision:
    state = store.load_or_initialize()
    state["last_issue"] = issue
    append_limited_history(state["issue_history"], build_issue_record(issue))
    repeat_analysis = summarize_repeated_issue(state["issue_history"], issue)
    state["last_issue_repeat_analysis"] = repeat_analysis
    store.save(state)

    prompt = build_recovery_prompt(issue, state, rollout_snapshot)
    invalid_reply_attempt = 0
    while True:
        wake_attempt = state["wake_count"] + 1
        store.record_event("assistant_wake_requested", {"wake_attempt": wake_attempt, "issue": issue})
        try:
            reply = bridge.wake_same_session(prompt)
        except Exception as error:
            wake_record = {
                "wake_attempt": wake_attempt,
                "status": "failed",
                "message": truncate_text(str(error)),
            }
            state["last_wake"] = wake_record
            append_limited_history(state["wake_history"], wake_record)
            store.save(state)
            store.record_event("assistant_wake_failed", wake_record)
            sleep_fn(WAKE_RETRY_DELAY_SECONDS)
            continue

        wake_record = {
            "wake_attempt": wake_attempt,
            "status": "succeeded",
            "reply_excerpt": truncate_text(reply),
        }
        state["wake_count"] = wake_attempt
        state["last_wake"] = wake_record
        append_limited_history(state["wake_history"], wake_record)
        store.save(state)
        store.record_event("assistant_wake_succeeded", wake_record)
        try:
            decision = RecoveryDecision.from_text(reply)
        except Exception as error:
            invalid_reply_attempt += 1
            store.record_event(
                "assistant_reply_invalid",
                {
                    "wake_attempt": wake_attempt,
                    "invalid_reply_attempt": invalid_reply_attempt,
                    "validation_error": str(error),
                    "reply_excerpt": truncate_text(reply),
                },
            )
            prompt = build_invalid_reply_prompt(reply, error)
            sleep_fn(WAKE_RETRY_DELAY_SECONDS)
            continue

        original_decision = decision
        if should_force_loop_guard_wait(issue, repeat_analysis, decision):
            decision = forced_loop_guard_decision(decision, repeat_analysis)
            store.record_event(
                "assistant_decision_loop_guard_override",
                {
                    "original_decision": original_decision.to_dict(),
                    "overridden_decision": decision.to_dict(),
                    "repeat_analysis": repeat_analysis,
                },
            )

        state["last_decision"] = decision.to_dict()
        state["last_issue_repeat_analysis"] = repeat_analysis
        store.save(state)
        store.record_event("assistant_decision", decision.to_dict())
        return decision
