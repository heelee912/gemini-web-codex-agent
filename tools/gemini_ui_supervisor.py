from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from build_state import ROOT_DIR, rebuild_state
from gemini_resume_hint import extract_resume_hint
from next_batch import StateItem, choose_next_batch, load_state, state_items
from runtime_config import (
    discover_episode_numbers,
    discover_segment_numbers,
    env_path,
    project_label,
    prompt_path,
    stop_request_path,
    worker_log_dir,
    worker_window_title,
)
from same_session_recovery import CodexResumeBridge, RecoveryStateStore, request_recovery_decision
from teogonia_rollout import (
    ACCEPTANCE_EVIDENCE_VERSION,
    SegmentAcceptanceDecision,
    evaluate_segment_group,
    merge_episode_final,
)


STATE_PATH = ROOT_DIR / ".codex" / "state.json"
PROGRESS_PATH = ROOT_DIR / ".codex" / "PROGRESS.md"
PLAN_PATH = ROOT_DIR / "PLAN.md" if (ROOT_DIR / "PLAN.md").exists() else ROOT_DIR / "plan.md"
RESULT_PATH = ROOT_DIR / ".codex" / "rollout_result.json"
RECOVERY_STATE_PATH = ROOT_DIR / ".codex" / "same_session_supervisor_state.json"
RECOVERY_EVENT_LOG_PATH = ROOT_DIR / ".codex" / "same_session_supervisor_events.jsonl"
SCREENSHOT_DIR = ROOT_DIR / ".codex" / "screenshots"
WORKER_LOG_DIR = worker_log_dir()
BATCH_SCRIPT = ROOT_DIR / "tools" / "gemini_ui_batch_shell.py"
STRICT_PROMPT_PATH = prompt_path(ROOT_DIR, strict=True)
SESSION_ID = os.environ.get("CODEX_THREAD_ID")
DEFAULT_CODEX_COMMAND = os.environ.get("CODEX_COMMAND", "codex")
WINDOWS_PYTHON = env_path("GEMINI_WINDOWS_PYTHON")
BATCH_TIMEOUT_BASE_SECONDS = int(os.environ.get("GEMINI_BATCH_TIMEOUT_BASE_SECONDS", "180"))
BATCH_TIMEOUT_PER_SEGMENT_SECONDS = int(os.environ.get("GEMINI_BATCH_TIMEOUT_PER_SEGMENT_SECONDS", "240"))
WORKER_WINDOW_TITLE = worker_window_title()
STOP_REQUEST_PATH = stop_request_path(ROOT_DIR)
HARD_BLOCKED_QUALITY_STATE = "hard_blocked"
MAX_RECOVERY_HISTORY = 24
MAX_DISTINCT_FAILED_RECOVERY_PATHS = 3
DEFAULT_INTERRUPTED_RECOVERY_DELAY_SECONDS = int(
    os.environ.get("GEMINI_INTERRUPTED_RECOVERY_DELAY_SECONDS", "60")
)
DEFAULT_PRO_LIMIT_WAIT_SECONDS = int(os.environ.get("GEMINI_PRO_LIMIT_WAIT_SECONDS", "300"))
COUNTABLE_HARD_BLOCK_PATHS = {
    "strict_prompt_batch_retry",
    "single_item_recovery",
    "same_session_retry_now",
    "same_session_wait",
    "resume_hint_wait",
    "supervisor_required",
    "ui_fallback",
}

PRO_LIMIT_PATTERNS = (
    "pro_limit_reached",
    "usage limit",
    "rate limit",
    "quota",
    "한도",
    "제한",
    "나중에 다시",
    "잠시 후 다시",
)
SUPERVISOR_REQUIRED_PATTERNS = (
    "supervisor_required:",
    "file dialog filename field mismatch",
    "file dialog filename field was not found",
    "file open dialog did not close",
)
PRO_MODE_PATTERNS = (
    "pro_mode_required",
    "fast mode",
    "flash mode",
    "빠른",
    "flash",
)
WORKER_INTERRUPTION_PATTERNS = (
    "interrupted",
    "terminated",
    "window was closed",
    "worker window closed",
    "operation canceled by user",
    "operation cancelled by user",
)
WORKER_INTERRUPTED_RETURNCODES = {
    -1073741510,
    3221225786,
    130,
}
SUPERVISOR_REQUIRED_RETURNCODES = {86}
IMMEDIATE_SAME_SESSION_RECOVERY_CATEGORIES = {
    "automation_stalled",
    "worker_interrupted",
    "pro_limit",
    "pro_mode_required",
    "supervisor_required",
}


class AutomationRuntimeUnavailable(RuntimeError):
    def __init__(self, message: str, attempts: list[str]) -> None:
        super().__init__(message)
        self.attempts = attempts


class BatchCommandError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        command: list[str],
        returncode: int,
        stdout: str,
        stderr: str,
        category: str,
        resume_hint: dict[str, Any] | None,
    ) -> None:
        super().__init__(message)
        self.command = command
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.category = category
        self.resume_hint = resume_hint


@dataclass(frozen=True)
class WorkerRuntime:
    label: str
    command_prefix: tuple[str, ...]
    uses_windows_paths: bool
    opens_visible_window: bool = False

    def build_python_command(self, *args: str | Path) -> list[str]:
        command = list(self.command_prefix)
        for arg in args:
            if isinstance(arg, Path):
                command.append(to_windows_path(arg) if self.uses_windows_paths else str(arg))
            else:
                command.append(str(arg))
        return command

    def build_command(self, *args: str | Path) -> list[str]:
        command = self.build_python_command(*args)
        if not self.opens_visible_window:
            return command
        payload = command[2:] if command[:2] == ["cmd.exe", "/c"] else command
        return build_visible_worker_launch_command(payload)


@dataclass(frozen=True)
class RecoveryPlan:
    path: str
    detail: str
    cause: str
    supervisor_phase: str
    action_kind: str = "retry_now"
    wait_seconds: int = 0
    resume_at_iso: str | None = None
    browser_probe: dict[str, Any] | None = None
    wait_scope: str = "batch"

    def validate(self) -> None:
        if self.action_kind not in {"retry_now", "wait"}:
            raise ValueError("RecoveryPlan.action_kind must be retry_now or wait")
        if self.wait_scope not in {"batch", "global"}:
            raise ValueError("RecoveryPlan.wait_scope must be batch or global")
        if self.action_kind == "wait" and self.wait_seconds <= 0 and not self.resume_at_iso:
            raise ValueError("wait RecoveryPlan requires wait_seconds > 0 or resume_at_iso")


def now_stamp() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z %Z")


def current_time() -> datetime:
    return datetime.now().astimezone()


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=current_time().tzinfo)
    return parsed


def schedule_retry_not_before(wait_seconds: int, resume_at_iso: str | None = None) -> str:
    parsed_resume = parse_iso_datetime(resume_at_iso)
    if parsed_resume is not None and parsed_resume > current_time():
        return parsed_resume.isoformat()
    return (current_time() + timedelta(seconds=max(wait_seconds, 1))).isoformat()


def seconds_until(iso_datetime: str | None) -> int:
    target = parse_iso_datetime(iso_datetime)
    if target is None:
        return 0
    return max(int((target - current_time()).total_seconds()), 0)


def powershell_single_quote(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def encode_powershell_command(script: str) -> str:
    return base64.b64encode(script.encode("utf-16le")).decode("ascii")


def build_visible_worker_launch_payload(payload: list[str]) -> str:
    return subprocess.list2cmdline(build_visible_worker_launch_command(payload))


def build_visible_worker_launch_command(payload: list[str]) -> list[str]:
    if not payload:
        raise ValueError("visible worker launch payload must not be empty")
    executable, *arguments = payload
    argument_array = ", ".join(powershell_single_quote(arg) for arg in arguments)
    log_dir = to_windows_path(WORKER_LOG_DIR)
    child_script = (
        f"New-Item -ItemType Directory -Force -Path {powershell_single_quote(log_dir)} | Out-Null; "
        f"$logPath = Join-Path {powershell_single_quote(log_dir)} ((Get-Date -Format 'yyyyMMdd_HHmmss') + '_worker.log'); "
        f"$Host.UI.RawUI.WindowTitle = {powershell_single_quote(WORKER_WINDOW_TITLE)}; "
        "Write-Host ('[worker-log] ' + $logPath); "
        f"& {powershell_single_quote(executable)} @({argument_array}) *>&1 | Tee-Object -FilePath $logPath; "
        "exit $LASTEXITCODE"
    )
    child_encoded = encode_powershell_command(child_script)
    parent_script = (
        "$proc = Start-Process "
        f"-FilePath {powershell_single_quote('powershell.exe')} "
        "-ArgumentList @("
        f"{powershell_single_quote('-NoProfile')}, "
        f"{powershell_single_quote('-ExecutionPolicy')}, "
        f"{powershell_single_quote('Bypass')}, "
        f"{powershell_single_quote('-EncodedCommand')}, "
        f"{powershell_single_quote(child_encoded)}"
        ") "
        "-PassThru -Wait; "
        "exit $proc.ExitCode"
    )
    parent_encoded = encode_powershell_command(parent_script)
    return [
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-EncodedCommand",
        parent_encoded,
    ]


def read_text_with_fallback(path: Path, encodings: tuple[str, ...] = ("utf-8", "cp949")) -> str:
    last_error: Exception | None = None
    for encoding in encodings:
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    return path.read_text(encoding="utf-8")


def ensure_parent_directory(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent_directory(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def log_progress(message: str) -> None:
    existing = read_text_with_fallback(PROGRESS_PATH) if PROGRESS_PATH.exists() else "# Progress\n"
    if not existing.endswith("\n"):
        existing += "\n"
    existing += f"- {now_stamp()}: {message}\n"
    ensure_parent_directory(PROGRESS_PATH)
    PROGRESS_PATH.write_text(existing, encoding="utf-8")
    print(f"[progress] {message}", flush=True)


def log_plan(message: str) -> None:
    if not PLAN_PATH.exists():
        return
    existing = PLAN_PATH.read_text(encoding="utf-8")
    entry = f"- {now_stamp()}: {message}"
    if entry in existing:
        return
    if not existing.endswith("\n"):
        existing += "\n"
    existing += entry + "\n"
    PLAN_PATH.write_text(existing, encoding="utf-8")


def to_windows_path(path: Path) -> str:
    raw = str(path)
    try:
        completed = subprocess.run(
            ["wslpath", "-w", raw],
            capture_output=True,
            text=True,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        if raw.startswith(("/mnt/", "\\mnt\\")):
            return raw.replace("\\", "/")
        return raw
    return completed.stdout.strip()


def screenshot_file_name(tag: str) -> str:
    safe_tag = re.sub(r"[^A-Za-z0-9._-]+", "_", tag).strip("._") or "screen"
    timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    return f"{timestamp}_{safe_tag}.png"


def capture_desktop_screenshot(tag: str) -> Path | None:
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    screenshot_path = SCREENSHOT_DIR / screenshot_file_name(tag)
    windows_path = to_windows_path(screenshot_path)
    script = f"""
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
$bounds = [System.Windows.Forms.Screen]::PrimaryScreen.Bounds
$bitmap = New-Object System.Drawing.Bitmap $bounds.Width, $bounds.Height
$graphics = [System.Drawing.Graphics]::FromImage($bitmap)
$graphics.CopyFromScreen($bounds.Location, [System.Drawing.Point]::Empty, $bounds.Size)
$bitmap.Save('{windows_path}', [System.Drawing.Imaging.ImageFormat]::Png)
$graphics.Dispose()
$bitmap.Dispose()
"""
    try:
        completed = subprocess.run(
            ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script],
            capture_output=True,
            text=True,
            errors="replace",
            check=False,
        )
    except (FileNotFoundError, OSError) as exc:
        print(f"[screenshot] skipped: {exc}", flush=True)
        return None
    if completed.returncode == 0 and screenshot_path.exists():
        print(f"[screenshot] {screenshot_path}", flush=True)
        return screenshot_path
    return None


def truncate_text(text: str, limit: int = 600) -> str:
    if len(text) <= limit:
        return text
    return f"{text[:limit]}...(truncated)"


def coerce_process_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def explicit_stop_requested(
    stop_request_path: Path = STOP_REQUEST_PATH,
    env_value: str | None = None,
) -> bool:
    raw_value = os.environ.get("GEMINI_SUPERVISOR_STOP_REQUESTED") if env_value is None else env_value
    if raw_value is not None and raw_value.strip().lower() not in {"", "0", "false", "no"}:
        return True
    return stop_request_path.exists()


def worker_process_was_interrupted(
    *,
    returncode: int,
    stdout: str,
    stderr: str,
    runtime: WorkerRuntime | None,
) -> bool:
    combined = f"{stdout}\n{stderr}".lower()
    if any(pattern in combined for pattern in WORKER_INTERRUPTION_PATTERNS):
        return True
    if returncode in WORKER_INTERRUPTED_RETURNCODES:
        return True
    return bool(runtime and runtime.opens_visible_window and returncode != 0 and not stdout.strip() and not stderr.strip())


def requires_same_session_recovery(category: str) -> bool:
    return category in IMMEDIATE_SAME_SESSION_RECOVERY_CATEGORIES


def classify_worker_failure(
    stdout: str,
    stderr: str,
    *,
    returncode: int | None = None,
    runtime: WorkerRuntime | None = None,
) -> str:
    combined = f"{stdout}\n{stderr}".lower()
    if any(pattern in combined for pattern in SUPERVISOR_REQUIRED_PATTERNS):
        return "supervisor_required"
    if returncode in SUPERVISOR_REQUIRED_RETURNCODES:
        return "supervisor_required"
    if any(pattern in combined for pattern in PRO_LIMIT_PATTERNS):
        return "pro_limit"
    if any(pattern in combined for pattern in PRO_MODE_PATTERNS):
        return "pro_mode_required"
    if returncode is not None and worker_process_was_interrupted(
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
        runtime=runtime,
    ):
        return "worker_interrupted"
    return "automation_failed"


def extract_resume_hint_from_output(stdout: str, stderr: str) -> dict[str, Any] | None:
    combined = f"{stdout}\n{stderr}"

    wait_match = re.search(r"wait_seconds=(\d+)", combined)
    resume_match = re.search(r"resume_at=([0-9T:+-]+)", combined)
    visible_match = re.search(r"visible_text=(.+)", combined)
    if wait_match or resume_match or visible_match:
        return {
            "wait_seconds": int(wait_match.group(1)) if wait_match else None,
            "resume_at_iso": resume_match.group(1) if resume_match else None,
            "matched_text": visible_match.group(1).strip() if visible_match else None,
        }

    hint = extract_resume_hint(combined.splitlines())
    return hint.to_dict() if hint else None


def resolve_session_anchor() -> str:
    if SESSION_ID:
        return SESSION_ID
    if RECOVERY_STATE_PATH.exists():
        payload = json.loads(RECOVERY_STATE_PATH.read_text(encoding="utf-8"))
        session_id = payload.get("session_id")
        if session_id:
            return str(session_id)
    raise RuntimeError("CODEX_THREAD_ID is required on the first supervisor run to bind the same-session anchor.")


def preflight_probe(runtime: WorkerRuntime) -> tuple[bool, str]:
    command = runtime.build_python_command(BATCH_SCRIPT, "--help")
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=20,
            check=False,
        )
    except FileNotFoundError as exc:
        return False, f"{runtime.label}: executable not found ({exc})"
    except OSError as exc:
        return False, f"{runtime.label}: OS error ({exc})"
    except subprocess.TimeoutExpired:
        return False, f"{runtime.label}: probe timed out"

    output = " | ".join(part.strip() for part in (completed.stdout, completed.stderr) if part and part.strip())
    output = output or f"exit {completed.returncode}"
    if completed.returncode == 0 and "Run a Gemini UI subtitle batch through shell automation." in completed.stdout:
        return True, f"{runtime.label}: ready"
    return False, f"{runtime.label}: {output}"


def resolve_worker_runtime() -> tuple[WorkerRuntime, list[str]]:
    candidates: list[WorkerRuntime] = []
    env_candidate = os.environ.get("GEMINI_UI_PYTHON")
    if env_candidate:
        env_executable = env_candidate
        direct_env_executable = env_candidate
        if env_candidate.startswith("/mnt/"):
            env_executable = to_windows_path(Path(env_candidate))
        candidates.append(
            WorkerRuntime(
                label="env-python",
                command_prefix=(direct_env_executable,),
                uses_windows_paths=False,
            )
        )
        if env_executable.lower().endswith(".exe") and os.environ.get("GEMINI_UI_VISIBLE_WINDOW", "").lower() not in {
            "",
            "0",
            "false",
            "no",
        }:
            candidates.append(
                WorkerRuntime(
                    label="env-visible-python",
                    command_prefix=("cmd.exe", "/c", env_executable),
                    uses_windows_paths=True,
                    opens_visible_window=True,
                )
            )

    if WINDOWS_PYTHON is not None and WINDOWS_PYTHON.exists():
        candidates.append(
            WorkerRuntime(
                label="windows-cmd-python",
                command_prefix=("cmd.exe", "/c", to_windows_path(WINDOWS_PYTHON)),
                uses_windows_paths=True,
                opens_visible_window=True,
            )
        )
    candidates.append(
        WorkerRuntime(
            label="windows-py-launcher",
            command_prefix=("cmd.exe", "/c", "py", "-3"),
            uses_windows_paths=True,
            opens_visible_window=True,
        )
    )
    candidates.append(
        WorkerRuntime(
            label="windows-python-on-path",
            command_prefix=("cmd.exe", "/c", "python"),
            uses_windows_paths=True,
            opens_visible_window=True,
        )
    )

    candidates.append(
        WorkerRuntime(
            label="current-python",
            command_prefix=(sys.executable,),
            uses_windows_paths=False,
        )
    )

    attempts: list[str] = []
    seen: set[tuple[str, ...]] = set()
    for runtime in candidates:
        if runtime.command_prefix in seen:
            continue
        seen.add(runtime.command_prefix)
        ok, detail = preflight_probe(runtime)
        attempts.append(detail)
        if ok:
            return runtime, attempts
    raise AutomationRuntimeUnavailable("automation runtime unavailable from this Codex session", attempts)


def probe_browser_state(runtime: WorkerRuntime) -> dict[str, Any] | None:
    command = runtime.build_python_command(BATCH_SCRIPT, "--probe-browser-state")
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=60,
            check=False,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return None

    if completed.returncode != 0:
        return None
    payload = completed.stdout.strip()
    if not payload:
        return None
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def browser_probe_requires_wait(probe: dict[str, Any] | None) -> bool:
    return bool(probe and probe.get("status") == "pro_limit")


def browser_probe_has_pro_constraint(probe: dict[str, Any] | None) -> bool:
    return bool(probe and probe.get("status") in {"pro_limit", "pro_mode_required"})


def browser_probe_ready_for_worker(probe: dict[str, Any] | None) -> bool:
    return bool(probe and probe.get("status") in {"ready", "response_ready", "draft_with_attachment"})


def browser_probe_wait_details(
    probe: dict[str, Any] | None,
    resume_hint: dict[str, Any] | None,
    fallback_wait_seconds: int,
) -> tuple[int, str | None]:
    for source in (probe, resume_hint):
        if not isinstance(source, dict):
            continue
        raw_wait = source.get("wait_seconds")
        raw_resume = source.get("resume_at_iso")
        if isinstance(raw_wait, int) and raw_wait > 0:
            return raw_wait, str(raw_resume) if raw_resume else None
    return max(fallback_wait_seconds, 1), None


def recovery_plan_deadline(plan: RecoveryPlan) -> str | None:
    if plan.action_kind != "wait":
        return None
    return schedule_retry_not_before(plan.wait_seconds, plan.resume_at_iso)


def pending_recovery_deadline(pending: dict[str, Any] | None) -> datetime | None:
    if not isinstance(pending, dict):
        return None
    return parse_iso_datetime(
        str(
            pending.get("retry_not_before")
            or pending.get("resume_at_iso")
            or ""
        )
    )


def pending_recovery_is_waiting(pending: dict[str, Any] | None, *, now: datetime | None = None) -> bool:
    if not isinstance(pending, dict):
        return False
    if str(pending.get("kind") or "") != "wait":
        return False
    deadline = pending_recovery_deadline(pending)
    if deadline is None:
        return False
    reference_time = now or current_time()
    return deadline > reference_time


def item_waiting_for_recovery(item: dict[str, Any], *, now: datetime | None = None) -> bool:
    if item.get("quality_state") == HARD_BLOCKED_QUALITY_STATE:
        return False
    return pending_recovery_is_waiting(item.get("pending_recovery_action"), now=now)


def actionable_state_items(data: dict[str, Any]) -> list[StateItem]:
    raw_items = {
        item["id"]: item
        for item in data.get("items", [])
        if isinstance(item, dict) and item.get("id")
    }
    reference_time = current_time()
    return [
        item
        for item in state_items(data)
        if not item_waiting_for_recovery(raw_items.get(item.id, {}), now=reference_time)
    ]


def next_scheduled_batch_recovery(data: dict[str, Any]) -> dict[str, Any] | None:
    reference_time = current_time()
    candidates: list[tuple[datetime, dict[str, Any], dict[str, Any]]] = []
    for item in data.get("items", []):
        if not isinstance(item, dict) or not state_item_needs_generation(item):
            continue
        pending = item.get("pending_recovery_action")
        if not pending_recovery_is_waiting(pending, now=reference_time):
            continue
        deadline = pending_recovery_deadline(pending)
        if deadline is None:
            continue
        candidates.append((deadline, item, pending))
    if not candidates:
        return None
    deadline, item, pending = min(candidates, key=lambda entry: (entry[0], str(entry[1]["id"])))
    batch_ids = sorted(
        candidate["id"]
        for candidate in data.get("items", [])
        if isinstance(candidate, dict)
        and state_item_needs_generation(candidate)
        and candidate.get("pending_recovery_action") == pending
    )
    return {
        "scope": "batch",
        "resume_at_iso": deadline.isoformat(),
        "wait_seconds": max(int((deadline - reference_time).total_seconds()), 1),
        "detail": str(pending.get("detail") or item["id"]),
        "cause": str(pending.get("cause") or ""),
        "batch": batch_ids or [item["id"]],
    }


def rollout_snapshot(
    data: dict[str, Any],
    *,
    runtime: WorkerRuntime | None = None,
    blocker: str | None = None,
    browser_probe: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot = {
        "session_anchor": resolve_session_anchor(),
        "summary": data["summary"],
        "runtime_strategy": runtime.label if runtime else None,
        "pending_generation": [
            item["id"]
            for item in data["items"]
            if (not item["generated"]) or item.get("quality_state") == "needs_regeneration"
        ][:12],
        "ready_for_acceptance": [group["id"] for group in data.get("segment_groups", []) if group.get("ready_for_acceptance")][:12],
        "needs_regeneration": [group["id"] for group in data.get("segment_groups", []) if group.get("needs_regeneration")][:12],
        "hard_blocked": [item["id"] for item in data["items"] if item.get("quality_state") == HARD_BLOCKED_QUALITY_STATE][:12],
        "episode_finals_complete": data["summary"].get("episode_finals_complete", 0),
        "episode_finals_total": data["summary"].get("episode_finals_total", 0),
        "blocker": blocker,
    }
    if browser_probe is not None:
        snapshot["browser_probe"] = browser_probe
    return snapshot


def write_rollout_result(
    status: str,
    *,
    runtime: WorkerRuntime | None = None,
    blocker: str | None = None,
    recovery_attempts: list[str] | None = None,
    wait_seconds: int | None = None,
    resume_at_iso: str | None = None,
    browser_probe: dict[str, Any] | None = None,
) -> None:
    data = load_state(STATE_PATH)
    result = {
        "status": status,
        "session_id": resolve_session_anchor(),
        "generated_at": now_stamp(),
        "summary": data["summary"],
        "runtime_strategy": runtime.label if runtime else None,
        "remaining_items": [
            item["id"]
            for item in data["items"]
            if (not item["generated"])
            or item.get("quality_state") == "needs_regeneration"
            or item.get("quality_state") == HARD_BLOCKED_QUALITY_STATE
        ],
        "ready_for_acceptance": [group["id"] for group in data.get("segment_groups", []) if group.get("ready_for_acceptance")],
        "accepted_segments": data["summary"].get("segment_groups_accepted", 0),
        "episode_finals_complete": data["summary"].get("episode_finals_complete", 0),
        "hard_blocked_items": [
            item["id"]
            for item in data["items"]
            if item.get("quality_state") == HARD_BLOCKED_QUALITY_STATE
        ],
    }
    if blocker is not None:
        result["blocker"] = blocker
    if recovery_attempts is not None:
        result["recovery_attempts"] = recovery_attempts
    if wait_seconds is not None:
        result["wait_seconds"] = wait_seconds
    if resume_at_iso is not None:
        result["resume_at_iso"] = resume_at_iso
    if browser_probe is not None:
        result["browser_probe"] = browser_probe
    write_json(RESULT_PATH, result)


def load_state_for_edit() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {
            "items": [],
            "segment_groups": [],
            "episode_outputs": [],
            "summary": {},
        }
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))


def save_state_for_edit(state: dict[str, Any]) -> None:
    write_json(STATE_PATH, state)


def update_item_supervisor_state(
    item: dict[str, Any],
    *,
    phase: str | None = None,
    cause: str | None = None,
    detail: str | None = None,
) -> bool:
    changed = False
    if phase is not None and item.get("supervisor_phase") != phase:
        item["supervisor_phase"] = phase
        changed = True
    if cause is not None and item.get("last_recovery_cause") != cause:
        item["last_recovery_cause"] = cause
        changed = True
    if detail is not None and item.get("last_recovery_detail") != detail:
        item["last_recovery_detail"] = detail
        changed = True
    if changed:
        item["supervisor_phase_updated_at"] = now_stamp()
    return changed


def state_item_map(data: dict[str, Any]) -> dict[tuple[int, int, int], dict[str, Any]]:
    return {
        (item["episode"], item["segment"], item["pass_number"]): item
        for item in data.get("items", [])
    }


def state_item_needs_generation(item: dict[str, Any]) -> bool:
    quality_state = str(item.get("quality_state") or "unchecked")
    if quality_state == HARD_BLOCKED_QUALITY_STATE:
        return False
    return (not bool(item.get("generated"))) or quality_state == "needs_regeneration"


def batch_item_keys(batch: list[StateItem]) -> set[tuple[int, int, int]]:
    return {(item.episode, item.segment, item.pass_number) for item in batch}


def matching_batch_items(state: dict[str, Any], batch: list[StateItem]) -> list[dict[str, Any]]:
    keys = batch_item_keys(batch)
    return [
        item
        for item in state.get("items", [])
        if (item["episode"], item["segment"], item["pass_number"]) in keys
    ]


def append_recovery_attempt(item: dict[str, Any], path: str, detail: str, *, cause: str | None = None) -> None:
    attempts = item.setdefault("recovery_attempts", [])
    attempt: dict[str, Any] = {
        "path": path,
        "detail": detail,
        "status": "failed",
        "at": now_stamp(),
    }
    if cause:
        attempt["cause"] = cause
    attempts.append(attempt)
    overflow = len(attempts) - MAX_RECOVERY_HISTORY
    if overflow > 0:
        del attempts[:overflow]


def active_supervisor_wait(data: dict[str, Any]) -> dict[str, Any] | None:
    wait = data.get("supervisor_wait")
    if not isinstance(wait, dict):
        return None
    if str(wait.get("kind") or "") != "wait":
        return None
    deadline = pending_recovery_deadline(wait)
    if deadline is None or deadline <= current_time():
        return None
    return wait


def set_supervisor_wait(state: dict[str, Any], plan: RecoveryPlan, batch: list[StateItem]) -> bool:
    if plan.action_kind != "wait" or plan.wait_scope != "global":
        return False
    payload: dict[str, Any] = {
        "kind": "wait",
        "path": plan.path,
        "cause": plan.cause,
        "detail": plan.detail,
        "queued_at": now_stamp(),
        "wait_seconds": plan.wait_seconds,
        "retry_not_before": recovery_plan_deadline(plan),
        "resume_at_iso": plan.resume_at_iso,
        "batch": [item.id for item in batch],
    }
    if plan.browser_probe is not None:
        payload["browser_probe"] = plan.browser_probe
    if state.get("supervisor_wait") == payload:
        return False
    state["supervisor_wait"] = payload
    state["supervisor_wait_updated_at"] = now_stamp()
    return True


def clear_supervisor_wait(state: dict[str, Any]) -> bool:
    changed = False
    if "supervisor_wait" in state:
        state.pop("supervisor_wait", None)
        changed = True
    if changed:
        state["supervisor_wait_updated_at"] = now_stamp()
    return changed


def clear_due_supervisor_wait() -> bool:
    state = load_state_for_edit()
    wait = state.get("supervisor_wait")
    deadline = pending_recovery_deadline(wait if isinstance(wait, dict) else None)
    if not isinstance(wait, dict) or deadline is None or deadline > current_time():
        return False
    if clear_supervisor_wait(state):
        save_state_for_edit(state)
        rebuild_state(STATE_PATH)
        return True
    return False


def next_scheduled_recovery(data: dict[str, Any]) -> dict[str, Any] | None:
    global_wait = active_supervisor_wait(data)
    if global_wait is not None:
        resume_at_iso = str(
            global_wait.get("retry_not_before")
            or global_wait.get("resume_at_iso")
            or ""
        )
        return {
            "scope": "global",
            "resume_at_iso": resume_at_iso,
            "wait_seconds": max(seconds_until(resume_at_iso), 1),
            "detail": str(global_wait.get("detail") or "Gemini Pro recovery wait"),
            "cause": str(global_wait.get("cause") or ""),
            "batch": list(global_wait.get("batch") or []),
        }
    return next_scheduled_batch_recovery(data)


def finalize_pending_recovery_actions(state: dict[str, Any], batch: list[StateItem]) -> bool:
    changed = False
    for item in matching_batch_items(state, batch):
        pending = item.get("pending_recovery_action")
        if not isinstance(pending, dict):
            continue
        if pending_recovery_is_waiting(pending):
            continue
        item.pop("pending_recovery_action", None)
        append_recovery_attempt(
            item,
            str(pending.get("path") or "same_session_retry_now"),
            str(
                pending.get("detail")
                or "Same-session recovery returned control, but the item re-entered recovery unresolved."
            ),
            cause=str(pending.get("cause") or "") or None,
        )
        update_item_supervisor_state(
            item,
            phase="recovering",
            cause=str(pending.get("cause") or ""),
            detail=str(pending.get("detail") or ""),
        )
        changed = True
    return changed


def clear_pending_recovery_actions(state: dict[str, Any], batch: list[StateItem]) -> bool:
    changed = False
    for item in matching_batch_items(state, batch):
        if "pending_recovery_action" not in item:
            continue
        if state_item_needs_generation(item):
            continue
        item.pop("pending_recovery_action", None)
        if item.get("accepted"):
            update_item_supervisor_state(item, phase="accepted")
        elif item.get("generated"):
            update_item_supervisor_state(item, phase="generated")
        changed = True
    return changed


def queue_pending_recovery_action(state: dict[str, Any], batch: list[StateItem], plan: RecoveryPlan) -> bool:
    plan.validate()
    changed = False
    retry_not_before = recovery_plan_deadline(plan)
    for item in matching_batch_items(state, batch):
        if not state_item_needs_generation(item):
            continue
        pending: dict[str, Any] = {
            "path": plan.path,
            "detail": plan.detail,
            "cause": plan.cause,
            "queued_at": now_stamp(),
            "kind": plan.action_kind,
        }
        if plan.action_kind == "wait":
            pending["wait_seconds"] = plan.wait_seconds
            pending["resume_at_iso"] = plan.resume_at_iso
            pending["retry_not_before"] = retry_not_before
        if plan.browser_probe is not None:
            pending["browser_probe"] = plan.browser_probe
        item["pending_recovery_action"] = pending
        update_item_supervisor_state(
            item,
            phase=plan.supervisor_phase,
            cause=plan.cause,
            detail=plan.detail,
        )
        changed = True
    return changed


def distinct_failed_hard_block_paths(item: dict[str, Any]) -> list[str]:
    seen: set[str] = set()
    paths: list[str] = []
    for attempt in item.get("recovery_attempts", []):
        if attempt.get("status") != "failed":
            continue
        path = str(attempt.get("path") or "")
        if path not in COUNTABLE_HARD_BLOCK_PATHS or path in seen:
            continue
        seen.add(path)
        paths.append(path)
    return paths


def mark_hard_blocked_items(
    state: dict[str, Any],
    batch: list[StateItem],
    blocker: str,
) -> list[dict[str, Any]]:
    blocked: list[dict[str, Any]] = []
    for item in matching_batch_items(state, batch):
        if not state_item_needs_generation(item):
            continue
        failed_paths = distinct_failed_hard_block_paths(item)
        if len(failed_paths) < MAX_DISTINCT_FAILED_RECOVERY_PATHS:
            continue
        item["quality_state"] = HARD_BLOCKED_QUALITY_STATE
        item["hard_blocked"] = True
        item["hard_block_reason"] = blocker
        item["hard_blocked_at"] = now_stamp()
        item["last_error"] = blocker
        item.pop("pending_recovery_action", None)
        update_item_supervisor_state(item, phase="hard_blocked", detail=blocker)
        blocked.append(
            {
                "id": item["id"],
                "failed_paths": failed_paths,
            }
        )
    return blocked


def finalize_pending_same_session_failures(batch: list[StateItem], blocker: str) -> list[dict[str, Any]]:
    state = load_state_for_edit()
    changed = finalize_pending_recovery_actions(state, batch)
    blocked = mark_hard_blocked_items(state, batch, blocker)
    if changed or blocked:
        save_state_for_edit(state)
        rebuild_state(STATE_PATH)
    return blocked


def record_failed_recovery_path(batch: list[StateItem], path: str, detail: str) -> list[dict[str, Any]]:
    state = load_state_for_edit()
    changed = finalize_pending_recovery_actions(state, batch)
    for item in matching_batch_items(state, batch):
        append_recovery_attempt(item, path, detail)
        update_item_supervisor_state(item, phase="recovering", detail=detail)
        changed = True
    blocked = mark_hard_blocked_items(state, batch, detail)
    if changed or blocked:
        save_state_for_edit(state)
        rebuild_state(STATE_PATH)
    return blocked


def clear_pending_recovery_path(batch: list[StateItem]) -> None:
    state = load_state_for_edit()
    changed = clear_pending_recovery_actions(state, batch)
    changed = clear_supervisor_wait(state) or changed
    if changed:
        save_state_for_edit(state)
        rebuild_state(STATE_PATH)


def queue_pending_same_session_recovery(batch: list[StateItem], plan: RecoveryPlan) -> None:
    state = load_state_for_edit()
    changed = queue_pending_recovery_action(state, batch, plan)
    changed = set_supervisor_wait(state, plan, batch) or changed
    if plan.action_kind != "wait" or plan.wait_scope != "global":
        changed = clear_supervisor_wait(state) or changed
    if changed:
        save_state_for_edit(state)
        rebuild_state(STATE_PATH)


def hard_blocked_recovery_attempts(blocked_items: list[dict[str, Any]]) -> list[str]:
    return [f"{entry['id']}: {', '.join(entry['failed_paths'])}" for entry in blocked_items]


def emit_hard_blocked(runtime: WorkerRuntime, blocker: str, blocked_items: list[dict[str, Any]]) -> int:
    item_ids = ", ".join(entry["id"] for entry in blocked_items)
    attempts = hard_blocked_recovery_attempts(blocked_items)
    log_progress(f"HARD_BLOCKED: {item_ids}. {blocker}")
    log_plan(f"HARD_BLOCKED: {item_ids}. {blocker}")
    write_rollout_result("HARD_BLOCKED", runtime=runtime, blocker=blocker, recovery_attempts=attempts)
    print("[supervisor] hard_blocked", flush=True)
    return 2


def apply_segment_decision(state: dict[str, Any], decision: SegmentAcceptanceDecision, session_anchor: str) -> bool:
    items = [
        item
        for item in state["items"]
        if item["episode"] == decision.episode and item["segment"] == decision.segment
    ]
    changed = False
    whisper_evidence = decision.whisper_evidence()

    for item in items:
        if item.get("accepted"):
            item["accepted"] = False
            changed = True
        if "accepted_session_id" in item:
            item.pop("accepted_session_id", None)
            changed = True
        if "accepted_at" in item:
            item.pop("accepted_at", None)
            changed = True
        if item.get("whisper_evidence") != whisper_evidence:
            item["whisper_evidence"] = whisper_evidence
            changed = True

    if decision.status == "accepted" and decision.selected_pass_number is not None:
        for item in items:
            if item["pass_number"] == decision.selected_pass_number:
                changed = changed or item.get("quality_state") != decision.quality_state
                item["accepted"] = True
                item["accepted_session_id"] = session_anchor
                item["accepted_at"] = now_stamp()
                item["quality_state"] = decision.quality_state
                item["last_error"] = None
                changed = update_item_supervisor_state(item, phase="accepted") or changed
            elif item["generated"]:
                changed = changed or item.get("quality_state") == "needs_regeneration"
                item["quality_state"] = "candidate"
                changed = update_item_supervisor_state(item, phase="generated") or changed
        return changed

    if decision.status == "needs_regeneration" and decision.retry_pass_number is not None:
        for item in items:
            if item["pass_number"] == decision.retry_pass_number:
                was_already_targeted = (
                    item.get("quality_state") == "needs_regeneration"
                    and item.get("last_error") == decision.reason
                )
                item["quality_state"] = "needs_regeneration"
                if not was_already_targeted:
                    item["retry_count"] = int(item.get("retry_count", 0)) + 1
                item["last_error"] = decision.reason
                changed = update_item_supervisor_state(
                    item,
                    phase="generation_pending",
                    detail=decision.reason,
                ) or changed
                changed = changed or (not was_already_targeted)
            elif item["generated"]:
                item["quality_state"] = "candidate"
                changed = update_item_supervisor_state(item, phase="generated") or changed
        return changed

    return changed


def group_items_for_acceptance(state: dict[str, Any], episode: int, segment: int) -> list[dict[str, Any]]:
    return [
        item
        for item in state.get("items", [])
        if item["episode"] == episode and item["segment"] == segment
    ]


def group_has_current_acceptance_evidence(group_items: list[dict[str, Any]]) -> bool:
    accepted_items = [
        item
        for item in group_items
        if item.get("accepted") and item.get("generated")
    ]
    if len(accepted_items) != 1:
        return False
    whisper_evidence = accepted_items[0].get("whisper_evidence")
    if not isinstance(whisper_evidence, dict):
        return False
    return int(whisper_evidence.get("acceptance_version", 0)) == ACCEPTANCE_EVIDENCE_VERSION


def process_segment_acceptance(session_anchor: str) -> bool:
    state = load_state_for_edit()
    changed = False
    for group in sorted(state.get("segment_groups", []), key=lambda item: (item["episode"], item["segment"])):
        if not group.get("has_all_passes"):
            continue
        if group.get("hard_blocked"):
            continue
        group_items = group_items_for_acceptance(state, group["episode"], group["segment"])
        if (
            group.get("accepted")
            and not group.get("needs_regeneration")
            and group_has_current_acceptance_evidence(group_items)
        ):
            continue
        decision = evaluate_segment_group(ROOT_DIR, group["episode"], group["segment"])
        if apply_segment_decision(state, decision, session_anchor=session_anchor):
            changed = True
            if decision.status == "accepted" and decision.selected_pass_number is not None:
                log_progress(
                    f"Accepted {decision.segment_id} using pass {decision.selected_pass_number}. {decision.reason}"
                )
            elif decision.status == "needs_regeneration" and decision.retry_pass_number is not None:
                log_progress(
                    f"Scheduled regeneration for {decision.segment_id} on pass {decision.retry_pass_number}. {decision.reason}"
                )
    if changed:
        save_state_for_edit(state)
        rebuild_state(STATE_PATH)
    return changed


def process_episode_merges() -> bool:
    state = load_state_for_edit()
    changed = False
    for episode in discover_episode_numbers(ROOT_DIR):
        expected_segment_count = len(discover_segment_numbers(ROOT_DIR, episode))
        if expected_segment_count <= 0:
            continue
        accepted_pass_by_segment = {
            item["segment"]: item["pass_number"]
            for item in state["items"]
            if item["episode"] == episode and item.get("accepted")
        }
        if len(accepted_pass_by_segment) != expected_segment_count:
            continue

        output = next((entry for entry in state.get("episode_outputs", []) if entry["episode"] == episode), None)
        if output and output.get("exists") and int(output.get("size", 0)) > 0:
            continue

        final_path = merge_episode_final(ROOT_DIR, episode, accepted_pass_by_segment)
        if (not final_path.exists()) or final_path.stat().st_size <= 0:
            raise RuntimeError(f"E{episode:02d} final merge did not produce a non-empty .final.srt")
        log_progress(f"Merged final episode subtitle for E{episode:02d} -> {final_path.name}.")
        changed = True

    if changed:
        rebuild_state(STATE_PATH)
    return changed


def rollout_is_complete(data: dict[str, Any]) -> bool:
    summary = data["summary"]
    return (
        summary["remaining"] == 0
        and summary.get("segment_groups_accepted", 0) == summary.get("segment_groups_total", 0)
        and summary.get("segment_groups_hard_blocked", 0) == 0
        and summary.get("episode_finals_complete", 0) == summary.get("episode_finals_total", 0)
    )


def unresolved_items(batch: list[StateItem]) -> list[StateItem]:
    latest = {item.id: item for item in state_items(load_state(STATE_PATH))}
    remaining: list[StateItem] = []
    for item in batch:
        current = latest.get(item.id)
        if current is None:
            remaining.append(item)
            continue
        if (not current.generated) or current.quality_state == "needs_regeneration":
            remaining.append(current)
    return remaining


def run_batch(
    runtime: WorkerRuntime,
    batch: list[StateItem],
    *,
    prompt_path: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    if not batch:
        raise ValueError("batch is empty")
    episode = batch[0].episode
    pass_number = batch[0].pass_number
    segments = [str(item.segment) for item in batch]
    command_args: list[str | Path] = [
        BATCH_SCRIPT,
        "--episode",
        str(episode),
        "--pass-number",
        str(pass_number),
        "--segments",
        *segments,
    ]
    if prompt_path is not None:
        command_args.extend(["--prompt-path", prompt_path])
    command = runtime.build_command(*command_args)
    label = batch_label(batch)
    timeout_seconds = BATCH_TIMEOUT_BASE_SECONDS + BATCH_TIMEOUT_PER_SEGMENT_SECONDS * len(batch)
    capture_desktop_screenshot(f"{label}_before_launch")
    print(f"[supervisor] run {' '.join(command)}", flush=True)
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            errors="replace",
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        capture_desktop_screenshot(f"{label}_timeout")
        timeout_stdout = coerce_process_output(exc.stdout)
        timeout_stderr = coerce_process_output(exc.stderr)
        raise BatchCommandError(
            f"worker timed out after {timeout_seconds}s (automation_stalled)",
            command=command,
            returncode=-9,
            stdout=timeout_stdout,
            stderr=timeout_stderr + f"\nworker timed out after {timeout_seconds}s",
            category="automation_stalled",
            resume_hint=None,
        ) from exc
    if completed.stdout.strip():
        print(completed.stdout.rstrip(), flush=True)
    if completed.returncode != 0:
        if completed.stdout.strip():
            print(completed.stdout.rstrip(), flush=True)
        if completed.stderr.strip():
            print(completed.stderr.rstrip(), file=sys.stderr, flush=True)
        category = classify_worker_failure(
            completed.stdout,
            completed.stderr,
            returncode=completed.returncode,
            runtime=runtime,
        )
        capture_desktop_screenshot(f"{label}_{category}")
        resume_hint = extract_resume_hint_from_output(completed.stdout, completed.stderr) if category == "pro_limit" else None
        raise BatchCommandError(
            f"worker exited with {completed.returncode} ({category})",
            command=command,
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            category=category,
            resume_hint=resume_hint,
        )
    return completed


def batch_label(batch: list[StateItem]) -> str:
    return ", ".join(item.id for item in batch)


def recovery_issue(
    category: str,
    blocker: str,
    batch: list[StateItem],
    runtime: WorkerRuntime,
    resume_hint: dict[str, Any] | None = None,
    explicit_stop: bool | None = None,
) -> dict[str, Any]:
    issue = {
        "category": category,
        "blocker": blocker,
        "batch": [item.id for item in batch],
        "runtime_strategy": runtime.label,
    }
    if resume_hint:
        issue["resume_hint"] = resume_hint
    if explicit_stop is not None:
        issue["explicit_stop_requested"] = explicit_stop
    return issue


def apply_same_session_recovery(
    bridge: CodexResumeBridge,
    store: RecoveryStateStore,
    *,
    category: str,
    blocker: str,
    batch: list[StateItem],
    runtime: WorkerRuntime,
    resume_hint: dict[str, Any] | None = None,
    explicit_stop: bool | None = None,
) -> RecoveryPlan:
    state = load_state(STATE_PATH)
    browser_probe = probe_browser_state(runtime) if category in {"supervisor_required", "pro_limit", "pro_mode_required"} else None
    if browser_probe and browser_probe.get("permission_dialog_cleared"):
        log_progress("Supervisor browser probe cleared a blocking browser permission dialog during recovery planning.")
    if category == "supervisor_required" and browser_probe_ready_for_worker(browser_probe):
        log_progress("Supervisor browser probe shows Gemini is ready during supervisor-required recovery. Retrying now.")
        return RecoveryPlan(
            path="supervisor_required",
            detail=blocker,
            cause=category,
            supervisor_phase="recovering",
            action_kind="retry_now",
            browser_probe=browser_probe,
        )
    issue = recovery_issue(
        category,
        blocker,
        batch,
        runtime,
        resume_hint=resume_hint,
        explicit_stop=explicit_stop,
    )
    if browser_probe is not None:
        issue["browser_probe"] = browser_probe
    decision = request_recovery_decision(
        bridge=bridge,
        store=store,
        issue=issue,
        rollout_snapshot=rollout_snapshot(state, runtime=runtime, blocker=blocker, browser_probe=browser_probe),
    )
    action = decision.recovery_action
    log_progress(f"Same-session recovery decision: {decision.diagnosis}. Action={action.kind}.")
    if action.kind == "wait":
        wait_seconds, resume_at_iso = browser_probe_wait_details(browser_probe, resume_hint, action.delay_seconds)
        if resume_hint and isinstance(resume_hint.get("wait_seconds"), int) and int(resume_hint["wait_seconds"]) > 0:
            log_progress(
                f"Using parsed Gemini resume hint wait={wait_seconds}s"
                + (f" resume_at={resume_at_iso}" if resume_at_iso else "")
                + "."
            )
        return RecoveryPlan(
            path="resume_hint_wait" if resume_hint and isinstance(resume_hint.get("wait_seconds"), int) else "same_session_wait",
            detail=action.reason or blocker,
            cause=category,
            supervisor_phase="waiting_quota" if browser_probe_has_pro_constraint(browser_probe) or category in {"pro_limit", "pro_mode_required"} else "waiting_recovery",
            action_kind="wait",
            wait_seconds=wait_seconds,
            resume_at_iso=resume_at_iso,
            browser_probe=browser_probe,
            wait_scope="global" if browser_probe_has_pro_constraint(browser_probe) or category in {"pro_limit", "pro_mode_required"} else "batch",
        )
    return RecoveryPlan(
        path="supervisor_required" if category == "supervisor_required" else "same_session_retry_now",
        detail=action.reason or blocker,
        cause=category,
        supervisor_phase="recovering",
        action_kind="retry_now",
        browser_probe=browser_probe,
    )


def persist_recovery_plan(batch: list[StateItem], plan: RecoveryPlan) -> None:
    queue_pending_same_session_recovery(batch, plan)


def persist_user_pause(batch: list[StateItem], blocker: str) -> None:
    state = load_state_for_edit()
    changed = False
    clear_supervisor_wait(state)
    for item in matching_batch_items(state, batch):
        changed = update_item_supervisor_state(
            item,
            phase="paused_by_user",
            cause="worker_interrupted",
            detail=blocker,
        ) or changed
        item.pop("pending_recovery_action", None)
    if changed:
        save_state_for_edit(state)
        rebuild_state(STATE_PATH)


def schedule_worker_interruption_recovery(batch: list[StateItem], blocker: str) -> RecoveryPlan:
    return RecoveryPlan(
        path="worker_interrupt_cooldown",
        detail=blocker,
        cause="worker_interrupted",
        supervisor_phase="waiting_recovery",
        action_kind="wait",
        wait_seconds=DEFAULT_INTERRUPTED_RECOVERY_DELAY_SECONDS,
        wait_scope="batch",
    )


def maybe_refresh_due_supervisor_wait(
    runtime: WorkerRuntime,
    *,
    default_batch: list[StateItem] | None = None,
) -> bool:
    state = load_state_for_edit()
    wait = state.get("supervisor_wait")
    deadline = pending_recovery_deadline(wait if isinstance(wait, dict) else None)
    if not isinstance(wait, dict) or deadline is None or deadline > current_time():
        return False

    cause = str(wait.get("cause") or "")
    if cause not in {"pro_limit", "pro_mode_required"}:
        if clear_supervisor_wait(state):
            save_state_for_edit(state)
            rebuild_state(STATE_PATH)
        return False

    probe = probe_browser_state(runtime)
    if probe and probe.get("permission_dialog_cleared"):
        log_progress("Supervisor browser probe cleared a blocking browser permission dialog while re-checking a due Pro wait.")
    if browser_probe_has_pro_constraint(probe):
        wait_seconds, resume_at_iso = browser_probe_wait_details(probe, None, DEFAULT_PRO_LIMIT_WAIT_SECONDS)
        plan = RecoveryPlan(
            path="browser_quota_wait",
            detail=str(wait.get("detail") or "Gemini Pro is still unavailable after the scheduled wake."),
            cause=cause,
            supervisor_phase="waiting_quota",
            action_kind="wait",
            wait_seconds=wait_seconds,
            resume_at_iso=resume_at_iso,
            browser_probe=probe,
            wait_scope="global",
        )
        batch_ids = [str(item_id) for item_id in wait.get("batch") or []]
        batch = default_batch or [
            item
            for item in actionable_state_items(load_state(STATE_PATH))
            if item.id in set(batch_ids)
        ]
        persist_recovery_plan(batch, plan)
        log_progress("Scheduled Pro recovery wake reached, but the browser still shows a Pro constraint. Persisting a new wait window.")
        log_plan(
            f"WAIT {plan.wait_seconds}s"
            + (f" until {plan.resume_at_iso}" if plan.resume_at_iso else "")
            + f": {plan.detail}"
        )
        write_rollout_result(
            "WAITING_FOR_RECOVERY",
            runtime=runtime,
            blocker=plan.detail,
            wait_seconds=plan.wait_seconds,
            resume_at_iso=recovery_plan_deadline(plan),
            browser_probe=probe,
        )
        return True

    if clear_supervisor_wait(state):
        save_state_for_edit(state)
        rebuild_state(STATE_PATH)
    if probe is not None:
        log_progress("Scheduled Pro recovery wake reached and the browser probe no longer shows a Pro constraint.")
    return False


def run_single_with_retries(runtime: WorkerRuntime, item: StateItem, max_attempts: int = 3) -> None:
    last_error: BatchCommandError | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            log_progress(f"Recovery single run {item.id} attempt {attempt}/{max_attempts}.")
            run_batch(runtime, [item])
            return
        except BatchCommandError as exc:
            last_error = exc
            log_progress(
                f"Recovery single run failed for {item.id} attempt {attempt}/{max_attempts}: "
                f"exit {exc.returncode} ({exc.category})."
            )
            if requires_same_session_recovery(exc.category):
                raise
    if last_error is None:
        raise RuntimeError(f"{item.id}: recovery loop exited without an error")
    raise last_error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"Run the {project_label()} same-session Gemini UI supervisor.")
    parser.add_argument("--codex-command", default=DEFAULT_CODEX_COMMAND)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    session_anchor = resolve_session_anchor()
    bridge = CodexResumeBridge(session_id=session_anchor, codex_command=args.codex_command, workdir=ROOT_DIR)
    recovery_store = RecoveryStateStore(
        state_path=RECOVERY_STATE_PATH,
        event_log_path=RECOVERY_EVENT_LOG_PATH,
        session_id=session_anchor,
        codex_command=args.codex_command,
    )

    log_progress(
        f"Supervisor bootstrap. The {project_label()} same-session Chrome rollout continues under the current Codex anchor."
    )
    data = rebuild_state(STATE_PATH)

    while True:
        try:
            runtime, _attempts = resolve_worker_runtime()
            log_progress(f"Automation runtime ready via {runtime.label}.")
            break
        except AutomationRuntimeUnavailable as exc:
            blocker = str(exc)
            log_progress(f"Runtime probe failed: {blocker}")
            write_rollout_result("WAITING_FOR_RECOVERY", blocker=blocker, recovery_attempts=exc.attempts)
            apply_same_session_recovery(
                bridge,
                recovery_store,
                category="runtime_unavailable",
                blocker=blocker,
                batch=[],
                runtime=WorkerRuntime("unavailable", tuple(), False),
                resume_hint=None,
            )

    while True:
        rebuild_state(STATE_PATH)
        acceptance_changed = process_segment_acceptance(session_anchor)
        merge_changed = process_episode_merges()
        data = rebuild_state(STATE_PATH)
        if maybe_refresh_due_supervisor_wait(runtime):
            continue
        data = load_state(STATE_PATH)

        hard_blocked_items = [
            {
                "id": item["id"],
                "failed_paths": distinct_failed_hard_block_paths(item),
            }
            for item in data["items"]
            if item.get("quality_state") == HARD_BLOCKED_QUALITY_STATE
        ]
        if hard_blocked_items:
            return emit_hard_blocked(
                runtime,
                "One or more work items exhausted three distinct recovery paths.",
                hard_blocked_items,
            )

        if rollout_is_complete(data):
            log_progress("DONE: generation, acceptance, and final episode merges are all complete.")
            log_plan("DONE: generation, acceptance, and final episode merges are all complete.")
            write_rollout_result("DONE", runtime=runtime)
            print("[supervisor] done", flush=True)
            return 0

        batch = choose_next_batch(actionable_state_items(data))
        if not batch:
            scheduled_recovery = next_scheduled_recovery(data)
            if scheduled_recovery is not None:
                wait_seconds = int(scheduled_recovery["wait_seconds"])
                resume_at_iso = str(scheduled_recovery["resume_at_iso"])
                blocker = str(scheduled_recovery["detail"])
                log_progress(
                    f"No runnable generation batch is currently available. "
                    f"Sleeping {wait_seconds}s until the persisted recovery wake at {resume_at_iso}."
                )
                log_plan(f"WAIT {wait_seconds}s until {resume_at_iso}: {blocker}")
                write_rollout_result(
                    "WAITING_FOR_RECOVERY",
                    runtime=runtime,
                    blocker=blocker,
                    wait_seconds=wait_seconds,
                    resume_at_iso=resume_at_iso,
                )
                time.sleep(wait_seconds)
                continue
            blocker = "No generation batch is pending, but acceptance or final merge work is still incomplete."
            if acceptance_changed or merge_changed:
                continue
            log_progress(f"Same-session recovery required: {blocker}")
            write_rollout_result("WAITING_FOR_RECOVERY", runtime=runtime, blocker=blocker)
            apply_same_session_recovery(
                bridge,
                recovery_store,
                category="acceptance_stalled",
                blocker=blocker,
                batch=[],
                runtime=runtime,
                resume_hint=None,
            )
            continue

        label = batch_label(batch)
        log_progress(f"Starting supervisor batch {label}.")
        try:
            run_batch(runtime, batch)
            data = rebuild_state(STATE_PATH)
            clear_pending_recovery_path(batch)
            log_progress(
                f"Completed supervisor batch {label}. State is {data['summary']['generated']}/{data['summary']['total']} generated."
            )
            continue
        except BatchCommandError as exc:
            rebuild_state(STATE_PATH)
            remaining_after_rebuild = unresolved_items(batch)
            if not remaining_after_rebuild:
                clear_pending_recovery_path(batch)
                data = load_state(STATE_PATH)
                log_progress(
                    f"Completed supervisor batch {label} via disk reconciliation after {exc.category}. "
                    f"State is {data['summary']['generated']}/{data['summary']['total']} generated."
                )
                continue
            blocker = f"{label}: {exc.category}"
            explicit_stop = None
            if exc.category == "worker_interrupted":
                stop_requested = explicit_stop_requested()
                explicit_stop = stop_requested
                if stop_requested:
                    blocker = f"{blocker} (explicit stop request present)"
                    log_progress(f"Detected worker interruption for {label} with an explicit stop request. Pausing the rollout.")
                    persist_user_pause(batch, blocker)
                    write_rollout_result("PAUSED_BY_USER", runtime=runtime, blocker=blocker)
                    return 0
                else:
                    log_progress(
                        f"Detected worker interruption for {label}. "
                        f"Scheduling a {DEFAULT_INTERRUPTED_RECOVERY_DELAY_SECONDS}s cooldown before retrying the worker."
                    )
                    blocked = finalize_pending_same_session_failures(batch, blocker)
                    if blocked:
                        return emit_hard_blocked(runtime, blocker, blocked)
                    recovery_plan = schedule_worker_interruption_recovery(batch, blocker)
                    persist_recovery_plan(batch, recovery_plan)
                    log_plan(
                        f"WAIT {recovery_plan.wait_seconds}s"
                        + f" until {recovery_plan_deadline(recovery_plan)}: {blocker}"
                    )
                    write_rollout_result(
                        "WAITING_FOR_RECOVERY",
                        runtime=runtime,
                        blocker=blocker,
                        wait_seconds=recovery_plan.wait_seconds,
                        resume_at_iso=recovery_plan_deadline(recovery_plan),
                    )
                    continue
            blocked = finalize_pending_same_session_failures(batch, blocker)
            if blocked:
                return emit_hard_blocked(runtime, blocker, blocked)

            if requires_same_session_recovery(exc.category):
                log_progress(f"Same-session recovery required for {label}: {exc.category}.")
                write_rollout_result("WAITING_FOR_RECOVERY", runtime=runtime, blocker=blocker)
                recovery_plan = apply_same_session_recovery(
                    bridge,
                    recovery_store,
                    category=exc.category,
                    blocker=blocker,
                    batch=batch,
                    runtime=runtime,
                    resume_hint=exc.resume_hint,
                    explicit_stop=explicit_stop,
                )
                persist_recovery_plan(batch, recovery_plan)
                if recovery_plan.action_kind == "wait":
                    resume_at_iso = recovery_plan_deadline(recovery_plan)
                    log_plan(
                        f"WAIT {recovery_plan.wait_seconds}s"
                        + (f" until {resume_at_iso}" if resume_at_iso else "")
                        + f": {recovery_plan.detail}"
                    )
                    write_rollout_result(
                        "WAITING_FOR_RECOVERY",
                        runtime=runtime,
                        blocker=recovery_plan.detail,
                        wait_seconds=recovery_plan.wait_seconds,
                        resume_at_iso=resume_at_iso,
                        browser_probe=recovery_plan.browser_probe,
                    )
                continue

            log_progress(
                f"Batch failed for {label}: exit {exc.returncode}. "
                "Retrying the same bounded batch once with a stricter exact-SRT-only prompt."
            )
            try:
                run_batch(runtime, batch, prompt_path=STRICT_PROMPT_PATH)
                data = rebuild_state(STATE_PATH)
                clear_pending_recovery_path(batch)
                log_progress(
                    f"Completed strict prompt retry for {label}. "
                    f"State is {data['summary']['generated']}/{data['summary']['total']} generated."
                )
                continue
            except BatchCommandError as strict_exc:
                strict_blocker = (
                    f"{label}: strict prompt retry failed with exit "
                    f"{strict_exc.returncode} ({strict_exc.category})"
                )
                blocked = record_failed_recovery_path(batch, "strict_prompt_batch_retry", strict_blocker)
                if blocked:
                    return emit_hard_blocked(runtime, strict_blocker, blocked)

                if requires_same_session_recovery(strict_exc.category):
                    log_progress(
                        f"Same-session recovery required for {label} after strict prompt retry: {strict_exc.category}."
                    )
                    write_rollout_result("WAITING_FOR_RECOVERY", runtime=runtime, blocker=strict_blocker)
                    recovery_plan = apply_same_session_recovery(
                        bridge,
                        recovery_store,
                        category=strict_exc.category,
                        blocker=strict_blocker,
                        batch=batch,
                        runtime=runtime,
                        resume_hint=strict_exc.resume_hint,
                    )
                    persist_recovery_plan(batch, recovery_plan)
                    if recovery_plan.action_kind == "wait":
                        resume_at_iso = recovery_plan_deadline(recovery_plan)
                        log_plan(
                            f"WAIT {recovery_plan.wait_seconds}s"
                            + (f" until {resume_at_iso}" if resume_at_iso else "")
                            + f": {recovery_plan.detail}"
                        )
                        write_rollout_result(
                            "WAITING_FOR_RECOVERY",
                            runtime=runtime,
                            blocker=recovery_plan.detail,
                            wait_seconds=recovery_plan.wait_seconds,
                            resume_at_iso=resume_at_iso,
                            browser_probe=recovery_plan.browser_probe,
                        )
                    continue

                rebuild_state(STATE_PATH)
                retry_batch = unresolved_items(batch)
                if not retry_batch:
                    data = load_state(STATE_PATH)
                    log_progress(
                        f"Strict prompt retry failed for {label}: exit {strict_exc.returncode}, but unresolved items were already completed. "
                        f"State is {data['summary']['generated']}/{data['summary']['total']} generated."
                    )
                    continue

                retry_label = batch_label(retry_batch)
                log_progress(
                    f"Strict prompt retry failed for {label}: exit {strict_exc.returncode}. "
                    f"Retrying unresolved items {retry_label} once before split recovery."
                )
                try:
                    run_batch(runtime, retry_batch)
                    data = rebuild_state(STATE_PATH)
                    clear_pending_recovery_path(retry_batch)
                    log_progress(
                        f"Completed unresolved retry for {retry_label}. "
                        f"State is {data['summary']['generated']}/{data['summary']['total']} generated."
                    )
                    continue
                except BatchCommandError as retry_exc:
                    retry_blocker = (
                        f"{retry_label}: unresolved retry failed with exit "
                        f"{retry_exc.returncode} ({retry_exc.category})"
                    )
                    blocked = record_failed_recovery_path(retry_batch, "unresolved_retry", retry_blocker)
                    if blocked:
                        return emit_hard_blocked(runtime, retry_blocker, blocked)

                    if requires_same_session_recovery(retry_exc.category):
                        log_progress(f"Same-session recovery required for {retry_label}: {retry_exc.category}.")
                        write_rollout_result("WAITING_FOR_RECOVERY", runtime=runtime, blocker=retry_blocker)
                        recovery_plan = apply_same_session_recovery(
                            bridge,
                            recovery_store,
                            category=retry_exc.category,
                            blocker=retry_blocker,
                            batch=retry_batch,
                            runtime=runtime,
                            resume_hint=retry_exc.resume_hint,
                        )
                        persist_recovery_plan(retry_batch, recovery_plan)
                        if recovery_plan.action_kind == "wait":
                            resume_at_iso = recovery_plan_deadline(recovery_plan)
                            log_plan(
                                f"WAIT {recovery_plan.wait_seconds}s"
                                + (f" until {resume_at_iso}" if resume_at_iso else "")
                                + f": {recovery_plan.detail}"
                            )
                            write_rollout_result(
                                "WAITING_FOR_RECOVERY",
                                runtime=runtime,
                                blocker=recovery_plan.detail,
                                wait_seconds=recovery_plan.wait_seconds,
                                resume_at_iso=resume_at_iso,
                                browser_probe=recovery_plan.browser_probe,
                            )
                        continue

                    rebuild_state(STATE_PATH)
                    split_batch = unresolved_items(retry_batch)
                    if not split_batch:
                        data = load_state(STATE_PATH)
                        log_progress(
                            f"Unresolved retry failed for {retry_label}: exit {retry_exc.returncode}, but every item was already written. "
                            f"State is {data['summary']['generated']}/{data['summary']['total']} generated."
                        )
                        continue

                    log_progress(
                        f"Unresolved retry failed for {retry_label}: exit {retry_exc.returncode}. "
                        "Switching to single-item recovery."
                    )
                    try:
                        for item in split_batch:
                            run_single_with_retries(runtime, item)
                        data = rebuild_state(STATE_PATH)
                        clear_pending_recovery_path(split_batch)
                        log_progress(
                            f"Completed split recovery for {batch_label(split_batch)}. "
                            f"State is {data['summary']['generated']}/{data['summary']['total']} generated."
                        )
                        continue
                    except BatchCommandError as hard_error:
                        hard_blocker = (
                            f"{batch_label(split_batch)}: exhausted automatic recovery "
                            f"after exit {hard_error.returncode} ({hard_error.category})"
                        )
                        blocked = record_failed_recovery_path(
                            split_batch,
                            "single_item_recovery",
                            hard_blocker,
                        )
                        if blocked:
                            return emit_hard_blocked(runtime, hard_blocker, blocked)
                        log_progress(f"Same-session recovery required: {hard_blocker}")
                        log_plan(f"Same-session recovery required: {hard_blocker}")
                        write_rollout_result("WAITING_FOR_RECOVERY", runtime=runtime, blocker=hard_blocker)
                        recovery_plan = apply_same_session_recovery(
                            bridge,
                            recovery_store,
                            category=hard_error.category,
                            blocker=hard_blocker,
                            batch=split_batch,
                            runtime=runtime,
                            resume_hint=hard_error.resume_hint,
                        )
                        persist_recovery_plan(split_batch, recovery_plan)
                        if recovery_plan.action_kind == "wait":
                            resume_at_iso = recovery_plan_deadline(recovery_plan)
                            log_plan(
                                f"WAIT {recovery_plan.wait_seconds}s"
                                + (f" until {resume_at_iso}" if resume_at_iso else "")
                                + f": {recovery_plan.detail}"
                            )
                            write_rollout_result(
                                "WAITING_FOR_RECOVERY",
                                runtime=runtime,
                                blocker=recovery_plan.detail,
                                wait_seconds=recovery_plan.wait_seconds,
                                resume_at_iso=resume_at_iso,
                                browser_probe=recovery_plan.browser_probe,
                            )
                        continue


if __name__ == "__main__":
    sys.exit(main())
