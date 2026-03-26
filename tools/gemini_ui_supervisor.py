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
from datetime import datetime
from pathlib import Path
from typing import Any

from build_state import EPISODES, ROOT_DIR, rebuild_state
from gemini_resume_hint import extract_resume_hint
from next_batch import StateItem, choose_next_batch, load_state, state_items
from same_session_recovery import CodexResumeBridge, RecoveryStateStore, request_recovery_decision
from teogonia_rollout import SegmentAcceptanceDecision, evaluate_segment_group, merge_episode_final


STATE_PATH = ROOT_DIR / ".codex" / "state.json"
PROGRESS_PATH = ROOT_DIR / ".codex" / "PROGRESS.md"
PLAN_PATH = ROOT_DIR / "PLAN.md" if (ROOT_DIR / "PLAN.md").exists() else ROOT_DIR / "plan.md"
RESULT_PATH = ROOT_DIR / ".codex" / "rollout_result.json"
RECOVERY_STATE_PATH = ROOT_DIR / ".codex" / "same_session_supervisor_state.json"
RECOVERY_EVENT_LOG_PATH = ROOT_DIR / ".codex" / "same_session_supervisor_events.jsonl"
SCREENSHOT_DIR = ROOT_DIR / ".codex" / "screenshots"
WORKER_LOG_DIR = Path(
    os.environ.get("GEMINI_WORKER_LOG_DIR", "/mnt/c/Users/Master/AppData/Local/Temp/teogonia_worker_logs")
)
BATCH_SCRIPT = ROOT_DIR / "tools" / "gemini_ui_batch_shell.py"
STRICT_PROMPT_PATH = ROOT_DIR / "timer" / "worker_prompt_ko_strict.txt"
SESSION_ID = os.environ.get("CODEX_THREAD_ID")
DEFAULT_CODEX_COMMAND = os.environ.get("CODEX_COMMAND", "codex")
WINDOWS_PYTHON = Path("/mnt/c/Users/Master/AppData/Local/Programs/Python/Python310/python.exe")
BATCH_TIMEOUT_BASE_SECONDS = int(os.environ.get("GEMINI_BATCH_TIMEOUT_BASE_SECONDS", "180"))
BATCH_TIMEOUT_PER_SEGMENT_SECONDS = int(os.environ.get("GEMINI_BATCH_TIMEOUT_PER_SEGMENT_SECONDS", "240"))
WORKER_WINDOW_TITLE = os.environ.get("GEMINI_WORKER_WINDOW_TITLE", "Teogonia Gemini Worker")
STOP_REQUEST_PATH = Path(
    os.environ.get("GEMINI_SUPERVISOR_STOP_REQUEST_PATH", str(ROOT_DIR / "SUPERVISOR_STOP"))
)
HARD_BLOCKED_QUALITY_STATE = "hard_blocked"
MAX_RECOVERY_HISTORY = 24
MAX_DISTINCT_FAILED_RECOVERY_PATHS = 3
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
    "powershell",
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


def now_stamp() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z %Z")


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


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def log_progress(message: str) -> None:
    existing = read_text_with_fallback(PROGRESS_PATH) if PROGRESS_PATH.exists() else "# Progress\n"
    if not existing.endswith("\n"):
        existing += "\n"
    existing += f"- {now_stamp()}: {message}\n"
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
    completed = subprocess.run(
        ["wslpath", "-w", str(path)],
        capture_output=True,
        text=True,
        check=True,
    )
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
    completed = subprocess.run(
        ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script],
        capture_output=True,
        text=True,
        errors="replace",
        check=False,
    )
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
        if env_candidate.startswith("/mnt/"):
            env_executable = to_windows_path(Path(env_candidate))
        if env_candidate.lower().endswith(".exe"):
            candidates.append(
                WorkerRuntime(
                    label="env-windows-cmd-python",
                    command_prefix=("cmd.exe", "/c", env_executable),
                    uses_windows_paths=True,
                    opens_visible_window=True,
                )
            )
        else:
            candidates.append(
                WorkerRuntime(
                    label="env-python",
                    command_prefix=(env_candidate,),
                    uses_windows_paths=False,
                )
            )

    if WINDOWS_PYTHON.exists():
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


def wait_for_browser_recovery_window(
    runtime: WorkerRuntime,
    *,
    blocker: str,
    fallback_wait_seconds: int,
    resume_hint: dict[str, Any] | None = None,
) -> str:
    probe = probe_browser_state(runtime)
    if probe and probe.get("permission_dialog_cleared"):
        log_progress("Supervisor browser probe cleared a blocking browser permission dialog before waiting.")
    if browser_probe_ready_for_worker(probe):
        log_progress("Supervisor browser probe shows Gemini is already ready. Restarting the worker immediately.")
        return "resume_hint_wait" if resume_hint and resume_hint.get("wait_seconds") else "same_session_wait"

    wait_seconds, resume_at_iso = browser_probe_wait_details(probe, resume_hint, fallback_wait_seconds)
    while True:
        wait_message = f"WAIT {wait_seconds}s"
        if resume_at_iso:
            wait_message += f" until {resume_at_iso}"
        wait_message += f": {blocker}"
        log_plan(wait_message)
        write_rollout_result(
            "WAITING_FOR_RECOVERY",
            runtime=runtime,
            blocker=blocker,
            wait_seconds=wait_seconds,
            resume_at_iso=resume_at_iso,
            browser_probe=probe,
        )
        time.sleep(wait_seconds)

        probe = probe_browser_state(runtime)
        if probe and probe.get("permission_dialog_cleared"):
            log_progress("Supervisor browser probe cleared a blocking browser permission dialog after waking.")
        if browser_probe_ready_for_worker(probe):
            log_progress("Supervisor browser probe confirms Gemini is ready after the scheduled wait.")
            return "resume_hint_wait" if resume_hint and resume_hint.get("wait_seconds") else "same_session_wait"
        if browser_probe_requires_wait(probe):
            wait_seconds, resume_at_iso = browser_probe_wait_details(probe, None, max(wait_seconds, 60))
            log_progress("Supervisor browser probe still shows a Pro limit after waking. Rescheduling the wait window.")
            continue

        log_progress("Supervisor browser probe no longer shows a Pro limit after waking. Restarting the worker.")
        return "resume_hint_wait" if resume_hint and resume_hint.get("wait_seconds") else "same_session_wait"


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
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))


def save_state_for_edit(state: dict[str, Any]) -> None:
    write_json(STATE_PATH, state)


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


def append_recovery_attempt(item: dict[str, Any], path: str, detail: str) -> None:
    attempts = item.setdefault("recovery_attempts", [])
    attempts.append(
        {
            "path": path,
            "detail": detail,
            "status": "failed",
            "at": now_stamp(),
        }
    )
    overflow = len(attempts) - MAX_RECOVERY_HISTORY
    if overflow > 0:
        del attempts[:overflow]


def finalize_pending_recovery_actions(state: dict[str, Any], batch: list[StateItem]) -> bool:
    changed = False
    for item in matching_batch_items(state, batch):
        pending = item.pop("pending_recovery_action", None)
        if not isinstance(pending, dict):
            continue
        append_recovery_attempt(
            item,
            str(pending.get("path") or "same_session_retry_now"),
            str(
                pending.get("detail")
                or "Same-session recovery returned control, but the item re-entered recovery unresolved."
            ),
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
        changed = True
    return changed


def queue_pending_recovery_action(state: dict[str, Any], batch: list[StateItem], path: str, detail: str) -> bool:
    changed = False
    for item in matching_batch_items(state, batch):
        if not state_item_needs_generation(item):
            continue
        item["pending_recovery_action"] = {
            "path": path,
            "detail": detail,
            "queued_at": now_stamp(),
        }
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
        changed = True
    blocked = mark_hard_blocked_items(state, batch, detail)
    if changed or blocked:
        save_state_for_edit(state)
        rebuild_state(STATE_PATH)
    return blocked


def clear_pending_recovery_path(batch: list[StateItem]) -> None:
    state = load_state_for_edit()
    if clear_pending_recovery_actions(state, batch):
        save_state_for_edit(state)
        rebuild_state(STATE_PATH)


def queue_pending_same_session_recovery(batch: list[StateItem], path: str, detail: str) -> None:
    state = load_state_for_edit()
    if queue_pending_recovery_action(state, batch, path, detail):
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
            elif item["generated"]:
                changed = changed or item.get("quality_state") == "needs_regeneration"
                item["quality_state"] = "candidate"
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
                changed = changed or (not was_already_targeted)
            elif item["generated"]:
                item["quality_state"] = "candidate"
        return changed

    return changed


def process_segment_acceptance(session_anchor: str) -> bool:
    state = load_state_for_edit()
    changed = False
    for group in sorted(state.get("segment_groups", []), key=lambda item: (item["episode"], item["segment"])):
        if not group.get("has_all_passes"):
            continue
        if group.get("hard_blocked"):
            continue
        if group.get("accepted") and not group.get("needs_regeneration"):
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
    for episode in EPISODES:
        accepted_pass_by_segment = {
            item["segment"]: item["pass_number"]
            for item in state["items"]
            if item["episode"] == episode and item.get("accepted")
        }
        if len(accepted_pass_by_segment) != 7:
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
) -> str:
    state = load_state(STATE_PATH)
    browser_probe = probe_browser_state(runtime) if category == "supervisor_required" else None
    if category == "supervisor_required":
        if browser_probe and browser_probe.get("permission_dialog_cleared"):
            log_progress("Supervisor browser probe cleared a blocking permission dialog during supervisor-required recovery.")
        if browser_probe_ready_for_worker(browser_probe):
            log_progress("Supervisor browser probe shows Gemini is ready during supervisor-required recovery. Retrying now.")
            return "supervisor_required"
        if browser_probe_requires_wait(browser_probe):
            log_progress("Supervisor browser probe detected a Pro/quota wait during supervisor-required recovery.")
            return wait_for_browser_recovery_window(
                runtime,
                blocker=blocker,
                fallback_wait_seconds=browser_probe_wait_details(browser_probe, resume_hint, 300)[0],
                resume_hint=resume_hint,
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
        wait_seconds, resume_at_iso = browser_probe_wait_details(None, resume_hint, action.delay_seconds)
        if resume_hint and isinstance(resume_hint.get("wait_seconds"), int) and int(resume_hint["wait_seconds"]) > 0:
            log_progress(
                f"Using parsed Gemini resume hint wait={wait_seconds}s"
                + (f" resume_at={resume_at_iso}" if resume_at_iso else "")
                + "."
            )
        if category in {"pro_limit", "pro_mode_required"}:
            return wait_for_browser_recovery_window(
                runtime,
                blocker=blocker,
                fallback_wait_seconds=wait_seconds,
                resume_hint=resume_hint,
            )
        log_plan(f"WAIT {wait_seconds}s: {action.reason}")
        write_rollout_result(
            "WAITING_FOR_RECOVERY",
            runtime=runtime,
            blocker=blocker,
            wait_seconds=wait_seconds,
            resume_at_iso=resume_at_iso,
        )
        time.sleep(wait_seconds)
        if resume_hint and isinstance(resume_hint.get("wait_seconds"), int):
            return "resume_hint_wait"
        if category == "supervisor_required":
            return "supervisor_required"
        return "same_session_wait"
    if category == "supervisor_required":
        return "supervisor_required"
    return "same_session_retry_now"


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
    parser = argparse.ArgumentParser(description="Run the Teogonia same-session Gemini UI supervisor.")
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

    log_progress("Supervisor bootstrap. Same-session Chrome rollout continues under the current Codex anchor.")
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

        batch = choose_next_batch(state_items(data))
        if not batch:
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
                    log_progress(
                        f"Detected worker interruption for {label}. "
                        "An explicit stop request is present, but this still escalates to same-session recovery."
                    )
                else:
                    log_progress(
                        f"Detected worker interruption for {label}. "
                        "Escalating to same-session recovery before any local retry."
                    )
            blocked = finalize_pending_same_session_failures(batch, blocker)
            if blocked:
                return emit_hard_blocked(runtime, blocker, blocked)

            if requires_same_session_recovery(exc.category):
                log_progress(f"Same-session recovery required for {label}: {exc.category}.")
                write_rollout_result("WAITING_FOR_RECOVERY", runtime=runtime, blocker=blocker)
                same_session_path = apply_same_session_recovery(
                    bridge,
                    recovery_store,
                    category=exc.category,
                    blocker=blocker,
                    batch=batch,
                    runtime=runtime,
                    resume_hint=exc.resume_hint,
                    explicit_stop=explicit_stop,
                )
                queue_pending_same_session_recovery(batch, same_session_path, blocker)
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
                    same_session_path = apply_same_session_recovery(
                        bridge,
                        recovery_store,
                        category=strict_exc.category,
                        blocker=strict_blocker,
                        batch=batch,
                        runtime=runtime,
                        resume_hint=strict_exc.resume_hint,
                    )
                    queue_pending_same_session_recovery(batch, same_session_path, strict_blocker)
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
                        same_session_path = apply_same_session_recovery(
                            bridge,
                            recovery_store,
                            category=retry_exc.category,
                            blocker=retry_blocker,
                            batch=retry_batch,
                            runtime=runtime,
                            resume_hint=retry_exc.resume_hint,
                        )
                        queue_pending_same_session_recovery(retry_batch, same_session_path, retry_blocker)
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
                        same_session_path = apply_same_session_recovery(
                            bridge,
                            recovery_store,
                            category=hard_error.category,
                            blocker=hard_blocker,
                            batch=split_batch,
                            runtime=runtime,
                            resume_hint=hard_error.resume_hint,
                        )
                        queue_pending_same_session_recovery(split_batch, same_session_path, hard_blocker)
                        continue


if __name__ == "__main__":
    sys.exit(main())
