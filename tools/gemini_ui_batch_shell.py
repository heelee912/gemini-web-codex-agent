from __future__ import annotations

import argparse
import ctypes
import json
import os
import re
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path

import pygetwindow as gw
import pyperclip
import uiautomation as auto

from gemini_resume_hint import candidate_texts, extract_resume_hint


GEMINI_URL = "https://gemini.google.com/app?hl=ko"
ALLOW_BUTTON_NAMES = {"allow", "허용", "승인", "예"}
PRO_LIMIT_TEXTS = (
    "usage limit",
    "rate limit",
    "quota",
    "try again later",
    "잠시 후 다시",
    "나중에 다시",
    "한도",
    "제한",
)
FAST_MODE_TEXTS = (
    "flash",
    "빠른",
    "2.5 flash",
    "gemini flash",
)
CHROME_WINDOW_TOKENS = ("Chrome", "Google Gemini")
WORKER_WINDOW_TITLE = os.environ.get("GEMINI_WORKER_WINDOW_TITLE", "Teogonia Gemini Worker")
CHROME_LAUNCH_COMMANDS = (
    [r"C:\Program Files\Google\Chrome\Application\chrome.exe", GEMINI_URL],
    ["cmd.exe", "/c", "start", "", "chrome", GEMINI_URL],
)
FILE_DIALOG_TITLES = {"열기", "open"}
FILE_DIALOG_ACCEPT_BUTTONS = {"열기", "열기(o)", "열기(&o)", "open", "open(&o)"}
FILE_DIALOG_CANCEL_BUTTONS = {"취소", "cancel", "cancel(&c)"}
SUPERVISOR_REQUIRED_PREFIX = "supervisor_required:"
SUPERVISOR_REQUIRED_PATTERNS = (
    "Gemini ready screen did not appear",
    "Upload button not found",
    "Upload menu did not open",
    "Upload menu item did not appear",
    "File open dialog did not appear",
    "File dialog filename field mismatch",
    "File dialog filename field was not found",
    "File open dialog did not close",
    "Copy button not found",
    "Copy button returned empty clipboard",
)
COPY_BUTTON_EXACT_LABELS = {"코드 복사", "copy code"}
SRT_TIMING_PATTERN = re.compile(r"^\d{1,2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{1,2}:\d{2}:\d{2},\d{3}$")


@dataclass(frozen=True)
class SubtitleBatchItem:
    episode: int
    pass_number: int
    segment: int
    segment_path: Path
    output_path: Path

    @property
    def item_id(self) -> str:
        return f"E{self.episode:02d}-P{self.pass_number}-S{self.segment:02d}"


@dataclass(frozen=True)
class BrowserProbeResult:
    status: str
    wait_seconds: int | None
    resume_at_iso: str | None
    permission_dialog_cleared: bool
    visible_texts: list[str]
    matched_texts: list[str]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "wait_seconds": self.wait_seconds,
            "resume_at_iso": self.resume_at_iso,
            "permission_dialog_cleared": self.permission_dialog_cleared,
            "visible_texts": self.visible_texts,
            "matched_texts": self.matched_texts,
        }


class SupervisorRequiredError(RuntimeError):
    pass


def requires_supervisor(exc: Exception) -> bool:
    message = str(exc)
    return any(pattern in message for pattern in SUPERVISOR_REQUIRED_PATTERNS)


def extract_code_block(raw_text: str) -> str:
    if "```" not in raw_text:
        return raw_text.strip()
    for chunk in raw_text.split("```"):
        chunk = chunk.strip()
        if not chunk:
            continue
        if chunk.startswith("srt"):
            return chunk[3:].lstrip()
        return chunk
    return raw_text.strip()


def looks_like_copy_button(name: str, klass: str) -> bool:
    normalized_name = name.strip().lower()
    normalized_class = klass.strip().lower()
    if "copy-button" in normalized_class:
        return True
    return normalized_name in COPY_BUTTON_EXACT_LABELS


def parse_srt_cues(text: str) -> list[tuple[str, str, str]]:
    pattern = re.compile(
        r"(?ms)^\s*\d+\s*\n(\d{2}:\d{2}:\d{2},\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2},\d{3})\s*\n(.*?)(?=\n\s*\n|\Z)"
    )
    return pattern.findall(text)


def normalize_timestamp(value: str) -> str:
    parts = value.split(":")
    if len(parts) == 3:
        parts[0] = parts[0].zfill(2)
    return ":".join(parts)


def normalize_srt_text(text: str) -> str:
    timing_line = re.compile(r"^(\d{1,2}:\d{2}:\d{2},\d{3})\s*-->\s*(\d{1,2}:\d{2}:\d{2},\d{3})\s*$")
    normalized_lines: list[str] = []
    for raw_line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = raw_line.rstrip()
        match = timing_line.match(line.strip())
        if match:
            line = f"{normalize_timestamp(match.group(1))} --> {normalize_timestamp(match.group(2))}"
        normalized_lines.append(line)
    return "\n".join(normalized_lines).strip()


def build_items(root_dir: Path, episode: int, pass_number: int, segments: list[int]) -> list[SubtitleBatchItem]:
    work_dir = root_dir / f"video_only_retry_s01e{episode:02d}_rerun2"
    segment_dir = work_dir / "segments"
    output_dir = work_dir / "raw_speech_only" / f"pass{pass_number}"
    output_dir.mkdir(parents=True, exist_ok=True)
    items: list[SubtitleBatchItem] = []
    for segment in segments:
        segment_path = segment_dir / f"s01e{episode:02d}_seg{segment:02d}.mp4"
        output_path = output_dir / f"s01e{episode:02d}_seg{segment:02d}.srt"
        if not segment_path.exists():
            raise FileNotFoundError(f"Missing segment file: {segment_path}")
        items.append(
            SubtitleBatchItem(
                episode=episode,
                pass_number=pass_number,
                segment=segment,
                segment_path=segment_path,
                output_path=output_path,
            )
        )
    return items


class GeminiShellBatchRunner:
    def __init__(self, prompt_text: str) -> None:
        self.prompt_text = prompt_text.strip()

    @staticmethod
    def click_absolute_screen_position(x: int, y: int) -> None:
        user32 = getattr(ctypes, "windll", None)
        if user32 is None:
            raise RuntimeError("Absolute screen click is unavailable on this platform")
        user32 = user32.user32
        if not user32.SetCursorPos(int(x), int(y)):
            raise RuntimeError(f"SetCursorPos failed for absolute click at ({x}, {y})")
        time.sleep(0.05)
        user32.mouse_event(0x0002, 0, 0, 0, 0)
        user32.mouse_event(0x0004, 0, 0, 0, 0)
        time.sleep(0.05)

    def run_batch(self, items: list[SubtitleBatchItem]) -> list[SubtitleBatchItem]:
        for item in items:
            print(f"[{item.item_id}] start {item.segment_path.name}", flush=True)
            srt_text = self.run_item(item)
            item.output_path.write_text(srt_text.rstrip() + "\n", encoding="utf-8")
            if not item.output_path.exists() or item.output_path.stat().st_size <= 0:
                raise RuntimeError(f"{item.item_id}: output file was not written")
            print(f"[{item.item_id}] saved {item.output_path}\t{item.output_path.stat().st_size}", flush=True)
            self.close_active_tab()
        return items

    def run_item(self, item: SubtitleBatchItem) -> str:
        attempts = [
            ("fresh-click-send", "click"),
            ("fresh-enter-send", "send-enter"),
            ("fresh-ctrl-enter", "ctrl-enter"),
        ]
        errors: list[str] = []
        for attempt_name, send_strategy in attempts:
            try:
                print(f"[{item.item_id}] attempt {attempt_name}", flush=True)
                return self._run_single_attempt(item, send_strategy)
            except SupervisorRequiredError:
                raise
            except Exception as exc:
                if requires_supervisor(exc):
                    print(f"[{item.item_id}] escalate supervisor after {attempt_name}: {exc}", flush=True)
                    raise SupervisorRequiredError(str(exc))
                trace = traceback.format_exc(limit=8).strip().replace("\r", "").replace("\n", " || ")
                errors.append(f"{attempt_name}: {exc} || {trace}")
                print(f"[{item.item_id}] attempt failed {attempt_name}: {exc}", flush=True)
        raise RuntimeError(f"{item.item_id}: " + " | ".join(errors))

    def _run_single_attempt(self, item: SubtitleBatchItem, send_strategy: str) -> str:
        try:
            self.open_clean_tab()
            print(f"[{item.item_id}] opened clean tab", flush=True)
            self.ensure_ready_screen()
            print(f"[{item.item_id}] ready screen detected", flush=True)
            self.upload_segment(item.segment_path)
            print(f"[{item.item_id}] uploaded {item.segment_path.name}", flush=True)
            self.fill_prompt(self.prompt_text)
            print(f"[{item.item_id}] prompt filled", flush=True)
            self.send_prompt(send_strategy)
            print(f"[{item.item_id}] prompt sent via {send_strategy}", flush=True)
            raw_text = self.wait_for_completed_response()
            print(f"[{item.item_id}] response completed", flush=True)
            normalized = normalize_srt_text(extract_code_block(raw_text))
            if not parse_srt_cues(normalized):
                raise RuntimeError("response was not valid SRT")
            return normalized
        except Exception:
            self.dismiss_file_dialog()
            try:
                self.close_active_tab()
            except Exception:
                pass
            raise

    @staticmethod
    def looks_like_browser_window(window) -> bool:
        title = (window.title or "").strip()
        if not title:
            return False
        if title == WORKER_WINDOW_TITLE or WORKER_WINDOW_TITLE in title:
            return False
        return any(token in title for token in CHROME_WINDOW_TOKENS)

    @staticmethod
    def chrome_windows():
        return [
            window
            for window in gw.getAllWindows()
            if GeminiShellBatchRunner.looks_like_browser_window(window)
        ]

    def find_target_window(self):
        windows = self.chrome_windows()
        gemini_windows = [window for window in windows if "Gemini" in window.title]
        return gemini_windows[0] if gemini_windows else (windows[0] if windows else None)

    def launch_gemini_window(self) -> None:
        last_error: Exception | None = None
        for command in CHROME_LAUNCH_COMMANDS:
            try:
                subprocess.Popen(command)
            except Exception as exc:
                last_error = exc
                continue
            time.sleep(3.0)
            if self.find_target_window() is not None:
                return
        if last_error is not None:
            raise last_error

    def activate_window(self):
        target = self.find_target_window()
        if target is None:
            self.launch_gemini_window()
        for _attempt in range(5):
            target = self.find_target_window()
            if target is not None:
                break
            time.sleep(2.0)
        if target is None:
            raise RuntimeError("Chrome window not found")
        try:
            if target.isMinimized:
                target.restore()
            target.activate()
        except Exception:
            pass
        time.sleep(0.7)
        return target

    def root_control(self):
        window = self.activate_window()
        return auto.ControlFromHandle(window._hWnd)

    def walk_controls(self, max_depth: int = 50):
        last_error = None
        for _ in range(3):
            try:
                root = self.root_control()
                controls = list(auto.WalkControl(root, maxDepth=max_depth))
                for control, depth in controls:
                    yield control, depth
                return
            except Exception as exc:
                last_error = exc
                time.sleep(0.5)
        if last_error is not None:
            raise last_error

    @staticmethod
    def control_name(control) -> str:
        try:
            return (control.Name or "").strip()
        except Exception:
            return ""

    @staticmethod
    def control_class(control) -> str:
        try:
            return (control.ClassName or "").strip()
        except Exception:
            return ""

    @staticmethod
    def control_type(control) -> str:
        try:
            return (control.ControlTypeName or "").strip()
        except Exception:
            return ""

    @staticmethod
    def control_enabled(control) -> bool:
        try:
            return bool(control.IsEnabled)
        except Exception:
            return False

    def click_control(self, control) -> None:
        try:
            control.Click(simulateMove=False, waitTime=0.1)
            return
        except Exception:
            pass
        rect = control.BoundingRectangle
        self.click_absolute_screen_position((rect.left + rect.right) // 2, (rect.top + rect.bottom) // 2)

    @staticmethod
    def send_keys(keys: str, wait_time: float = 0.2) -> None:
        auto.SendKeys(keys, waitTime=wait_time)

    @staticmethod
    def visible(control) -> bool:
        try:
            rect = control.BoundingRectangle
            return rect.right > rect.left and rect.bottom > rect.top and not bool(control.IsOffscreen)
        except Exception:
            return False

    def has_visible_text(self, needles: tuple[str, ...]) -> bool:
        for control, _depth in self.walk_controls():
            if not self.visible(control):
                continue
            name = self.control_name(control)
            if not name:
                continue
            if any(needle in name for needle in needles):
                return True
        return False

    def visible_texts(self, limit: int = 200) -> list[str]:
        texts: list[str] = []
        seen: set[str] = set()
        for control, _depth in self.walk_controls():
            if not self.visible(control):
                continue
            name = self.control_name(control)
            if not name or name in seen:
                continue
            seen.add(name)
            texts.append(name)
            if len(texts) >= limit:
                break
        return texts

    def has_upload_read_error(self) -> bool:
        return self.has_visible_text(
            (
                "업로드하신 파일을 읽을 수 없습니다",
                "파일에 문제가 없는지 확인해 주세요",
                "파일이 삭제되었습니다",
                "couldn't read the uploaded file",
                "check whether the file has a problem",
                "file was deleted",
            )
        )

    def has_pro_limit_error(self) -> bool:
        return self.has_visible_text(PRO_LIMIT_TEXTS)

    def has_fast_mode_marker(self) -> bool:
        return self.has_visible_text(FAST_MODE_TEXTS)

    def build_pro_limit_error_message(self) -> str:
        texts = self.visible_texts()
        matched = candidate_texts(texts, limit=4)
        hint = extract_resume_hint(matched)
        parts = ["pro_limit_reached: Gemini Pro usage limit is visible"]
        if hint and hint.wait_seconds:
            parts.append(f"wait_seconds={hint.wait_seconds}")
        if hint and hint.resume_at_iso:
            parts.append(f"resume_at={hint.resume_at_iso}")
        if matched:
            joined = " || ".join(text.replace("\n", " ")[:180] for text in matched)
            parts.append(f"visible_text={joined}")
        return " | ".join(parts)

    def guard_session_constraints(self) -> None:
        if self.has_pro_limit_error():
            raise RuntimeError(self.build_pro_limit_error_message())
        if self.has_fast_mode_marker():
            raise RuntimeError("pro_mode_required: Fast or Flash mode marker is visible")

    def probe_browser_state(self) -> BrowserProbeResult:
        permission_dialog_cleared = self.clear_permission_popups()
        texts = self.visible_texts()
        matched = candidate_texts(texts, limit=6)
        hint = extract_resume_hint(matched)

        if self.has_pro_limit_error():
            status = "pro_limit"
        elif self.has_fast_mode_marker():
            status = "pro_mode_required"
        elif self.find_upload_button() and self.find_prompt_editor() and not self.has_processing_marker() and not self.has_attachment():
            status = "ready"
        elif self.find_copy_buttons():
            status = "response_ready"
        elif self.has_attachment() and self.find_prompt_editor():
            status = "draft_with_attachment"
        elif self.has_processing_marker() or self.send_button_is_stop():
            status = "busy"
        else:
            status = "unknown"

        return BrowserProbeResult(
            status=status,
            wait_seconds=hint.wait_seconds if hint else None,
            resume_at_iso=hint.resume_at_iso if hint else None,
            permission_dialog_cleared=permission_dialog_cleared,
            visible_texts=texts[:32],
            matched_texts=matched,
        )

    def clear_permission_popups(self) -> bool:
        clicked = False
        try:
            desktop = auto.GetRootControl()
            for window in desktop.GetChildren():
                window_name = self.control_name(window)
                if not window_name:
                    continue
                lowered = window_name.lower()
                if not any(token in lowered for token in ("chrome", "permission", "권한", "gemini", "google", "remote")):
                    continue
                try:
                    controls = list(auto.WalkControl(window, maxDepth=8))
                except Exception:
                    continue
                for control, _depth in controls:
                    if self.control_type(control) != "ButtonControl":
                        continue
                    name = self.control_name(control)
                    if not name or not self.visible(control):
                        continue
                    if name.lower() in ALLOW_BUTTON_NAMES:
                        self.click_control(control)
                        clicked = True
                        time.sleep(1.0)
        except Exception:
            return clicked
        return clicked

    def find_prompt_editor(self):
        for control, _depth in self.walk_controls():
            if self.control_type(control) != "EditControl":
                continue
            if "new-input-ui" in self.control_class(control):
                return control
        return None

    def find_upload_button(self):
        for control, _depth in self.walk_controls():
            if self.control_type(control) != "ButtonControl":
                continue
            if "upload-card-button" in self.control_class(control):
                return control
        return None

    def find_send_button(self):
        for control, _depth in self.walk_controls():
            if self.control_type(control) != "ButtonControl":
                continue
            if "send-button" in self.control_class(control):
                return control
        return None

    def find_new_chat_button(self):
        for control, _depth in self.walk_controls():
            if self.control_type(control) != "ButtonControl":
                continue
            name = self.control_name(control)
            if name == "새 채팅" or "bard-logo-container" in self.control_class(control):
                return control
        return None

    def find_copy_buttons(self) -> list:
        matches = []
        for control, _depth in self.walk_controls():
            if self.control_type(control) != "ButtonControl":
                continue
            name = self.control_name(control)
            klass = self.control_class(control)
            if looks_like_copy_button(name, klass):
                matches.append(control)
        matches.sort(key=lambda control: control.BoundingRectangle.bottom)
        return matches

    def has_processing_marker(self) -> bool:
        for control, _depth in self.walk_controls():
            if self.control_type(control) != "ButtonControl":
                continue
            if "processing-state_button" in self.control_class(control):
                return True
        return False

    def send_button_is_stop(self) -> bool:
        for control, _depth in self.walk_controls():
            if self.control_type(control) != "ButtonControl":
                continue
            klass = self.control_class(control)
            if "send-button" in klass and "stop" in klass:
                return True
        return False

    def has_attachment(self, filename: str | None = None) -> bool:
        for control, _depth in self.walk_controls():
            name = self.control_name(control)
            klass = self.control_class(control)
            if "upload-file-card-container" in klass:
                return True
            if filename and filename in name:
                return True
            if filename and name == f"{filename} 파일 삭제":
                return True
        return False

    def current_draft_matches(self, filename: str) -> bool:
        return self.has_attachment(filename) and self.find_prompt_editor() is not None

    def desktop_windows(self):
        desktop = auto.GetRootControl()
        seen: set[int] = set()
        for window, _depth in auto.WalkControl(desktop, maxDepth=4):
            if self.control_type(window) != "WindowControl":
                continue
            if not self.visible(window):
                continue
            handle = getattr(window, "NativeWindowHandle", 0) or id(window)
            if handle in seen:
                continue
            seen.add(handle)
            yield window

    def find_file_dialog(self):
        candidates = []
        for window in self.desktop_windows():
            name = self.control_name(window)
            lowered = name.lower()
            klass = self.control_class(window)
            if lowered in FILE_DIALOG_TITLES or "열기" in name or klass == "#32770":
                candidates.append(window)
        for window in candidates:
            lowered = self.control_name(window).lower()
            if lowered in FILE_DIALOG_TITLES or "열기" in self.control_name(window):
                return window
        return candidates[0] if candidates else None

    def wait_for_file_dialog(self, timeout_seconds: float = 15.0):
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            dialog = self.find_file_dialog()
            if dialog is not None:
                return dialog
            time.sleep(0.2)
        raise TimeoutError("File open dialog did not appear")

    def dialog_controls(self, dialog, max_depth: int = 15):
        controls = []
        queue = [(dialog, 0)]
        while queue:
            parent, depth = queue.pop(0)
            if depth >= max_depth:
                continue
            try:
                children = parent.GetChildren()
            except Exception:
                continue
            for child in children:
                child_depth = depth + 1
                controls.append((child, child_depth))
                queue.append((child, child_depth))
        return controls

    def find_file_dialog_filename_edit(self, dialog):
        candidates = []
        for control, _depth in self.dialog_controls(dialog):
            if self.control_type(control) != "EditControl":
                continue
            if not self.visible(control):
                continue
            rect = control.BoundingRectangle
            width = rect.right - rect.left
            if width < 160:
                continue
            candidates.append(control)
        candidates.sort(key=lambda control: (control.BoundingRectangle.bottom, control.BoundingRectangle.left))
        return candidates[-1] if candidates else None

    def find_file_dialog_accept_button(self, dialog):
        for control, _depth in self.dialog_controls(dialog):
            if self.control_type(control) != "ButtonControl":
                continue
            if not self.visible(control):
                continue
            lowered = self.control_name(control).lower()
            if lowered in FILE_DIALOG_ACCEPT_BUTTONS:
                return control
        return None

    def find_file_dialog_cancel_button(self, dialog):
        for control, _depth in self.dialog_controls(dialog):
            if self.control_type(control) != "ButtonControl":
                continue
            if not self.visible(control):
                continue
            lowered = self.control_name(control).lower()
            if lowered in FILE_DIALOG_CANCEL_BUTTONS:
                return control
        return None

    @staticmethod
    def normalize_dialog_path(value: str) -> str:
        return value.strip().strip('"').replace("/", "\\").lower()

    def verify_dialog_filename_value(self, expected_path: Path) -> None:
        self.send_keys("{Ctrl}a")
        time.sleep(0.1)
        pyperclip.copy("")
        self.send_keys("{Ctrl}c")
        time.sleep(0.2)
        actual = self.normalize_dialog_path(pyperclip.paste())
        expected = self.normalize_dialog_path(str(expected_path))
        if actual != expected:
            raise SupervisorRequiredError(
                f"File dialog filename field mismatch: expected={expected_path} actual={pyperclip.paste()!r}"
            )

    def dismiss_file_dialog(self) -> bool:
        dialog = self.find_file_dialog()
        if dialog is None:
            return False
        cancel_button = self.find_file_dialog_cancel_button(dialog)
        if cancel_button is not None:
            self.click_control(cancel_button)
        else:
            try:
                dialog.SetFocus()
            except Exception:
                pass
            self.send_keys("{Esc}")
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if self.find_file_dialog() is None:
                return True
            time.sleep(0.2)
        return False

    def choose_file_from_dialog(self, segment_path: Path) -> None:
        dialog = self.wait_for_file_dialog()
        filename_edit = self.find_file_dialog_filename_edit(dialog)
        pyperclip.copy(str(segment_path))
        if filename_edit is not None:
            filename_edit.SetFocus()
            self.click_control(filename_edit)
            time.sleep(0.2)
        else:
            raise SupervisorRequiredError("File dialog filename field was not found")
        self.send_keys("{Ctrl}a")
        time.sleep(0.1)
        self.send_keys("{Ctrl}v")
        time.sleep(0.2)
        self.verify_dialog_filename_value(segment_path)
        accept_button = self.find_file_dialog_accept_button(dialog)
        if accept_button is not None:
            self.click_control(accept_button)
        else:
            self.send_keys("{Enter}")

        deadline = time.time() + 15.0
        while time.time() < deadline:
            if self.find_file_dialog() is None:
                return
            time.sleep(0.2)
        raise SupervisorRequiredError(f"File open dialog did not close for {segment_path.name}")

    def open_clean_tab(self) -> None:
        self.dismiss_file_dialog()
        self.activate_window()
        self.send_keys("{Ctrl}t")
        time.sleep(0.5)
        pyperclip.copy(GEMINI_URL)
        self.send_keys("{Ctrl}l")
        time.sleep(0.2)
        self.send_keys("{Ctrl}v")
        time.sleep(0.2)
        self.send_keys("{Enter}")
        time.sleep(5.0)

    def close_active_tab(self) -> None:
        self.activate_window()
        self.send_keys("{Ctrl}w")
        time.sleep(0.5)

    def ensure_ready_screen(self, timeout_seconds: float = 60.0) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            self.guard_session_constraints()
            if self.find_upload_button() and self.find_prompt_editor() and not self.has_processing_marker() and not self.has_attachment():
                return
            self.clear_permission_popups()
            time.sleep(1.0)
        raise TimeoutError("Gemini ready screen did not appear")

    def open_upload_menu(self) -> None:
        button = self.find_upload_button()
        if not button:
            raise RuntimeError("Upload button not found")
        try:
            button.SetFocus()
        except Exception:
            pass
        self.send_keys("{Enter}")
        time.sleep(0.7)
        if self.upload_menu_is_open():
            return
        self.click_control(button)
        time.sleep(0.5)
        if self.upload_menu_is_open():
            return
        raise RuntimeError("Upload menu did not open")

    def upload_menu_is_open(self) -> bool:
        button = self.find_upload_button()
        if button is not None:
            klass = self.control_class(button)
            name = self.control_name(button)
            if "upload-card-button close" in klass or "메뉴 닫기" in name or "menu close" in name.lower():
                return True
        for control, _depth in self.walk_controls():
            if self.control_type(control) != "MenuItemControl":
                continue
            if "mat-mdc-list-item" not in self.control_class(control):
                continue
            if self.visible(control):
                return True
        return False

    def select_first_upload_menu_item(self) -> None:
        deadline = time.time() + 15.0
        keyboard_fallback_used = False
        while time.time() < deadline:
            if self.find_file_dialog() is not None:
                return
            items = []
            for control, _depth in self.walk_controls():
                if self.control_type(control) != "MenuItemControl":
                    continue
                if "mat-mdc-list-item" in self.control_class(control) and self.visible(control):
                    items.append(control)
            if items:
                try:
                    items[0].SetFocus()
                except Exception:
                    pass
                self.send_keys("{Enter}")
                time.sleep(0.7)
                if self.find_file_dialog() is not None:
                    return
                self.click_control(items[0])
                time.sleep(0.7)
                if self.find_file_dialog() is not None:
                    return
            elif not keyboard_fallback_used:
                self.send_keys("{Enter}")
                keyboard_fallback_used = True
                time.sleep(1.0)
                if self.find_file_dialog() is not None:
                    return
            time.sleep(0.2)
        final_deadline = time.time() + 5.0
        while time.time() < final_deadline:
            if self.find_file_dialog() is not None:
                return
            time.sleep(0.2)
        raise RuntimeError("Upload menu item did not appear")

    def upload_segment(self, segment_path: Path) -> None:
        self.open_upload_menu()
        self.select_first_upload_menu_item()
        self.choose_file_from_dialog(segment_path)
        deadline = time.time() + 180.0
        while time.time() < deadline:
            self.guard_session_constraints()
            if self.has_upload_read_error():
                raise RuntimeError(f"Gemini reported upload read failure for {segment_path.name}")
            if self.has_attachment(segment_path.name):
                return
            self.clear_permission_popups()
            time.sleep(0.5)
        raise TimeoutError(f"Attachment did not appear for {segment_path.name}")

    def fill_prompt(self, prompt: str) -> None:
        self.guard_session_constraints()
        editor = self.find_prompt_editor()
        if not editor:
            raise RuntimeError("Prompt editor not found")
        self.click_control(editor)
        time.sleep(0.2)
        self.send_keys("{Ctrl}a")
        pyperclip.copy(prompt)
        self.send_keys("{Ctrl}v")
        time.sleep(0.8)

    def send_prompt(self, strategy: str, send_wait_seconds: float = 60.0) -> None:
        deadline = time.time() + send_wait_seconds
        while time.time() < deadline:
            self.guard_session_constraints()
            if self.has_upload_read_error():
                raise RuntimeError("Gemini reported upload read failure before send")
            send_button = self.find_send_button()
            if send_button and self.visible(send_button):
                if strategy == "click":
                    self.click_control(send_button)
                elif strategy == "send-enter":
                    self.click_control(send_button)
                    time.sleep(0.2)
                    self.send_keys("{Enter}")
                elif strategy == "ctrl-enter":
                    editor = self.find_prompt_editor()
                    if not editor:
                        raise RuntimeError("Prompt editor not found before send")
                    self.click_control(editor)
                    time.sleep(0.2)
                    self.send_keys("{Ctrl}{Enter}")
                else:
                    raise RuntimeError(f"Unknown send strategy: {strategy}")
                time.sleep(2.0)
                if self.has_processing_marker() or self.send_button_is_stop() or self.find_copy_buttons():
                    return

            self.clear_permission_popups()
            time.sleep(5.0)

        raise TimeoutError("Send did not leave draft state")

    def wait_for_completed_response(self, timeout_seconds: float = 900.0) -> str:
        deadline = time.time() + timeout_seconds
        stable_since: float | None = None
        while time.time() < deadline:
            self.guard_session_constraints()
            if self.has_upload_read_error():
                raise RuntimeError("Gemini reported upload read failure while waiting for response")
            copy_buttons = self.find_copy_buttons()
            processing = self.has_processing_marker()
            if copy_buttons and not processing:
                if stable_since is None:
                    stable_since = time.time()
                elif time.time() - stable_since >= 5.0:
                    response_text = self.copy_last_response()
                    if response_text.strip():
                        return response_text
                    raise SupervisorRequiredError("Copy button returned empty clipboard")
            elif not processing:
                response_text = self.extract_srt_from_visible_texts()
                if response_text:
                    return response_text
            else:
                stable_since = None
            time.sleep(1.5)
        raise TimeoutError("Gemini response did not complete")

    def wait_for_clipboard_change(self, previous_text: str = "", timeout_seconds: float = 3.0) -> str:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            current = pyperclip.paste()
            if current and current != previous_text:
                return current
            time.sleep(0.1)
        return pyperclip.paste()

    def extract_srt_from_visible_texts(self) -> str:
        candidate_lines: list[str] = []
        capturing = False
        for text in self.visible_texts(limit=400):
            for raw_line in text.replace("\r", "\n").split("\n"):
                line = raw_line.strip()
                if not line:
                    continue
                if looks_like_copy_button(line, ""):
                    continue
                if SRT_TIMING_PATTERN.match(line):
                    if candidate_lines and candidate_lines[-1].isdigit():
                        capturing = True
                    elif not candidate_lines:
                        candidate_lines.append("1")
                        capturing = True
                    candidate_lines.append(line)
                    continue
                if line.isdigit() and (not candidate_lines or SRT_TIMING_PATTERN.match(candidate_lines[-1])):
                    candidate_lines.append(line)
                    continue
                if capturing:
                    candidate_lines.append(line)
        if not candidate_lines:
            return ""
        candidate = normalize_srt_text("\n".join(candidate_lines))
        return candidate if parse_srt_cues(candidate) else ""

    def copy_last_response(self) -> str:
        copy_buttons = self.find_copy_buttons()
        if not copy_buttons:
            raise SupervisorRequiredError("Copy button not found")
        for copy_button in reversed(copy_buttons):
            for trigger in ("click", "enter"):
                pyperclip.copy("")
                if trigger == "click":
                    self.click_control(copy_button)
                else:
                    try:
                        copy_button.SetFocus()
                    except Exception:
                        pass
                    self.send_keys("{Enter}")
                copied = self.wait_for_clipboard_change("", timeout_seconds=2.5)
                if copied.strip():
                    return copied
        visible_fallback = self.extract_srt_from_visible_texts()
        if visible_fallback:
            return visible_fallback
        raise SupervisorRequiredError("Copy button returned empty clipboard")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Gemini UI subtitle batch through shell automation.")
    parser.add_argument("--probe-browser-state", action="store_true")
    parser.add_argument("--episode", type=int)
    parser.add_argument("--pass-number", type=int)
    parser.add_argument("--segments", type=int, nargs="+")
    parser.add_argument("--prompt-path", type=Path, default=Path("timer/worker_prompt_ko.txt"))
    args = parser.parse_args()
    if not args.probe_browser_state:
        if args.episode is None:
            parser.error("--episode is required unless --probe-browser-state is used")
        if args.pass_number is None:
            parser.error("--pass-number is required unless --probe-browser-state is used")
        if not args.segments:
            parser.error("--segments is required unless --probe-browser-state is used")
    return args


def main() -> int:
    args = parse_args()
    root_dir = Path(__file__).resolve().parents[1]
    prompt_text = ""
    if not args.probe_browser_state:
        prompt_path = (root_dir / args.prompt_path).resolve()
        prompt_text = prompt_path.read_text(encoding="utf-8-sig").strip()
    runner = GeminiShellBatchRunner(prompt_text)
    try:
        if args.probe_browser_state:
            print(json.dumps(runner.probe_browser_state().to_dict(), ensure_ascii=False), flush=True)
            return 0
        items = build_items(root_dir, args.episode, args.pass_number, args.segments)
        runner.run_batch(items)
    except SupervisorRequiredError as exc:
        print(f"{SUPERVISOR_REQUIRED_PREFIX} {exc}", file=sys.stderr, flush=True)
        return 86
    for item in items:
        size = item.output_path.stat().st_size
        print(f"{item.output_path}\t{size}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
