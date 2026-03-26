from __future__ import annotations

import json
import sys
import tempfile
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
sys.modules.setdefault("pyautogui", types.SimpleNamespace(FAILSAFE=False, PAUSE=0.0))
sys.modules.setdefault("pygetwindow", types.SimpleNamespace(getAllWindows=lambda: []))
sys.modules.setdefault("pyperclip", types.SimpleNamespace(copy=lambda _text: None, paste=lambda: ""))
sys.modules.setdefault("uiautomation", types.SimpleNamespace(SendKeys=lambda *_args, **_kwargs: None))

from gemini_ui_supervisor import (  # noqa: E402
    HARD_BLOCKED_QUALITY_STATE,
    WorkerRuntime,
    build_visible_worker_launch_payload,
    browser_probe_ready_for_worker,
    browser_probe_requires_wait,
    browser_probe_wait_details,
    classify_worker_failure,
    coerce_process_output,
    distinct_failed_hard_block_paths,
    mark_hard_blocked_items,
    recovery_issue,
    requires_same_session_recovery,
    rollout_snapshot,
    rollout_is_complete,
    unresolved_items,
)
from gemini_ui_batch_shell import GeminiShellBatchRunner, looks_like_copy_button, requires_supervisor  # noqa: E402
from next_batch import StateItem, choose_next_batch  # noqa: E402
from same_session_recovery import (  # noqa: E402
    RecoveryStateStore,
    request_recovery_decision,
    summarize_repeated_issue,
)
from teogonia_rollout import SubtitleCue, evaluate_segment_group, render_srt  # noqa: E402


def write_srt(path: Path, text: str) -> None:
    cues = [
        SubtitleCue(
            start_ms=0,
            end_ms=900,
            text=text,
        )
    ]
    path.write_text(render_srt(cues), encoding="utf-8")


def write_episode_fixture(root_dir: Path, *, episode: int = 1) -> Path:
    work_dir = root_dir / f"video_only_retry_s01e{episode:02d}_rerun2"
    manifest = {
        "segments": [{"durationMs": 1000} for _ in range(7)],
    }
    work_dir.mkdir(parents=True, exist_ok=True)
    (work_dir / f"s01e{episode:02d}.manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    for pass_number in (1, 2, 3):
        (work_dir / "raw_speech_only" / f"pass{pass_number}").mkdir(parents=True, exist_ok=True)
    return work_dir


def write_whisper_fixture(whisper_dir: Path, *, episode: int = 1, text: str = "こんにちは 世界") -> None:
    whisper_dir.mkdir(parents=True, exist_ok=True)
    cues = [
        SubtitleCue(
            start_ms=0,
            end_ms=900,
            text=text,
        )
    ]
    (whisper_dir / f"[Judas] Teogonia - S01E{episode:02d}.srt").write_text(
        render_srt(cues),
        encoding="utf-8",
    )


class RolloutRulesTests(unittest.TestCase):
    def test_evaluate_segment_group_marks_unanimous(self) -> None:
        with tempfile.TemporaryDirectory() as root_tmp, tempfile.TemporaryDirectory() as whisper_tmp:
            root_dir = Path(root_tmp)
            whisper_dir = Path(whisper_tmp)
            work_dir = write_episode_fixture(root_dir)
            write_whisper_fixture(whisper_dir, text="こんにちは 世界")
            for pass_number in (1, 2, 3):
                write_srt(
                    work_dir / "raw_speech_only" / f"pass{pass_number}" / "s01e01_seg01.srt",
                    "こんにちは 世界",
                )

            decision = evaluate_segment_group(root_dir, 1, 1, whisper_dir=whisper_dir)

        self.assertEqual(decision.status, "accepted")
        self.assertEqual(decision.quality_state, "accepted_unanimous")
        self.assertEqual(decision.selected_pass_number, 1)

    def test_evaluate_segment_group_uses_whisper_tiebreaker(self) -> None:
        with tempfile.TemporaryDirectory() as root_tmp, tempfile.TemporaryDirectory() as whisper_tmp:
            root_dir = Path(root_tmp)
            whisper_dir = Path(whisper_tmp)
            work_dir = write_episode_fixture(root_dir)
            write_whisper_fixture(whisper_dir, text="こんにちは 世界 今日は本当にいい天気ですね 散歩に行きましょう")
            write_srt(work_dir / "raw_speech_only" / "pass1" / "s01e01_seg01.srt", "こんにちは 世界 今日はいい天気です 散歩しましょう")
            write_srt(work_dir / "raw_speech_only" / "pass2" / "s01e01_seg01.srt", "こんにちは 世界 今日は本当にいい天気ですね 散歩に行きましょう")
            write_srt(work_dir / "raw_speech_only" / "pass3" / "s01e01_seg01.srt", "まったく ちがう")

            decision = evaluate_segment_group(root_dir, 1, 1, whisper_dir=whisper_dir)

        self.assertEqual(decision.quality_state, "accepted_consistent")
        self.assertEqual(decision.selected_pass_number, 2)
        self.assertEqual(decision.selection_basis, "whisper_similarity")

    def test_evaluate_segment_group_keeps_gemini_when_whisper_is_garbled(self) -> None:
        with tempfile.TemporaryDirectory() as root_tmp, tempfile.TemporaryDirectory() as whisper_tmp:
            root_dir = Path(root_tmp)
            whisper_dir = Path(whisper_tmp)
            work_dir = write_episode_fixture(root_dir)
            write_whisper_fixture(whisper_dir, text="아")
            write_srt(work_dir / "raw_speech_only" / "pass1" / "s01e01_seg01.srt", "こんにちは 世界")
            write_srt(work_dir / "raw_speech_only" / "pass2" / "s01e01_seg01.srt", "こんにちは 世界だ")
            write_srt(work_dir / "raw_speech_only" / "pass3" / "s01e01_seg01.srt", "まったく ちがう")

            decision = evaluate_segment_group(root_dir, 1, 1, whisper_dir=whisper_dir)

        self.assertEqual(decision.quality_state, "accepted_consistent")
        self.assertEqual(decision.selected_pass_number, 1)
        self.assertEqual(decision.selection_basis, "gemini_consistency_preserved")
        self.assertEqual(decision.whisper_review["status"], "garbled")

    def test_choose_next_batch_prioritizes_missing_generation(self) -> None:
        items = [
            StateItem(
                id="E01-P1-S01",
                episode=1,
                pass_number=1,
                segment=1,
                path="one",
                generated=True,
                exists=True,
                size=10,
                quality_state="needs_regeneration",
            ),
            StateItem(
                id="E04-P2-S01",
                episode=4,
                pass_number=2,
                segment=1,
                path="two",
                generated=False,
                exists=False,
                size=0,
                quality_state="unchecked",
            ),
        ]

        batch = choose_next_batch(items, batch_size=3)

        self.assertEqual([item.id for item in batch], ["E04-P2-S01"])

    def test_choose_next_batch_uses_regeneration_after_generation_is_drained(self) -> None:
        items = [
            StateItem(
                id="E01-P1-S01",
                episode=1,
                pass_number=1,
                segment=1,
                path="one",
                generated=True,
                exists=True,
                size=10,
                quality_state="candidate",
            ),
            StateItem(
                id="E01-P2-S02",
                episode=1,
                pass_number=2,
                segment=2,
                path="two",
                generated=True,
                exists=True,
                size=11,
                quality_state="needs_regeneration",
            ),
        ]

        batch = choose_next_batch(items, batch_size=3)

        self.assertEqual([item.id for item in batch], ["E01-P2-S02"])

    def test_classify_worker_failure_detects_interrupted_visible_worker(self) -> None:
        runtime = WorkerRuntime(
            label="windows-cmd-python",
            command_prefix=("cmd.exe", "/c", "python"),
            uses_windows_paths=True,
            opens_visible_window=True,
        )

        category = classify_worker_failure("", "", returncode=130, runtime=runtime)

        self.assertEqual(category, "worker_interrupted")
        self.assertTrue(requires_same_session_recovery(category))

    def test_classify_worker_failure_treats_exit_86_as_supervisor_required(self) -> None:
        runtime = WorkerRuntime(
            label="windows-cmd-python",
            command_prefix=("cmd.exe", "/c", "python"),
            uses_windows_paths=True,
            opens_visible_window=True,
        )

        category = classify_worker_failure("", "", returncode=86, runtime=runtime)

        self.assertEqual(category, "supervisor_required")

    def test_requires_same_session_recovery_includes_timeout_and_worker_close(self) -> None:
        self.assertTrue(requires_same_session_recovery("automation_stalled"))
        self.assertTrue(requires_same_session_recovery("worker_interrupted"))
        self.assertTrue(requires_same_session_recovery("supervisor_required"))
        self.assertFalse(requires_same_session_recovery("automation_failed"))

    def test_coerce_process_output_decodes_timeout_bytes(self) -> None:
        self.assertEqual(coerce_process_output(None), "")
        self.assertEqual(coerce_process_output("plain"), "plain")
        self.assertEqual(coerce_process_output("한글".encode("utf-8")), "한글")

    def test_rollout_snapshot_includes_browser_probe_when_present(self) -> None:
        snapshot = rollout_snapshot(
            {
                "summary": {"total": 1, "generated": 0, "remaining": 1},
                "items": [{"id": "E01-P1-S01", "generated": False, "quality_state": "unchecked"}],
                "segment_groups": [],
            },
            blocker="E01-P1-S01: automation_stalled",
            browser_probe={"status": "pro_limit", "wait_seconds": 300},
        )

        self.assertEqual(snapshot["browser_probe"]["status"], "pro_limit")
        self.assertEqual(snapshot["blocker"], "E01-P1-S01: automation_stalled")

    def test_visible_worker_launch_payload_uses_powershell_start_process(self) -> None:
        payload = build_visible_worker_launch_payload(
            [
                r"C:\Users\Master\AppData\Local\Programs\Python\Python310\python.exe",
                r"E:\Media\신통기\tools\gemini_ui_batch_shell.py",
                "--episode",
                "8",
            ]
        )

        self.assertIn("powershell.exe", payload)
        self.assertIn("-EncodedCommand", payload)

    def test_visible_worker_runtime_builds_direct_powershell_command(self) -> None:
        runtime = WorkerRuntime(
            label="windows-cmd-python",
            command_prefix=("cmd.exe", "/c", r"C:\Users\Master\AppData\Local\Programs\Python\Python310\python.exe"),
            uses_windows_paths=True,
            opens_visible_window=True,
        )

        command = runtime.build_command(Path("/mnt/e/Media/신통기/tools/gemini_ui_batch_shell.py"), "--episode", "8")

        self.assertEqual(command[0], "powershell.exe")
        self.assertIn("-EncodedCommand", command)
        self.assertNotEqual(command[:2], ["cmd.exe", "/c"])

    def test_browser_window_filter_excludes_worker_console_title(self) -> None:
        class FakeWindow:
            def __init__(self, title: str) -> None:
                self.title = title

        self.assertFalse(GeminiShellBatchRunner.looks_like_browser_window(FakeWindow("Teogonia Gemini Worker")))
        self.assertTrue(GeminiShellBatchRunner.looks_like_browser_window(FakeWindow("Google Gemini - Chrome")))

    def test_looks_like_copy_button_requires_response_specific_copy_labels(self) -> None:
        self.assertTrue(looks_like_copy_button("코드 복사", ""))
        self.assertTrue(looks_like_copy_button("Copy code", ""))
        self.assertTrue(looks_like_copy_button("", "mat copy-button action"))
        self.assertFalse(looks_like_copy_button("복사", ""))
        self.assertFalse(looks_like_copy_button("Copy", ""))
        self.assertFalse(looks_like_copy_button("새 채팅", ""))

    def test_copy_button_failures_escalate_to_supervisor(self) -> None:
        self.assertTrue(requires_supervisor(RuntimeError("Copy button not found")))
        self.assertTrue(requires_supervisor(RuntimeError("Copy button returned empty clipboard")))

    def test_extract_srt_from_visible_texts_recovers_response_without_copy_button(self) -> None:
        runner = GeminiShellBatchRunner("prompt")
        runner.visible_texts = lambda limit=400: [
            "Gemini",
            "1\n00:00:01,000 --> 00:00:02,000\n안녕하세요",
            "코드 복사",
        ]

        extracted = runner.extract_srt_from_visible_texts()

        self.assertEqual(
            extracted,
            "1\n00:00:01,000 --> 00:00:02,000\n안녕하세요",
        )

    def test_browser_probe_wait_helpers_prefer_browser_hint_and_ready_signal(self) -> None:
        probe = {
            "status": "pro_limit",
            "wait_seconds": 3600,
            "resume_at_iso": "2026-03-26T02:00:00+09:00",
        }

        wait_seconds, resume_at_iso = browser_probe_wait_details(
            probe,
            {"wait_seconds": 120, "resume_at_iso": "2026-03-26T01:05:00+09:00"},
            30,
        )

        self.assertTrue(browser_probe_requires_wait(probe))
        self.assertEqual(wait_seconds, 3600)
        self.assertEqual(resume_at_iso, "2026-03-26T02:00:00+09:00")
        self.assertTrue(browser_probe_ready_for_worker({"status": "ready"}))
        self.assertTrue(browser_probe_ready_for_worker({"status": "draft_with_attachment"}))
        self.assertFalse(browser_probe_requires_wait({"status": "ready"}))

    def test_mark_hard_blocked_after_three_distinct_countable_paths(self) -> None:
        state = {
            "items": [
                {
                    "id": "E01-P2-S01",
                    "episode": 1,
                    "segment": 1,
                    "pass_number": 2,
                    "generated": False,
                    "quality_state": "unchecked",
                    "recovery_attempts": [
                        {"path": "strict_prompt_batch_retry", "status": "failed"},
                        {"path": "single_item_recovery", "status": "failed"},
                        {"path": "same_session_wait", "status": "failed"},
                    ],
                }
            ]
        }
        batch = [
            StateItem(
                id="E01-P2-S01",
                episode=1,
                pass_number=2,
                segment=1,
                path="one",
                generated=False,
                exists=False,
                size=0,
                quality_state="unchecked",
            )
        ]

        blocked = mark_hard_blocked_items(state, batch, "test blocker")

        self.assertEqual(distinct_failed_hard_block_paths(state["items"][0]), [
            "strict_prompt_batch_retry",
            "single_item_recovery",
            "same_session_wait",
        ])
        self.assertEqual(blocked[0]["id"], "E01-P2-S01")
        self.assertEqual(state["items"][0]["quality_state"], HARD_BLOCKED_QUALITY_STATE)

    def test_unresolved_items_ignores_batch_already_reconciled_on_disk(self) -> None:
        with tempfile.TemporaryDirectory() as root_tmp:
            root_dir = Path(root_tmp)
            work_dir = write_episode_fixture(root_dir, episode=7)
            write_srt(work_dir / "raw_speech_only" / "pass3" / "s01e07_seg01.srt", "こんにちは")
            write_srt(work_dir / "raw_speech_only" / "pass3" / "s01e07_seg02.srt", "こんばんは")
            write_srt(work_dir / "raw_speech_only" / "pass3" / "s01e07_seg03.srt", "おはよう")

            state_path = root_dir / ".codex" / "state.json"
            root_dir.joinpath(".codex").mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "id": "E07-P3-S01",
                                "episode": 7,
                                "pass_number": 3,
                                "segment": 1,
                                "path": "video_only_retry_s01e07_rerun2/raw_speech_only/pass3/s01e07_seg01.srt",
                                "generated": True,
                                "exists": True,
                                "size": 100,
                                "quality_state": "unchecked",
                            },
                            {
                                "id": "E07-P3-S02",
                                "episode": 7,
                                "pass_number": 3,
                                "segment": 2,
                                "path": "video_only_retry_s01e07_rerun2/raw_speech_only/pass3/s01e07_seg02.srt",
                                "generated": True,
                                "exists": True,
                                "size": 100,
                                "quality_state": "unchecked",
                            },
                            {
                                "id": "E07-P3-S03",
                                "episode": 7,
                                "pass_number": 3,
                                "segment": 3,
                                "path": "video_only_retry_s01e07_rerun2/raw_speech_only/pass3/s01e07_seg03.srt",
                                "generated": True,
                                "exists": True,
                                "size": 100,
                                "quality_state": "unchecked",
                            },
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            original_state_path = sys.modules["gemini_ui_supervisor"].STATE_PATH
            sys.modules["gemini_ui_supervisor"].STATE_PATH = state_path
            try:
                pending = unresolved_items(
                    [
                        StateItem("E07-P3-S01", 7, 3, 1, "one", False, False, 0, "unchecked"),
                        StateItem("E07-P3-S02", 7, 3, 2, "two", False, False, 0, "unchecked"),
                        StateItem("E07-P3-S03", 7, 3, 3, "three", False, False, 0, "unchecked"),
                    ]
                )
            finally:
                sys.modules["gemini_ui_supervisor"].STATE_PATH = original_state_path

        self.assertEqual(pending, [])

    def test_recovery_issue_records_explicit_stop_signal(self) -> None:
        runtime = WorkerRuntime(
            label="windows-cmd-python",
            command_prefix=("cmd.exe", "/c", "python"),
            uses_windows_paths=True,
            opens_visible_window=True,
        )

        issue = recovery_issue(
            "worker_interrupted",
            "E06-P3-S07: worker_interrupted",
            [],
            runtime,
            explicit_stop=True,
        )

        self.assertTrue(issue["explicit_stop_requested"])

    def test_summarize_repeated_issue_collapses_non_pro_loop_categories(self) -> None:
        issue_history = [
            {
                "category": "supervisor_required",
                "category_family": "non_pro_worker_cycle",
                "batch": ["E12-P1-S03"],
                "batch_key": "E12-P1-S03",
                "explicit_stop_requested": False,
            },
            {
                "category": "automation_stalled",
                "category_family": "non_pro_worker_cycle",
                "batch": ["E12-P1-S03"],
                "batch_key": "E12-P1-S03",
                "explicit_stop_requested": False,
            },
            {
                "category": "worker_interrupted",
                "category_family": "non_pro_worker_cycle",
                "batch": ["E12-P1-S03"],
                "batch_key": "E12-P1-S03",
                "explicit_stop_requested": False,
            },
        ]
        issue = {
            "category": "worker_interrupted",
            "blocker": "E12-P1-S03: worker_interrupted",
            "batch": ["E12-P1-S03"],
            "runtime_strategy": "windows-cmd-python",
        }

        summary = summarize_repeated_issue(issue_history, issue)

        self.assertEqual(summary["category_family"], "non_pro_worker_cycle")
        self.assertEqual(summary["same_batch_family_streak"], 3)
        self.assertTrue(summary["repeat_loop_detected"])
        self.assertEqual(
            summary["raw_categories"],
            ["automation_stalled", "supervisor_required", "worker_interrupted"],
        )

    def test_request_recovery_decision_forces_wait_after_repeated_same_item_loop(self) -> None:
        class StubBridge:
            def wake_same_session(self, _prompt: str) -> str:
                return json.dumps(
                    {
                        "decision_version": 1,
                        "workflow_status": "continue",
                        "diagnosis": "Non-Pro worker interruption should retry immediately.",
                        "recovery_action": {
                            "kind": "retry_now",
                            "reason": "Immediate relaunch.",
                            "delay_seconds": 0,
                        },
                    }
                )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            state_path = temp_path / "same_session_supervisor_state.json"
            event_log_path = temp_path / "same_session_supervisor_events.jsonl"
            state_path.write_text(
                json.dumps(
                    {
                        "session_id": "current-session",
                        "codex_command": "codex",
                        "wake_count": 2,
                        "wake_history": [],
                        "issue_history": [
                            {
                                "at": "2026-03-26 19:10:00 +0900 KST",
                                "category": "supervisor_required",
                                "category_family": "non_pro_worker_cycle",
                                "batch": ["E12-P1-S03"],
                                "batch_key": "E12-P1-S03",
                                "explicit_stop_requested": False,
                            },
                            {
                                "at": "2026-03-26 19:11:00 +0900 KST",
                                "category": "automation_stalled",
                                "category_family": "non_pro_worker_cycle",
                                "batch": ["E12-P1-S03"],
                                "batch_key": "E12-P1-S03",
                                "explicit_stop_requested": False,
                            },
                        ],
                        "last_decision": None,
                        "last_wake": None,
                        "last_issue": None,
                        "last_issue_repeat_analysis": None,
                        "session_rebind_history": [],
                        "created_at": "2026-03-25 03:13:17 +0900 KST",
                        "updated_at": "2026-03-26 19:11:00 +0900 KST",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            store = RecoveryStateStore(
                state_path=state_path,
                event_log_path=event_log_path,
                session_id="current-session",
                codex_command="codex",
            )
            issue = {
                "category": "worker_interrupted",
                "blocker": "E12-P1-S03: worker_interrupted",
                "batch": ["E12-P1-S03"],
                "runtime_strategy": "windows-cmd-python",
                "explicit_stop_requested": False,
            }

            decision = request_recovery_decision(
                bridge=StubBridge(),
                store=store,
                issue=issue,
                rollout_snapshot={"summary": {"remaining": 1}, "blocker": issue["blocker"]},
                sleep_fn=lambda _seconds: None,
            )
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            event_log_text = event_log_path.read_text(encoding="utf-8")

        self.assertEqual(decision.recovery_action.kind, "wait")
        self.assertGreaterEqual(decision.recovery_action.delay_seconds, 60)
        self.assertTrue(persisted["last_issue_repeat_analysis"]["repeat_loop_detected"])
        self.assertEqual(persisted["last_issue_repeat_analysis"]["same_batch_family_streak"], 3)
        self.assertIn("assistant_decision_loop_guard_override", event_log_text)

    def test_recovery_state_store_rebinds_stale_session_to_current_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            state_path = temp_path / "same_session_supervisor_state.json"
            event_log_path = temp_path / "same_session_supervisor_events.jsonl"
            state_path.write_text(
                json.dumps(
                    {
                        "session_id": "old-session",
                        "codex_command": "codex",
                        "wake_count": 3,
                        "wake_history": [{"wake_attempt": 3, "status": "succeeded"}],
                        "last_decision": {"decision_version": 1},
                        "last_wake": {"wake_attempt": 3},
                        "last_issue": {"category": "pro_limit", "blocker": "E07-P2-S07"},
                        "created_at": "2026-03-25 03:13:17 +0900 KST",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            store = RecoveryStateStore(
                state_path=state_path,
                event_log_path=event_log_path,
                session_id="current-session",
                codex_command="codex",
            )

            rebound = store.load_or_initialize()
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            event_log_exists = event_log_path.exists()

        self.assertEqual(rebound["session_id"], "current-session")
        self.assertEqual(rebound["wake_count"], 0)
        self.assertEqual(rebound["wake_history"], [])
        self.assertIsNone(rebound["last_issue"])
        self.assertEqual(rebound["rebound_from_session_id"], "old-session")
        self.assertEqual(rebound["created_at"], "2026-03-25 03:13:17 +0900 KST")
        self.assertEqual(rebound["session_rebind_history"][0]["from_session_id"], "old-session")
        self.assertEqual(rebound["session_rebind_history"][0]["to_session_id"], "current-session")
        self.assertEqual(
            rebound["session_rebind_history"][0]["previous_last_issue"],
            {"category": "pro_limit", "blocker": "E07-P2-S07"},
        )
        self.assertEqual(persisted["session_id"], "current-session")
        self.assertEqual(persisted["session_rebind_history"][0]["from_session_id"], "old-session")
        self.assertTrue(event_log_exists)

    def test_rollout_is_complete_requires_generation_acceptance_and_finals(self) -> None:
        data = {
            "summary": {
                "remaining": 0,
                "segment_groups_accepted": 84,
                "segment_groups_total": 84,
                "segment_groups_hard_blocked": 0,
                "episode_finals_complete": 11,
                "episode_finals_total": 12,
            }
        }

        self.assertFalse(rollout_is_complete(data))

        data["summary"]["episode_finals_complete"] = 12
        self.assertTrue(rollout_is_complete(data))

        data["summary"]["segment_groups_accepted"] = 83
        self.assertFalse(rollout_is_complete(data))


if __name__ == "__main__":
    unittest.main()
