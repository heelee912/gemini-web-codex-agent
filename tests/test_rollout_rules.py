from __future__ import annotations

import json
import sys
import tempfile
import types
import unittest
from unittest import mock
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
sys.modules.setdefault("pyautogui", types.SimpleNamespace(FAILSAFE=False, PAUSE=0.0))
sys.modules.setdefault("pygetwindow", types.SimpleNamespace(getAllWindows=lambda: []))
sys.modules.setdefault("pyperclip", types.SimpleNamespace(copy=lambda _text: None, paste=lambda: ""))
sys.modules.setdefault("uiautomation", types.SimpleNamespace(SendKeys=lambda *_args, **_kwargs: None))

import build_state as build_state_module  # noqa: E402
import gemini_ui_supervisor as supervisor_module  # noqa: E402
from runtime_config import episode_workspace_dir, reference_dir, worker_window_title  # noqa: E402
from build_state import build_segment_groups  # noqa: E402
from gemini_ui_supervisor import (  # noqa: E402
    BatchCommandError,
    HARD_BLOCKED_QUALITY_STATE,
    DEFAULT_INTERRUPTED_RECOVERY_DELAY_SECONDS,
    WorkerRuntime,
    actionable_state_items,
    apply_same_session_recovery,
    build_visible_worker_launch_payload,
    browser_probe_ready_for_worker,
    browser_probe_has_pro_constraint,
    browser_probe_requires_wait,
    browser_probe_wait_details,
    classify_worker_failure,
    coerce_process_output,
    distinct_failed_hard_block_paths,
    finalize_pending_recovery_actions,
    maybe_refresh_due_supervisor_wait,
    main,
    mark_hard_blocked_items,
    next_scheduled_recovery,
    recovery_issue,
    requires_same_session_recovery,
    rollout_snapshot,
    rollout_is_complete,
    schedule_worker_interruption_recovery,
    to_windows_path,
    unresolved_items,
)
from gemini_ui_batch_shell import (  # noqa: E402
    GeminiShellBatchRunner,
    extract_code_block,
    looks_like_copy_button,
    requires_supervisor,
)
from next_batch import StateItem, choose_next_batch  # noqa: E402
from same_session_recovery import (  # noqa: E402
    RecoveryAction,
    RecoveryDecision,
    RecoveryStateStore,
    request_recovery_decision,
    summarize_repeated_issue,
)
from teogonia_rollout import (  # noqa: E402
    ACCEPTANCE_EVIDENCE_VERSION,
    SegmentAcceptanceDecision,
    SubtitleCue,
    evaluate_segment_group,
    render_srt,
)


def write_cues(path: Path, cues: list[SubtitleCue]) -> None:
    path.write_text(render_srt(cues), encoding="utf-8")


def write_srt(path: Path, text: str) -> None:
    write_cues(
        path,
        [
            SubtitleCue(
                start_ms=0,
                end_ms=900,
                text=text,
            )
        ],
    )


def write_episode_fixture(root_dir: Path, *, episode: int = 1) -> Path:
    work_dir = episode_workspace_dir(root_dir, episode)
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


def write_whisper_fixture(
    whisper_dir: Path,
    *,
    episode: int = 1,
    text: str = "こんにちは 世界",
    cues: list[SubtitleCue] | None = None,
) -> None:
    whisper_dir.mkdir(parents=True, exist_ok=True)
    write_cues(
        whisper_dir / f"episode-{episode:02d}.srt",
        cues
        or [
            SubtitleCue(
                start_ms=0,
                end_ms=900,
                text=text,
            )
        ],
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
        self.assertEqual(decision.selection_basis, "whisper_line_alignment")

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
        self.assertEqual(decision.selection_basis, "gemini_line_alignment_preserved")
        self.assertEqual(decision.whisper_review["status"], "garbled")

    def test_evaluate_segment_group_accepts_split_sentence_across_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as root_tmp, tempfile.TemporaryDirectory() as whisper_tmp:
            root_dir = Path(root_tmp)
            whisper_dir = Path(whisper_tmp)
            work_dir = write_episode_fixture(root_dir)
            write_whisper_fixture(
                whisper_dir,
                cues=[
                    SubtitleCue(start_ms=0, end_ms=450, text="こんにちは 世界"),
                    SubtitleCue(start_ms=450, end_ms=900, text="今日は本当にいい天気ですね 散歩に行きましょう"),
                ],
            )
            write_cues(
                work_dir / "raw_speech_only" / "pass1" / "s01e01_seg01.srt",
                [SubtitleCue(start_ms=0, end_ms=900, text="こんにちは 世界 今日は本当にいい天気ですね 散歩に行きましょう")],
            )
            write_cues(
                work_dir / "raw_speech_only" / "pass2" / "s01e01_seg01.srt",
                [
                    SubtitleCue(start_ms=0, end_ms=450, text="こんにちは 世界"),
                    SubtitleCue(start_ms=450, end_ms=900, text="今日は本当にいい天気ですね 散歩に行きましょう"),
                ],
            )
            write_srt(work_dir / "raw_speech_only" / "pass3" / "s01e01_seg01.srt", "まったく ちがう")

            decision = evaluate_segment_group(root_dir, 1, 1, whisper_dir=whisper_dir)

        self.assertEqual(decision.status, "accepted")
        self.assertEqual(decision.quality_state, "accepted_consistent")
        self.assertEqual(decision.selection_basis, "whisper_line_alignment")
        self.assertEqual(decision.selected_pass_number, 2)
        self.assertGreaterEqual(
            decision.pairwise_line_alignment["1-2"]["avg_score"],
            0.79,
        )

    def test_evaluate_segment_group_uses_project_whisper_directory_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as root_tmp:
            root_dir = Path(root_tmp)
            work_dir = write_episode_fixture(root_dir)
            write_whisper_fixture(reference_dir(root_dir), text="こんにちは 世界")
            for pass_number in (1, 2, 3):
                write_srt(
                    work_dir / "raw_speech_only" / f"pass{pass_number}" / "s01e01_seg01.srt",
                    "こんにちは 世界",
                )

            decision = evaluate_segment_group(root_dir, 1, 1)

        self.assertEqual(decision.status, "accepted")
        self.assertEqual(decision.whisper_excerpt, "こんにちは 世界")

    def test_evaluate_segment_group_rejects_empty_pass_even_if_two_passes_match(self) -> None:
        with tempfile.TemporaryDirectory() as root_tmp, tempfile.TemporaryDirectory() as whisper_tmp:
            root_dir = Path(root_tmp)
            whisper_dir = Path(whisper_tmp)
            work_dir = write_episode_fixture(root_dir)
            write_whisper_fixture(whisper_dir, text="こんにちは 世界")
            write_srt(work_dir / "raw_speech_only" / "pass1" / "s01e01_seg01.srt", "こんにちは 世界")
            write_srt(work_dir / "raw_speech_only" / "pass2" / "s01e01_seg01.srt", "こんにちは 世界")
            (work_dir / "raw_speech_only" / "pass3" / "s01e01_seg01.srt").write_text("not an srt", encoding="utf-8")

            decision = evaluate_segment_group(root_dir, 1, 1, whisper_dir=whisper_dir)

        self.assertEqual(decision.status, "needs_regeneration")
        self.assertEqual(decision.retry_pass_number, 3)
        self.assertEqual(decision.selection_basis, "empty_generation")

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
                r"C:\Runtime\python.exe",
                r"D:\Workspace\subtitle_rollout\tools\gemini_ui_batch_shell.py",
                "--episode",
                "8",
            ]
        )

        self.assertIn("powershell.exe", payload)
        self.assertIn("-EncodedCommand", payload)

    def test_visible_worker_runtime_builds_direct_powershell_command(self) -> None:
        runtime = WorkerRuntime(
            label="windows-cmd-python",
            command_prefix=("cmd.exe", "/c", r"C:\Runtime\python.exe"),
            uses_windows_paths=True,
            opens_visible_window=True,
        )

        command = runtime.build_command(Path("/mnt/d/workspace/subtitle_rollout/tools/gemini_ui_batch_shell.py"), "--episode", "8")

        self.assertEqual(command[0], "powershell.exe")
        self.assertIn("-EncodedCommand", command)
        self.assertNotEqual(command[:2], ["cmd.exe", "/c"])

    def test_browser_window_filter_excludes_worker_console_title(self) -> None:
        class FakeWindow:
            def __init__(self, title: str) -> None:
                self.title = title

        self.assertFalse(GeminiShellBatchRunner.looks_like_browser_window(FakeWindow(worker_window_title())))
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

    def test_extract_code_block_skips_plain_preamble_before_fenced_srt(self) -> None:
        raw = "설명입니다.\n```srt\n1\n00:00:01,000 --> 00:00:02,000\n안녕하세요\n```"

        extracted = extract_code_block(raw)

        self.assertEqual(extracted, "1\n00:00:01,000 --> 00:00:02,000\n안녕하세요")

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

    def test_rebuild_state_clears_stale_accepted_item_and_preserves_supervisor_wait(self) -> None:
        with tempfile.TemporaryDirectory() as root_tmp:
            root_dir = Path(root_tmp)
            write_episode_fixture(root_dir, episode=1)
            state_path = root_dir / ".codex" / "state.json"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "id": "E01-P1-S01",
                                "accepted": True,
                                "quality_state": "accepted_unanimous",
                                "supervisor_phase": "accepted",
                            }
                        ],
                        "supervisor_wait": {
                            "kind": "wait",
                            "cause": "pro_limit",
                            "detail": "Gemini Pro quota wait",
                            "retry_not_before": "2099-03-26T12:00:00+09:00",
                            "batch": ["E01-P1-S01"],
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            original_root_dir = build_state_module.ROOT_DIR
            build_state_module.ROOT_DIR = root_dir
            try:
                rebuilt = build_state_module.rebuild_state(state_path)
            finally:
                build_state_module.ROOT_DIR = original_root_dir

        item = next(entry for entry in rebuilt["items"] if entry["id"] == "E01-P1-S01")
        self.assertFalse(item["accepted"])
        self.assertEqual(item["quality_state"], "unchecked")
        self.assertEqual(item["supervisor_phase"], "generation_pending")
        self.assertEqual(rebuilt["supervisor_wait"]["cause"], "pro_limit")

    def test_actionable_state_items_skip_future_wait_and_global_wait_wins(self) -> None:
        future_wait = "2099-03-26T12:00:00+09:00"
        data = {
            "items": [
                {
                    "id": "E01-P1-S01",
                    "episode": 1,
                    "pass_number": 1,
                    "segment": 1,
                    "path": "one",
                    "generated": False,
                    "exists": False,
                    "size": 0,
                    "quality_state": "unchecked",
                    "pending_recovery_action": {
                        "kind": "wait",
                        "path": "worker_interrupt_cooldown",
                        "detail": "cooldown",
                        "cause": "worker_interrupted",
                        "retry_not_before": future_wait,
                    },
                },
                {
                    "id": "E01-P1-S02",
                    "episode": 1,
                    "pass_number": 1,
                    "segment": 2,
                    "path": "two",
                    "generated": False,
                    "exists": False,
                    "size": 0,
                    "quality_state": "unchecked",
                },
            ],
            "supervisor_wait": {
                "kind": "wait",
                "cause": "pro_limit",
                "detail": "Gemini Pro quota wait",
                "retry_not_before": future_wait,
                "batch": ["E01-P1-S01"],
            },
        }

        actionable = actionable_state_items(data)
        scheduled = next_scheduled_recovery(data)

        self.assertEqual([item.id for item in actionable], ["E01-P1-S02"])
        self.assertEqual(scheduled["scope"], "global")
        self.assertEqual(scheduled["cause"], "pro_limit")
        self.assertTrue(browser_probe_has_pro_constraint({"status": "pro_mode_required"}))

    def test_finalize_pending_recovery_actions_skips_future_waits(self) -> None:
        state = {
            "items": [
                {
                    "id": "E01-P1-S01",
                    "episode": 1,
                    "segment": 1,
                    "pass_number": 1,
                    "generated": False,
                    "quality_state": "unchecked",
                    "pending_recovery_action": {
                        "kind": "wait",
                        "path": "same_session_wait",
                        "detail": "Gemini Pro quota wait",
                        "cause": "pro_limit",
                        "retry_not_before": "2099-03-26T12:00:00+09:00",
                    },
                }
            ]
        }
        batch = [
            StateItem(
                id="E01-P1-S01",
                episode=1,
                pass_number=1,
                segment=1,
                path="one",
                generated=False,
                exists=False,
                size=0,
                quality_state="unchecked",
            )
        ]

        changed = finalize_pending_recovery_actions(state, batch)

        self.assertFalse(changed)
        self.assertIn("pending_recovery_action", state["items"][0])
        self.assertNotIn("recovery_attempts", state["items"][0])

    def test_schedule_worker_interruption_recovery_uses_one_minute_cooldown(self) -> None:
        batch = [
            StateItem(
                id="E12-P1-S03",
                episode=12,
                pass_number=1,
                segment=3,
                path="three",
                generated=False,
                exists=False,
                size=0,
                quality_state="unchecked",
            )
        ]

        plan = schedule_worker_interruption_recovery(batch, "E12-P1-S03: worker_interrupted")

        self.assertEqual(plan.action_kind, "wait")
        self.assertEqual(plan.wait_scope, "batch")
        self.assertEqual(plan.supervisor_phase, "waiting_recovery")
        self.assertEqual(plan.wait_seconds, DEFAULT_INTERRUPTED_RECOVERY_DELAY_SECONDS)

    def test_apply_same_session_recovery_uses_browser_probe_for_pro_wait(self) -> None:
        runtime = WorkerRuntime(
            label="windows-cmd-python",
            command_prefix=("cmd.exe", "/c", "python"),
            uses_windows_paths=True,
            opens_visible_window=True,
        )
        batch = [
            StateItem(
                id="E08-P2-S03",
                episode=8,
                pass_number=2,
                segment=3,
                path="three",
                generated=False,
                exists=False,
                size=0,
                quality_state="unchecked",
            )
        ]
        decision = RecoveryDecision(
            decision_version=1,
            workflow_status="continue",
            diagnosis="Gemini Pro quota is still active.",
            recovery_action=RecoveryAction(kind="wait", reason="Wait for Pro quota reset.", delay_seconds=30),
        )

        with mock.patch.object(supervisor_module, "load_state", return_value={"summary": {"remaining": 1}, "items": [], "segment_groups": []}), \
             mock.patch.object(
                 supervisor_module,
                 "probe_browser_state",
                 return_value={
                     "status": "pro_limit",
                     "wait_seconds": 7200,
                     "resume_at_iso": "2099-03-26T12:00:00+09:00",
                 },
             ), \
             mock.patch.object(supervisor_module, "request_recovery_decision", return_value=decision):
            plan = apply_same_session_recovery(
                bridge=object(),
                store=object(),
                category="pro_limit",
                blocker="E08-P2-S03: pro_limit",
                batch=batch,
                runtime=runtime,
                resume_hint={"wait_seconds": 120},
            )

        self.assertEqual(plan.action_kind, "wait")
        self.assertEqual(plan.wait_scope, "global")
        self.assertEqual(plan.wait_seconds, 7200)
        self.assertEqual(plan.resume_at_iso, "2099-03-26T12:00:00+09:00")
        self.assertEqual(plan.supervisor_phase, "waiting_quota")

    def test_apply_same_session_recovery_retries_immediately_when_browser_probe_is_ready(self) -> None:
        runtime = WorkerRuntime(
            label="windows-cmd-python",
            command_prefix=("cmd.exe", "/c", "python"),
            uses_windows_paths=True,
            opens_visible_window=True,
        )

        with mock.patch.object(supervisor_module, "load_state", return_value={"summary": {"remaining": 1}, "items": [], "segment_groups": []}), \
             mock.patch.object(supervisor_module, "probe_browser_state", return_value={"status": "ready"}), \
             mock.patch.object(supervisor_module, "request_recovery_decision") as request_mock:
            plan = apply_same_session_recovery(
                bridge=object(),
                store=object(),
                category="supervisor_required",
                blocker="E08-P2-S03: supervisor_required",
                batch=[],
                runtime=runtime,
            )

        self.assertEqual(plan.action_kind, "retry_now")
        self.assertEqual(plan.path, "supervisor_required")
        request_mock.assert_not_called()

    def test_maybe_refresh_due_supervisor_wait_reschedules_when_browser_still_pro_limited(self) -> None:
        runtime = WorkerRuntime(
            label="windows-cmd-python",
            command_prefix=("cmd.exe", "/c", "python"),
            uses_windows_paths=True,
            opens_visible_window=True,
        )
        batch = [
            StateItem(
                id="E09-P1-S01",
                episode=9,
                pass_number=1,
                segment=1,
                path="one",
                generated=False,
                exists=False,
                size=0,
                quality_state="unchecked",
            )
        ]
        state = {
            "supervisor_wait": {
                "kind": "wait",
                "cause": "pro_limit",
                "detail": "Gemini Pro quota wait",
                "retry_not_before": "2000-01-01T00:00:00+09:00",
                "batch": ["E09-P1-S01"],
            }
        }
        captured: list[object] = []

        with mock.patch.object(supervisor_module, "load_state_for_edit", return_value=state), \
             mock.patch.object(
                 supervisor_module,
                 "probe_browser_state",
                 return_value={"status": "pro_limit", "wait_seconds": 1800, "resume_at_iso": "2099-03-26T14:00:00+09:00"},
             ), \
             mock.patch.object(supervisor_module, "persist_recovery_plan", side_effect=lambda _batch, plan: captured.append(plan)), \
             mock.patch.object(supervisor_module, "write_rollout_result"), \
             mock.patch.object(supervisor_module, "log_progress"), \
             mock.patch.object(supervisor_module, "log_plan"):
            refreshed = maybe_refresh_due_supervisor_wait(runtime, default_batch=batch)

        self.assertTrue(refreshed)
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].wait_scope, "global")
        self.assertEqual(captured[0].wait_seconds, 1800)
        self.assertEqual(captured[0].supervisor_phase, "waiting_quota")

    def test_maybe_refresh_due_supervisor_wait_clears_due_wait_when_browser_is_ready(self) -> None:
        runtime = WorkerRuntime(
            label="windows-cmd-python",
            command_prefix=("cmd.exe", "/c", "python"),
            uses_windows_paths=True,
            opens_visible_window=True,
        )
        state = {
            "supervisor_wait": {
                "kind": "wait",
                "cause": "pro_limit",
                "detail": "Gemini Pro quota wait",
                "retry_not_before": "2000-01-01T00:00:00+09:00",
                "batch": ["E09-P1-S01"],
            }
        }

        with mock.patch.object(supervisor_module, "load_state_for_edit", return_value=state), \
             mock.patch.object(supervisor_module, "probe_browser_state", return_value={"status": "ready"}), \
             mock.patch.object(supervisor_module, "save_state_for_edit") as save_mock, \
             mock.patch.object(supervisor_module, "rebuild_state"), \
             mock.patch.object(supervisor_module, "log_progress"):
            refreshed = maybe_refresh_due_supervisor_wait(runtime)

        self.assertFalse(refreshed)
        self.assertNotIn("supervisor_wait", state)
        save_mock.assert_called_once()

    def test_to_windows_path_falls_back_when_wslpath_is_missing(self) -> None:
        with mock.patch("gemini_ui_supervisor.subprocess.run", side_effect=FileNotFoundError()):
            converted = to_windows_path(Path("/mnt/d/workspace/subtitle_rollout/tools/gemini_ui_batch_shell.py"))

        self.assertEqual(converted, "/mnt/d/workspace/subtitle_rollout/tools/gemini_ui_batch_shell.py")

    def test_run_batch_ignores_missing_powershell_for_screenshots(self) -> None:
        runtime = WorkerRuntime(
            label="env-python",
            command_prefix=("python",),
            uses_windows_paths=False,
        )
        batch = [
            StateItem(
                id="E01-P1-S01",
                episode=1,
                pass_number=1,
                segment=1,
                path="one",
                generated=False,
                exists=False,
                size=0,
                quality_state="unchecked",
            )
        ]
        completed = types.SimpleNamespace(returncode=0, stdout="", stderr="")

        def fake_run(command: list[str], **_kwargs: object) -> object:
            if command and command[0] == "powershell.exe":
                raise FileNotFoundError("powershell.exe")
            return completed

        with tempfile.TemporaryDirectory() as temp_dir, \
             mock.patch.object(supervisor_module, "SCREENSHOT_DIR", Path(temp_dir)), \
             mock.patch.object(supervisor_module.subprocess, "run", side_effect=fake_run):
            result = supervisor_module.run_batch(runtime, batch)

        self.assertEqual(result.returncode, 0)

    def test_main_pauses_when_worker_is_interrupted_with_explicit_stop(self) -> None:
        runtime = WorkerRuntime(
            label="windows-cmd-python",
            command_prefix=("cmd.exe", "/c", "python"),
            uses_windows_paths=True,
            opens_visible_window=True,
        )
        batch = [
            StateItem(
                id="E01-P1-S01",
                episode=1,
                pass_number=1,
                segment=1,
                path="one",
                generated=False,
                exists=False,
                size=0,
                quality_state="unchecked",
            )
        ]
        state = {
            "summary": {
                "remaining": 1,
                "segment_groups_accepted": 0,
                "segment_groups_total": 1,
                "segment_groups_hard_blocked": 0,
                "episode_finals_complete": 0,
                "episode_finals_total": 1,
            },
            "items": [
                {
                    "id": "E01-P1-S01",
                    "episode": 1,
                    "pass_number": 1,
                    "segment": 1,
                    "path": "one",
                    "generated": False,
                    "exists": False,
                    "size": 0,
                    "quality_state": "unchecked",
                }
            ],
            "segment_groups": [],
        }
        worker_error = BatchCommandError(
            "worker exited with 130 (worker_interrupted)",
            command=["python"],
            returncode=130,
            stdout="",
            stderr="",
            category="worker_interrupted",
            resume_hint=None,
        )

        with mock.patch.object(supervisor_module, "parse_args", return_value=types.SimpleNamespace(codex_command="codex")), \
             mock.patch.object(supervisor_module, "resolve_session_anchor", return_value="session-anchor"), \
             mock.patch.object(supervisor_module, "CodexResumeBridge"), \
             mock.patch.object(supervisor_module, "RecoveryStateStore"), \
             mock.patch.object(supervisor_module, "resolve_worker_runtime", return_value=(runtime, [])), \
             mock.patch.object(supervisor_module, "rebuild_state", side_effect=[state, state, state, state]), \
             mock.patch.object(supervisor_module, "process_segment_acceptance", return_value=False), \
             mock.patch.object(supervisor_module, "process_episode_merges", return_value=False), \
             mock.patch.object(supervisor_module, "maybe_refresh_due_supervisor_wait", return_value=False), \
             mock.patch.object(supervisor_module, "load_state", return_value=state), \
             mock.patch.object(supervisor_module, "choose_next_batch", return_value=batch), \
             mock.patch.object(supervisor_module, "run_batch", side_effect=worker_error), \
             mock.patch.object(supervisor_module, "unresolved_items", return_value=batch), \
             mock.patch.object(supervisor_module, "explicit_stop_requested", return_value=True), \
             mock.patch.object(supervisor_module, "persist_user_pause") as pause_mock, \
             mock.patch.object(supervisor_module, "write_rollout_result") as write_mock:
            result = main()

        self.assertEqual(result, 0)
        pause_mock.assert_called_once()
        self.assertEqual(write_mock.call_args.args[0], "PAUSED_BY_USER")

    def test_main_enters_scheduled_wait_when_no_batch_is_runnable(self) -> None:
        runtime = WorkerRuntime(
            label="windows-cmd-python",
            command_prefix=("cmd.exe", "/c", "python"),
            uses_windows_paths=True,
            opens_visible_window=True,
        )
        state = {
            "summary": {
                "remaining": 1,
                "segment_groups_accepted": 0,
                "segment_groups_total": 1,
                "segment_groups_hard_blocked": 0,
                "episode_finals_complete": 0,
                "episode_finals_total": 1,
            },
            "items": [],
            "segment_groups": [],
        }

        class StopLoop(RuntimeError):
            pass

        with mock.patch.object(supervisor_module, "parse_args", return_value=types.SimpleNamespace(codex_command="codex")), \
             mock.patch.object(supervisor_module, "resolve_session_anchor", return_value="session-anchor"), \
             mock.patch.object(supervisor_module, "CodexResumeBridge"), \
             mock.patch.object(supervisor_module, "RecoveryStateStore"), \
             mock.patch.object(supervisor_module, "resolve_worker_runtime", return_value=(runtime, [])), \
             mock.patch.object(supervisor_module, "rebuild_state", side_effect=[state, state, state]), \
             mock.patch.object(supervisor_module, "process_segment_acceptance", return_value=False), \
             mock.patch.object(supervisor_module, "process_episode_merges", return_value=False), \
             mock.patch.object(supervisor_module, "maybe_refresh_due_supervisor_wait", return_value=False), \
             mock.patch.object(supervisor_module, "load_state", return_value=state), \
             mock.patch.object(supervisor_module, "choose_next_batch", return_value=[]), \
             mock.patch.object(
                 supervisor_module,
                 "next_scheduled_recovery",
                 return_value={
                     "scope": "global",
                     "wait_seconds": 7,
                     "resume_at_iso": "2099-03-26T15:00:00+09:00",
                     "detail": "Gemini Pro quota wait",
                     "cause": "pro_limit",
                     "batch": ["E01-P1-S01"],
                 },
             ), \
             mock.patch.object(supervisor_module, "write_rollout_result") as write_mock, \
             mock.patch.object(supervisor_module.time, "sleep", side_effect=StopLoop()):
            with self.assertRaises(StopLoop):
                main()

        self.assertEqual(write_mock.call_args.args[0], "WAITING_FOR_RECOVERY")

    def test_main_reaches_same_session_recovery_after_strict_retry_failure(self) -> None:
        runtime = WorkerRuntime(
            label="windows-cmd-python",
            command_prefix=("cmd.exe", "/c", "python"),
            uses_windows_paths=True,
            opens_visible_window=True,
        )
        batch = [
            StateItem(
                id="E01-P1-S01",
                episode=1,
                pass_number=1,
                segment=1,
                path="one",
                generated=False,
                exists=False,
                size=0,
                quality_state="unchecked",
            )
        ]
        state = {
            "summary": {
                "remaining": 1,
                "segment_groups_accepted": 0,
                "segment_groups_total": 1,
                "segment_groups_hard_blocked": 0,
                "episode_finals_complete": 0,
                "episode_finals_total": 1,
            },
            "items": [
                {
                    "id": "E01-P1-S01",
                    "episode": 1,
                    "pass_number": 1,
                    "segment": 1,
                    "path": "one",
                    "generated": False,
                    "exists": False,
                    "size": 0,
                    "quality_state": "unchecked",
                }
            ],
            "segment_groups": [],
        }
        first_error = BatchCommandError(
            "worker exited with 1 (automation_failed)",
            command=["python"],
            returncode=1,
            stdout="",
            stderr="",
            category="automation_failed",
            resume_hint=None,
        )
        strict_error = BatchCommandError(
            "worker exited with 86 (supervisor_required)",
            command=["python"],
            returncode=86,
            stdout="",
            stderr="",
            category="supervisor_required",
            resume_hint=None,
        )

        class StopLoop(RuntimeError):
            pass

        with mock.patch.object(supervisor_module, "parse_args", return_value=types.SimpleNamespace(codex_command="codex")), \
             mock.patch.object(supervisor_module, "resolve_session_anchor", return_value="session-anchor"), \
             mock.patch.object(supervisor_module, "CodexResumeBridge"), \
             mock.patch.object(supervisor_module, "RecoveryStateStore"), \
             mock.patch.object(supervisor_module, "resolve_worker_runtime", return_value=(runtime, [])), \
             mock.patch.object(supervisor_module, "rebuild_state", side_effect=[state, state, state, state]), \
             mock.patch.object(supervisor_module, "process_segment_acceptance", return_value=False), \
             mock.patch.object(supervisor_module, "process_episode_merges", return_value=False), \
             mock.patch.object(supervisor_module, "maybe_refresh_due_supervisor_wait", return_value=False), \
             mock.patch.object(supervisor_module, "load_state", return_value=state), \
             mock.patch.object(supervisor_module, "choose_next_batch", return_value=batch), \
             mock.patch.object(supervisor_module, "run_batch", side_effect=[first_error, strict_error]), \
             mock.patch.object(supervisor_module, "unresolved_items", return_value=batch), \
             mock.patch.object(supervisor_module, "record_failed_recovery_path", return_value=[]), \
             mock.patch.object(supervisor_module, "apply_same_session_recovery", side_effect=StopLoop()), \
             mock.patch.object(supervisor_module, "write_rollout_result"):
            with self.assertRaises(StopLoop):
                main()

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
                                "path": "episode-07/raw_speech_only/pass3/s01e07_seg01.srt",
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
                                "path": "episode-07/raw_speech_only/pass3/s01e07_seg02.srt",
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
                                "path": "episode-07/raw_speech_only/pass3/s01e07_seg03.srt",
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

    def test_process_segment_acceptance_reevaluates_old_acceptance_version(self) -> None:
        state = {
            "items": [
                {
                    "id": "E01-P1-S01",
                    "episode": 1,
                    "pass_number": 1,
                    "segment": 1,
                    "generated": True,
                    "accepted": True,
                    "quality_state": "accepted_consistent",
                    "whisper_evidence": {"acceptance_version": 1},
                    "supervisor_phase": "accepted",
                },
                {
                    "id": "E01-P2-S01",
                    "episode": 1,
                    "pass_number": 2,
                    "segment": 1,
                    "generated": True,
                    "accepted": False,
                    "quality_state": "candidate",
                    "supervisor_phase": "generated",
                },
                {
                    "id": "E01-P3-S01",
                    "episode": 1,
                    "pass_number": 3,
                    "segment": 1,
                    "generated": True,
                    "accepted": False,
                    "quality_state": "candidate",
                    "supervisor_phase": "generated",
                },
            ],
            "segment_groups": [
                {
                    "episode": 1,
                    "segment": 1,
                    "has_all_passes": True,
                    "hard_blocked": False,
                    "accepted": True,
                    "needs_regeneration": False,
                }
            ],
        }
        decision = SegmentAcceptanceDecision(
            episode=1,
            segment=1,
            status="accepted",
            selected_pass_number=2,
            retry_pass_number=None,
            reason="re-evaluated with human-like line alignment evidence",
            quality_state="accepted_consistent",
            pairwise_similarity={"1-2": 0.88},
            whisper_similarity={"1": 0.81, "2": 0.93, "3": 0.12},
            pairwise_line_alignment={"1-2": {"avg_score": 0.84, "coverage_ratio": 1.0}},
            whisper_line_alignment={"1": {"avg_score": 0.71}, "2": {"avg_score": 0.94}, "3": {"avg_score": 0.11}},
            whisper_excerpt="こんにちは 世界",
            selected_pair=(1, 2),
            selection_basis="whisper_line_alignment",
            whisper_review={"status": "usable"},
        )

        with mock.patch.object(supervisor_module, "load_state_for_edit", return_value=state), \
             mock.patch.object(supervisor_module, "save_state_for_edit") as save_state, \
             mock.patch.object(supervisor_module, "rebuild_state") as rebuild_state, \
             mock.patch.object(supervisor_module, "evaluate_segment_group", return_value=decision) as evaluate:
            changed = supervisor_module.process_segment_acceptance("session-anchor")

        self.assertTrue(changed)
        evaluate.assert_called_once_with(supervisor_module.ROOT_DIR, 1, 1)
        self.assertTrue(state["items"][1]["accepted"])
        self.assertFalse(state["items"][0]["accepted"])
        self.assertEqual(
            state["items"][1]["whisper_evidence"]["acceptance_version"],
            ACCEPTANCE_EVIDENCE_VERSION,
        )
        save_state.assert_called_once()
        rebuild_state.assert_called_once()


if __name__ == "__main__":
    unittest.main()
