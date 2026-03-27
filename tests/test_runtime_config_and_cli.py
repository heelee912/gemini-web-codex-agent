from __future__ import annotations

import os
import sys
import tempfile
import types
import unittest
import json
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

import build_state  # noqa: E402
import rollout_cli  # noqa: E402
import runtime_config as runtime_config_module  # noqa: E402
from runtime_config import (  # noqa: E402
    default_env_file,
    discover_episode_numbers,
    load_env_file,
    parse_env_assignments,
    resolve_segment_file,
    workspace_root,
)


class RuntimeConfigAndCliTests(unittest.TestCase):
    def test_parse_env_assignments_ignores_comments_and_quotes(self) -> None:
        assignments = parse_env_assignments(
            """
            # comment
            ROLLOUT_PROJECT_LABEL="Subtitle Rollout"
            ROLLOUT_REFERENCE_DIR=reference_subtitles

            INVALID_LINE
            """
        )

        self.assertEqual(assignments["ROLLOUT_PROJECT_LABEL"], "Subtitle Rollout")
        self.assertEqual(assignments["ROLLOUT_REFERENCE_DIR"], "reference_subtitles")
        self.assertNotIn("INVALID_LINE", assignments)

    def test_load_env_file_respects_existing_values_without_override(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.dict(os.environ, {"ROLLOUT_PROJECT_LABEL": "Keep"}, clear=False):
            path = Path(temp_dir) / "rollout.env"
            path.write_text("ROLLOUT_PROJECT_LABEL=Replace\nROLLOUT_PROJECT_SLUG=subtitle_rollout\n", encoding="utf-8")

            assignments = load_env_file(path)
            self.assertEqual(os.environ["ROLLOUT_PROJECT_LABEL"], "Keep")
            self.assertEqual(os.environ["ROLLOUT_PROJECT_SLUG"], "subtitle_rollout")

        self.assertEqual(assignments["ROLLOUT_PROJECT_LABEL"], "Replace")

    def test_default_env_file_prefers_existing_rollout_env(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir)
            env_path = root_dir / "rollout.env"
            env_path.write_text("ROLLOUT_PROJECT_LABEL=Subtitle Rollout\n", encoding="utf-8")

            selected = default_env_file(root_dir)

        self.assertEqual(selected, env_path)

    def test_copy_config_template_writes_example_contents(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "rollout.env"

            created = rollout_cli.copy_config_template(output_path)

            self.assertEqual(created, output_path)
            text = output_path.read_text(encoding="utf-8")
            self.assertIn("ROLLOUT_PROJECT_LABEL", text)
            self.assertIn("ROLLOUT_REFERENCE_DIR", text)

    def test_run_supervisor_passes_thread_id_to_subprocess(self) -> None:
        completed = types.SimpleNamespace(returncode=0)
        with mock.patch.object(rollout_cli.subprocess, "run", return_value=completed) as run_mock:
            returncode = rollout_cli.run_supervisor("thread-123", codex_command="codex")

        self.assertEqual(returncode, 0)
        self.assertEqual(run_mock.call_args.kwargs["env"]["CODEX_THREAD_ID"], "thread-123")
        self.assertEqual(run_mock.call_args.kwargs["env"]["CODEX_COMMAND"], "codex")

    def test_setup_updates_workspace_root_and_reference_dir(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_file = Path(temp_dir) / "rollout.env"
            env_file.write_text("ROLLOUT_PROJECT_LABEL=Subtitle Rollout\n", encoding="utf-8")

            updated = rollout_cli.create_or_update_setup(
                env_file,
                workspace_root_value="D:/SubtitleRolloutData",
                reference_dir_value="D:/SubtitleRolloutData/reference_subtitles",
            )

            text = updated.read_text(encoding="utf-8")
            assignments = parse_env_assignments(text)

        self.assertEqual(updated, env_file)
        self.assertEqual(assignments["ROLLOUT_WORKSPACE_ROOT"], str(Path("D:/SubtitleRolloutData")))
        self.assertEqual(
            assignments["ROLLOUT_REFERENCE_DIR"],
            str(Path("D:/SubtitleRolloutData/reference_subtitles")),
        )

    def test_setup_translates_windows_absolute_paths_for_non_windows_hosts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            runtime_config_module,
            "running_on_windows_host",
            return_value=False,
        ), mock.patch.object(
            runtime_config_module.subprocess,
            "run",
            side_effect=FileNotFoundError("wslpath"),
        ):
            env_file = Path(temp_dir) / "rollout.env"
            env_file.write_text("ROLLOUT_PROJECT_LABEL=Subtitle Rollout\n", encoding="utf-8")

            updated = rollout_cli.create_or_update_setup(
                env_file,
                workspace_root_value="D:/SubtitleRolloutData",
                reference_dir_value="D:/SubtitleRolloutData/reference_subtitles",
            )

            assignments = parse_env_assignments(updated.read_text(encoding="utf-8"))

        self.assertEqual(Path(assignments["ROLLOUT_WORKSPACE_ROOT"]).as_posix(), "/mnt/d/SubtitleRolloutData")
        self.assertEqual(
            Path(assignments["ROLLOUT_REFERENCE_DIR"]).as_posix(),
            "/mnt/d/SubtitleRolloutData/reference_subtitles",
        )

    def test_workspace_root_can_point_outside_repo(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.dict(
            os.environ,
            {"ROLLOUT_WORKSPACE_ROOT": temp_dir},
            clear=False,
        ):
            self.assertEqual(workspace_root(ROOT), Path(temp_dir))

    def test_discover_episode_numbers_uses_generic_workspace_names(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.dict(
            os.environ,
            {"ROLLOUT_WORKSPACE_ROOT": temp_dir},
            clear=False,
        ):
            data_root = Path(temp_dir)
            alpha = data_root / "alpha"
            beta = data_root / "beta"
            alpha.mkdir(parents=True, exist_ok=True)
            beta.mkdir(parents=True, exist_ok=True)
            (alpha / "manifest.json").write_text(json.dumps({"segments": [{"durationMs": 1000}]}), encoding="utf-8")
            (beta / "manifest.json").write_text(json.dumps({"segments": [{"durationMs": 1000}]}), encoding="utf-8")

            self.assertEqual(discover_episode_numbers(ROOT), [1, 2])

    def test_resolve_segment_file_prefers_manifest_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.dict(
            os.environ,
            {"ROLLOUT_WORKSPACE_ROOT": temp_dir},
            clear=False,
        ):
            data_root = Path(temp_dir)
            work_dir = data_root / "episode-01"
            media_dir = data_root / "media"
            work_dir.mkdir(parents=True, exist_ok=True)
            media_dir.mkdir(parents=True, exist_ok=True)
            video_path = media_dir / "custom-scene.mov"
            video_path.write_bytes(b"video")
            (work_dir / "manifest.json").write_text(
                json.dumps({"segments": [{"durationMs": 1000, "path": "../media/custom-scene.mov"}]}),
                encoding="utf-8",
            )

            resolved = resolve_segment_file(ROOT, 1, 1)

        self.assertEqual(resolved, video_path.resolve())

    def test_rebuild_state_uses_external_workspace_root_and_dynamic_segments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, tempfile.TemporaryDirectory() as data_dir, mock.patch.dict(
            os.environ,
            {"ROLLOUT_WORKSPACE_ROOT": data_dir},
            clear=False,
        ):
            state_path = Path(temp_dir) / "state.json"
            work_dir = Path(data_dir) / "episode-03"
            work_dir.mkdir(parents=True, exist_ok=True)
            (work_dir / "manifest.json").write_text(
                json.dumps({"segments": [{"durationMs": 1000}, {"durationMs": 1000}]}),
                encoding="utf-8",
            )
            for pass_number in (1, 2, 3):
                output_dir = work_dir / "raw_speech_only" / f"pass{pass_number}"
                output_dir.mkdir(parents=True, exist_ok=True)
                (output_dir / f"s01e03_seg01.srt").write_text("", encoding="utf-8")
                (output_dir / f"s01e03_seg02.srt").write_text("", encoding="utf-8")

            state = build_state.rebuild_state(state_path)

        self.assertEqual(state["summary"]["total"], 6)
        self.assertEqual(state["summary"]["segment_groups_total"], 2)
        self.assertEqual(state["episode_outputs"][0]["episode"], 3)
        self.assertTrue(str(Path(data_dir)) in state["episode_outputs"][0]["final_path"])

    def test_launch_supervisor_window_avoids_bash_interpolation_for_thread_id(self) -> None:
        script_path = ROOT / "tools" / "launch_supervisor_window.cmd"
        script = script_path.read_text(encoding="utf-8")

        self.assertIn('wsl.exe env ^', script)
        self.assertIn('"CODEX_THREAD_ID=%CODEX_THREAD_ID%"', script)
        self.assertNotIn("bash -lc", script)


if __name__ == "__main__":
    unittest.main()
