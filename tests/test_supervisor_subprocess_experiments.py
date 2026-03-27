from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest
from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOLS_ROOT = ROOT / "tools"
FILES_TO_COPY = (
    "build_state.py",
    "gemini_ui_supervisor.py",
    "gemini_resume_hint.py",
    "next_batch.py",
    "runtime_config.py",
    "same_session_recovery.py",
    "teogonia_rollout.py",
)

FAKE_WORKER = """\
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SANDBOX_DIR = ROOT_DIR / ".sandbox"
SCENARIO_PATH = SANDBOX_DIR / "scenario.json"
RUNTIME_PATH = SANDBOX_DIR / "runtime_state.json"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\\n", encoding="utf-8")


def next_step(kind: str) -> dict:
    scenario = load_json(SCENARIO_PATH)
    runtime = load_json(RUNTIME_PATH)
    key = f"{kind}_steps"
    index_key = f"{kind}_index"
    steps = list(scenario.get(key) or [])
    index = int(runtime.get(index_key, 0))
    if not steps:
        step = {}
    elif index < len(steps):
        step = dict(steps[index])
    else:
        step = dict(steps[-1])
    runtime[index_key] = index + 1
    save_json(RUNTIME_PATH, runtime)
    return step


def write_output(episode: int, pass_number: int, segment: int, text: str) -> None:
    path = (
        ROOT_DIR
        / f"episode-{episode:02d}"
        / "raw_speech_only"
        / f"pass{pass_number}"
        / f"s01e{episode:02d}_seg{segment:02d}.srt"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = (
        "1\\n"
        "00:00:00,000 --> 00:00:00,900\\n"
        f"{text}\\n"
    )
    path.write_text(rendered, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a Gemini UI subtitle batch through shell automation.")
    parser.add_argument("--probe-browser-state", action="store_true")
    parser.add_argument("--episode", type=int)
    parser.add_argument("--pass-number", type=int)
    parser.add_argument("--segments", nargs="+")
    parser.add_argument("--prompt-path")
    args = parser.parse_args()

    if args.probe_browser_state:
        step = next_step("probe")
        payload = {
            "status": step.get("status", "ready"),
            "wait_seconds": step.get("wait_seconds"),
            "resume_at_iso": step.get("resume_at_iso"),
            "permission_dialog_cleared": bool(step.get("permission_dialog_cleared", False)),
            "visible_texts": list(step.get("visible_texts") or []),
            "matched_texts": list(step.get("matched_texts") or []),
        }
        print(json.dumps(payload, ensure_ascii=False))
        return 0

    step = next_step("run")
    if str(step.get("result") or "success") == "success":
        text = str(step.get("text") or "안녕하세요")
        for raw_segment in args.segments or []:
            write_output(args.episode, args.pass_number, int(raw_segment), text)
        return 0

    stdout = str(step.get("stdout") or "")
    stderr = str(step.get("stderr") or "")
    if stdout:
        print(stdout)
    if stderr:
        print(stderr, file=sys.stderr)
    return int(step.get("returncode", 1))


if __name__ == "__main__":
    raise SystemExit(main())
"""

FAKE_CODEX = """\
#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\\n", encoding="utf-8")


def main() -> int:
    root_dir = Path(__file__).resolve().parent
    sandbox_dir = root_dir / ".sandbox"
    scenario_path = sandbox_dir / "scenario.json"
    runtime_path = sandbox_dir / "runtime_state.json"
    scenario = load_json(scenario_path)
    runtime = load_json(runtime_path)
    steps = list(scenario.get("codex_steps") or [])
    index = int(runtime.get("codex_index", 0))
    step = dict(steps[index]) if index < len(steps) else (dict(steps[-1]) if steps else {})
    runtime["codex_index"] = index + 1
    save_json(runtime_path, runtime)

    output_path = None
    for position, token in enumerate(sys.argv):
        if token == "-o" and position + 1 < len(sys.argv):
            output_path = Path(sys.argv[position + 1])
            break
    if output_path is None:
        raise SystemExit("missing -o output path")

    _prompt = sys.stdin.read()
    if int(step.get("exit_code", 0)):
        print(str(step.get("stderr") or "fake codex failure"), file=sys.stderr)
        return int(step["exit_code"])

    reply = step.get("reply")
    if reply is None:
        reply = {
            "decision_version": 1,
            "workflow_status": "continue",
            "diagnosis": "Retry immediately.",
            "recovery_action": {
                "kind": "retry_now",
                "reason": "No wait required.",
                "delay_seconds": 0,
            },
        }
    output_path.write_text(
        reply if isinstance(reply, str) else json.dumps(reply, ensure_ascii=False),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
"""


class SupervisorSubprocessExperiments(unittest.TestCase):
    maxDiff = None

    def _prepare_sandbox(self, sandbox_root: Path) -> None:
        tools_dir = sandbox_root / "tools"
        tools_dir.mkdir(parents=True, exist_ok=True)
        for name in FILES_TO_COPY:
            source = TOOLS_ROOT / name
            target = tools_dir / name
            shutil.copy2(source, target)

        worker_path = tools_dir / "gemini_ui_batch_shell.py"
        worker_path.write_text(textwrap.dedent(FAKE_WORKER), encoding="utf-8")

        codex_path = sandbox_root / "fake_codex.py"
        codex_path.write_text(textwrap.dedent(FAKE_CODEX), encoding="utf-8")
        codex_path.chmod(codex_path.stat().st_mode | stat.S_IEXEC)

        (sandbox_root / "timer").mkdir(parents=True, exist_ok=True)
        (sandbox_root / "timer" / "worker_prompt_ko_strict.txt").write_text("strict prompt", encoding="utf-8")

        (sandbox_root / ".codex").mkdir(parents=True, exist_ok=True)
        (sandbox_root / ".sandbox").mkdir(parents=True, exist_ok=True)

        work_dir = sandbox_root / "episode-01"
        (work_dir / "raw_speech_only" / "pass1").mkdir(parents=True, exist_ok=True)
        (work_dir / "raw_speech_only" / "pass2").mkdir(parents=True, exist_ok=True)
        (work_dir / "raw_speech_only" / "pass3").mkdir(parents=True, exist_ok=True)
        (work_dir / "s01e01.manifest.json").write_text(
            json.dumps({"segments": [{"durationMs": 1000}]}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        whisper_dir = sandbox_root / "whisper"
        whisper_dir.mkdir(parents=True, exist_ok=True)
        (whisper_dir / "episode-01.srt").write_text(
            "1\n00:00:00,000 --> 00:00:00,900\n안녕하세요\n",
            encoding="utf-8",
        )

    def _iso_after(self, seconds: int) -> str:
        return (datetime.now().astimezone() + timedelta(seconds=seconds)).isoformat()

    def _run_scenario(
        self,
        scenario: dict,
        *,
        stop_requested: bool = False,
        timeout: int = 30,
    ) -> tuple[subprocess.CompletedProcess[str], dict, dict]:
        with tempfile.TemporaryDirectory() as temp_dir:
            sandbox_root = Path(temp_dir)
            self._prepare_sandbox(sandbox_root)

            scenario_path = sandbox_root / ".sandbox" / "scenario.json"
            runtime_path = sandbox_root / ".sandbox" / "runtime_state.json"
            scenario_path.write_text(json.dumps(scenario, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            runtime_path.write_text(
                json.dumps({"run_index": 0, "probe_index": 0, "codex_index": 0}, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )

            stop_path = sandbox_root / "SUPERVISOR_STOP"
            if stop_requested:
                stop_path.write_text("stop\n", encoding="utf-8")

            env = os.environ.copy()
            env.update(
                {
                    "CODEX_THREAD_ID": "sandbox-session",
                    "CODEX_COMMAND": str(sandbox_root / "fake_codex.py"),
                    "GEMINI_UI_PYTHON": sys.executable,
                    "ROLLOUT_REFERENCE_DIR": str(sandbox_root / "whisper"),
                    "GEMINI_SUPERVISOR_STOP_REQUEST_PATH": str(stop_path),
                    "GEMINI_INTERRUPTED_RECOVERY_DELAY_SECONDS": "1",
                    "GEMINI_PRO_LIMIT_WAIT_SECONDS": "1",
                    "PYTHONUNBUFFERED": "1",
                }
            )

            completed = subprocess.run(
                [sys.executable, str(sandbox_root / "tools" / "gemini_ui_supervisor.py")],
                cwd=sandbox_root,
                capture_output=True,
                text=True,
                env=env,
                timeout=timeout,
                check=False,
            )
            result = json.loads((sandbox_root / ".codex" / "rollout_result.json").read_text(encoding="utf-8"))
            state = json.loads((sandbox_root / ".codex" / "state.json").read_text(encoding="utf-8"))
            runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
            payload = {
                "completed": {
                    "returncode": completed.returncode,
                    "stdout": completed.stdout,
                    "stderr": completed.stderr,
                },
                "result": result,
                "state": state,
                "runtime": runtime,
            }
            return completed, payload["result"], payload["state"]

    def test_experiment_explicit_stop_pauses_rollout(self) -> None:
        completed, result, state = self._run_scenario(
            {
                "run_steps": [
                    {
                        "result": "fail",
                        "returncode": 130,
                        "stderr": "worker window closed",
                    }
                ]
            },
            stop_requested=True,
        )

        self.assertEqual(completed.returncode, 0)
        self.assertEqual(result["status"], "PAUSED_BY_USER")
        self.assertEqual(state["items"][0]["supervisor_phase"], "paused_by_user")

    def test_experiment_worker_interrupt_cooldown_then_resume_to_done(self) -> None:
        completed, result, state = self._run_scenario(
            {
                "run_steps": [
                    {"result": "fail", "returncode": 130, "stderr": "interrupted"},
                    {"result": "success", "text": "안녕하세요"},
                    {"result": "success", "text": "안녕하세요"},
                    {"result": "success", "text": "안녕하세요"},
                ]
            }
        )

        self.assertEqual(completed.returncode, 0)
        self.assertEqual(result["status"], "DONE")
        self.assertEqual(state["summary"]["generated"], 3)
        self.assertEqual(state["summary"]["segment_groups_accepted"], 1)
        self.assertEqual(state["summary"]["episode_finals_complete"], 1)

    def test_experiment_pro_wait_reschedules_then_resumes_to_done(self) -> None:
        first_resume = self._iso_after(1)
        second_resume = self._iso_after(2)
        completed, result, state = self._run_scenario(
            {
                "run_steps": [
                    {
                        "result": "fail",
                        "returncode": 1,
                        "stderr": f"pro_limit_reached: Gemini Pro usage limit is visible | wait_seconds=1 | resume_at={first_resume}",
                    },
                    {"result": "success", "text": "안녕하세요"},
                    {"result": "success", "text": "안녕하세요"},
                    {"result": "success", "text": "안녕하세요"},
                ],
                "probe_steps": [
                    {"status": "pro_limit", "wait_seconds": 1, "resume_at_iso": first_resume},
                    {"status": "pro_limit", "wait_seconds": 1, "resume_at_iso": second_resume},
                    {"status": "ready"},
                ],
                "codex_steps": [
                    {
                        "reply": {
                            "decision_version": 1,
                            "workflow_status": "continue",
                            "diagnosis": "Gemini Pro quota is still active.",
                            "recovery_action": {
                                "kind": "wait",
                                "reason": "Wait for Pro quota reset.",
                                "delay_seconds": 1,
                            },
                        }
                    }
                ],
            },
            timeout=40,
        )

        self.assertEqual(completed.returncode, 0)
        self.assertEqual(result["status"], "DONE")
        self.assertEqual(state.get("supervisor_wait"), None)
        self.assertEqual(state["summary"]["episode_finals_complete"], 1)

    def test_experiment_strict_retry_then_same_session_recovery_reaches_done(self) -> None:
        completed, result, state = self._run_scenario(
            {
                "run_steps": [
                    {"result": "fail", "returncode": 1, "stderr": "plain automation failure"},
                    {"result": "fail", "returncode": 86, "stderr": "supervisor_required: file open dialog did not close"},
                    {"result": "success", "text": "안녕하세요"},
                    {"result": "success", "text": "안녕하세요"},
                    {"result": "success", "text": "안녕하세요"},
                ],
                "probe_steps": [
                    {"status": "unknown"},
                ],
                "codex_steps": [
                    {
                        "reply": {
                            "decision_version": 1,
                            "workflow_status": "continue",
                            "diagnosis": "UI issue resolved enough to retry.",
                            "recovery_action": {
                                "kind": "retry_now",
                                "reason": "Retry now after supervisor adjustment.",
                                "delay_seconds": 0,
                            },
                        }
                    }
                ],
            }
        )

        self.assertEqual(completed.returncode, 0)
        self.assertEqual(result["status"], "DONE")
        self.assertEqual(state["summary"]["generated"], 3)

    def test_experiment_hard_block_after_three_distinct_paths(self) -> None:
        completed, result, state = self._run_scenario(
            {
                "run_steps": [
                    {"result": "fail", "returncode": 1, "stderr": "plain automation failure"},
                    {"result": "fail", "returncode": 1, "stderr": "plain automation failure"},
                    {"result": "fail", "returncode": 1, "stderr": "plain automation failure"},
                    {"result": "fail", "returncode": 86, "stderr": "supervisor_required: file open dialog did not close"},
                    {"result": "fail", "returncode": 86, "stderr": "supervisor_required: file open dialog did not close"},
                ],
                "probe_steps": [
                    {"status": "unknown"},
                    {"status": "unknown"},
                ],
                "codex_steps": [
                    {
                        "reply": {
                            "decision_version": 1,
                            "workflow_status": "continue",
                            "diagnosis": "Retry once more before giving up.",
                            "recovery_action": {
                                "kind": "retry_now",
                                "reason": "Retry now.",
                                "delay_seconds": 0,
                            },
                        }
                    }
                ],
            }
        )

        self.assertEqual(completed.returncode, 2)
        self.assertEqual(result["status"], "HARD_BLOCKED")
        self.assertEqual(state["summary"]["segment_groups_hard_blocked"], 1)


if __name__ == "__main__":
    unittest.main()
