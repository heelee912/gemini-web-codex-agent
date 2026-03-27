from __future__ import annotations

import argparse
import importlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from runtime_config import (
    ROOT_DIR,
    chrome_launch_commands,
    config_template_path,
    default_env_file,
    discover_episode_workspaces,
    loaded_env_file,
    parse_env_assignments,
    prompt_path,
    project_label,
    reference_dir,
    resolve_config_path,
    worker_log_dir,
    workspace_root,
)


REQUIRED_MODULES = ("pygetwindow", "pyperclip", "uiautomation")
SUPERVISOR_SCRIPT = ROOT_DIR / "tools" / "gemini_ui_supervisor.py"


def copy_config_template(output_path: Path, *, force: bool = False) -> Path:
    template_path = config_template_path(ROOT_DIR)
    if output_path.exists() and not force:
        raise FileExistsError(f"Config already exists: {output_path}")
    output_path.write_text(template_path.read_text(encoding="utf-8"), encoding="utf-8")
    return output_path


def python_version_status() -> dict[str, Any]:
    version_info = sys.version_info
    ok = (version_info.major, version_info.minor) >= (3, 11)
    return {
        "ok": ok,
        "value": f"{version_info.major}.{version_info.minor}.{version_info.micro}",
        "detail": "Python 3.11 or later is required.",
    }


def dependency_status() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for module_name in REQUIRED_MODULES:
        try:
            importlib.import_module(module_name)
        except Exception as exc:  # pragma: no cover - exercised by CLI only
            results.append(
                {
                    "name": module_name,
                    "ok": False,
                    "detail": str(exc),
                }
            )
            continue
        results.append({"name": module_name, "ok": True, "detail": "installed"})
    return results


def codex_command_status() -> dict[str, Any]:
    configured = os.environ.get("CODEX_COMMAND")
    if configured:
        command_path = Path(configured)
        if command_path.exists() or shutil.which(configured):
            return {"ok": True, "value": configured, "detail": "available"}
        return {"ok": False, "value": configured, "detail": "Configured CODEX_COMMAND was not found."}
    detected = shutil.which("codex")
    if detected:
        return {"ok": True, "value": detected, "detail": "Found on PATH."}
    return {"ok": False, "value": None, "detail": "codex was not found on PATH."}


def chrome_status() -> dict[str, Any]:
    candidates = chrome_launch_commands("about:blank")
    executable_candidates = [command[0] for command in candidates if command and command[0].lower().endswith(".exe")]
    detected = next((candidate for candidate in executable_candidates if Path(candidate).exists()), None)
    return {
        "ok": bool(detected) or shutil.which("chrome") is not None,
        "value": detected or "chrome",
        "detail": "Chrome launch command detected." if (detected or shutil.which("chrome")) else "Chrome was not found.",
        "candidates": executable_candidates,
    }


def quote_env_value(value: str) -> str:
    if not value:
        return '""'
    if any(char.isspace() for char in value) or "#" in value:
        return json.dumps(value, ensure_ascii=False)
    return value


def update_env_file(path: Path, updates: dict[str, str]) -> Path:
    existing_text = path.read_text(encoding="utf-8") if path.exists() else ""
    lines = existing_text.splitlines()
    remaining = dict(updates)
    rewritten: list[str] = []
    for line in lines:
        stripped = line.strip()
        replaced = False
        if stripped and not stripped.startswith("#") and "=" in line:
            key = line.split("=", 1)[0].strip()
            if key in remaining:
                rewritten.append(f"{key}={quote_env_value(remaining.pop(key))}")
                replaced = True
        if not replaced:
            rewritten.append(line)

    if rewritten and rewritten[-1].strip():
        rewritten.append("")
    for key, value in remaining.items():
        rewritten.append(f"{key}={quote_env_value(value)}")
    path.write_text("\n".join(rewritten).rstrip() + "\n", encoding="utf-8")
    return path


def install_requirements() -> int:
    command = [sys.executable, "-m", "pip", "install", "-r", str(ROOT_DIR / "requirements.txt")]
    completed = subprocess.run(command, cwd=ROOT_DIR, check=False)
    return completed.returncode


def create_or_update_setup(
    env_file: Path,
    *,
    force: bool = False,
    workspace_root_value: str | None = None,
    reference_dir_value: str | None = None,
    prompt_value: str | None = None,
    strict_prompt_value: str | None = None,
    chrome_path_value: str | None = None,
    codex_command_value: str | None = None,
) -> Path:
    if not env_file.exists():
        copy_config_template(env_file, force=force)

    updates: dict[str, str] = {}
    resolved_workspace_root: Path | None = None
    if workspace_root_value:
        resolved_workspace_root = resolve_config_path(workspace_root_value, ROOT_DIR)
        updates["ROLLOUT_WORKSPACE_ROOT"] = str(resolved_workspace_root)
    if reference_dir_value:
        reference_base = resolved_workspace_root or ROOT_DIR
        updates["ROLLOUT_REFERENCE_DIR"] = str(resolve_config_path(reference_dir_value, reference_base))
    if prompt_value:
        updates["ROLLOUT_PROMPT_PATH"] = str(resolve_config_path(prompt_value, ROOT_DIR))
    if strict_prompt_value:
        updates["ROLLOUT_STRICT_PROMPT_PATH"] = str(resolve_config_path(strict_prompt_value, ROOT_DIR))
    if chrome_path_value:
        updates["GEMINI_CHROME_PATH"] = chrome_path_value
    if codex_command_value:
        updates["CODEX_COMMAND"] = codex_command_value
    if updates:
        update_env_file(env_file, updates)
    return env_file


def build_check_report(root_dir: Path = ROOT_DIR) -> dict[str, Any]:
    env_file = loaded_env_file()
    workspace_path = workspace_root(root_dir)
    reference_path = reference_dir(root_dir)
    episode_dirs = list(discover_episode_workspaces(root_dir).values())
    deps = dependency_status()
    chrome = chrome_status()
    codex = codex_command_status()
    python_status = python_version_status()
    standard_prompt = prompt_path(root_dir)
    strict_prompt = prompt_path(root_dir, strict=True)
    checks = {
        "python": python_status,
        "dependencies": deps,
        "chrome": chrome,
        "codex": codex,
        "workspace_root": {
            "ok": workspace_path.exists(),
            "value": str(workspace_path),
            "detail": "Workspace root exists." if workspace_path.exists() else "Workspace root is missing.",
        },
        "reference_dir": {
            "ok": reference_path.exists(),
            "value": str(reference_path),
            "detail": "Reference subtitle directory exists." if reference_path.exists() else "Reference subtitle directory is missing.",
        },
        "episode_workspaces": {
            "ok": True,
            "value": [str(path) for path in episode_dirs],
            "detail": f"Detected {len(episode_dirs)} episode workspace(s).",
        },
        "supervisor_script": {
            "ok": SUPERVISOR_SCRIPT.exists(),
            "value": str(SUPERVISOR_SCRIPT),
            "detail": "Supervisor script exists." if SUPERVISOR_SCRIPT.exists() else "Supervisor script is missing.",
        },
        "prompt_file": {
            "ok": standard_prompt.exists(),
            "value": str(standard_prompt),
            "detail": "Prompt file exists." if standard_prompt.exists() else "Prompt file is missing.",
        },
        "strict_prompt_file": {
            "ok": strict_prompt.exists(),
            "value": str(strict_prompt),
            "detail": "Strict prompt file exists." if strict_prompt.exists() else "Strict prompt file is missing.",
        },
        "worker_log_dir": {
            "ok": True,
            "value": str(worker_log_dir(root_dir)),
            "detail": "Worker log directory will be created on demand.",
        },
    }
    hard_failures = []
    if not python_status["ok"]:
        hard_failures.append("python")
    if not all(item["ok"] for item in deps):
        hard_failures.append("dependencies")
    if not chrome["ok"]:
        hard_failures.append("chrome")
    if not codex["ok"]:
        hard_failures.append("codex")
    if not checks["workspace_root"]["ok"]:
        hard_failures.append("workspace_root")
    if not SUPERVISOR_SCRIPT.exists():
        hard_failures.append("supervisor_script")
    if not standard_prompt.exists():
        hard_failures.append("prompt_file")
    if not strict_prompt.exists():
        hard_failures.append("strict_prompt_file")

    warnings = []
    if env_file is None:
        warnings.append("No rollout.env file is loaded yet.")
    if not reference_path.exists():
        warnings.append("Reference subtitle directory is missing.")
    if not episode_dirs:
        warnings.append("No episode workspace was detected yet.")

    status = "ok" if not hard_failures and not warnings else ("warning" if not hard_failures else "fail")
    return {
        "project_label": project_label(),
        "root_dir": str(root_dir),
        "loaded_env_file": str(env_file) if env_file is not None else None,
        "default_env_file": str(default_env_file(root_dir)),
        "checks": checks,
        "warnings": warnings,
        "hard_failures": hard_failures,
        "status": status,
    }


def print_check_report(report: dict[str, Any]) -> None:
    print(f"Project: {report['project_label']}")
    print(f"Root: {report['root_dir']}")
    print(f"Config: {report['loaded_env_file'] or '(not loaded)'}")
    print(f"Default config path: {report['default_env_file']}")
    for name, payload in report["checks"].items():
        if isinstance(payload, list):
            failed = [item["name"] for item in payload if not item["ok"]]
            status = "OK" if not failed else "FAIL"
            detail = ", ".join(failed) if failed else "all installed"
            print(f"- {name}: {status} ({detail})")
            continue
        status = "OK" if payload["ok"] else "FAIL"
        print(f"- {name}: {status} ({payload['detail']})")
    if report["warnings"]:
        print("Warnings:")
        for warning in report["warnings"]:
            print(f"  - {warning}")


def run_supervisor(thread_id: str, *, codex_command: str | None = None) -> int:
    env = os.environ.copy()
    env["CODEX_THREAD_ID"] = thread_id
    if codex_command:
        env["CODEX_COMMAND"] = codex_command
    command = [sys.executable, str(SUPERVISOR_SCRIPT)]
    if codex_command:
        command.extend(["--codex-command", codex_command])
    completed = subprocess.run(command, cwd=ROOT_DIR, env=env, check=False)
    return completed.returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"Manage the {project_label()} toolchain.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-config", help="Create a local rollout.env file from the example template.")
    init_parser.add_argument("--output", type=Path, default=default_env_file(ROOT_DIR))
    init_parser.add_argument("--force", action="store_true")

    setup_parser = subparsers.add_parser("setup", help="Create or update rollout.env for the current machine.")
    setup_parser.add_argument("--env-file", type=Path, default=default_env_file(ROOT_DIR))
    setup_parser.add_argument("--force", action="store_true")
    setup_parser.add_argument("--workspace-root")
    setup_parser.add_argument("--reference-dir")
    setup_parser.add_argument("--prompt-path")
    setup_parser.add_argument("--strict-prompt-path")
    setup_parser.add_argument("--chrome-path")
    setup_parser.add_argument("--codex-command")
    setup_parser.add_argument("--install-deps", action="store_true")

    check_parser = subparsers.add_parser("check", help="Run a preflight check for the current machine.")
    check_parser.add_argument("--json", action="store_true")

    run_parser = subparsers.add_parser("run-supervisor", help="Launch the supervisor with the provided thread id.")
    run_parser.add_argument("--thread-id", required=True)
    run_parser.add_argument("--codex-command")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "init-config":
        output_path = copy_config_template(args.output, force=args.force)
        print(output_path)
        return 0

    if args.command == "setup":
        env_file = create_or_update_setup(
            args.env_file,
            force=args.force,
            workspace_root_value=args.workspace_root,
            reference_dir_value=args.reference_dir,
            prompt_value=args.prompt_path,
            strict_prompt_value=args.strict_prompt_path,
            chrome_path_value=args.chrome_path,
            codex_command_value=args.codex_command,
        )
        if args.install_deps:
            install_code = install_requirements()
            if install_code != 0:
                return install_code
        print(env_file)
        return 0

    if args.command == "check":
        report = build_check_report(ROOT_DIR)
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            print_check_report(report)
        return 0 if report["status"] != "fail" else 1

    if args.command == "run-supervisor":
        return run_supervisor(args.thread_id, codex_command=args.codex_command)

    raise RuntimeError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
