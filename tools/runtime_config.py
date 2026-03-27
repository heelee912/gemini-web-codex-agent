from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILENAME = "rollout.env"
DEFAULT_PROJECT_LABEL = "Subtitle Rollout"
DEFAULT_PROJECT_SLUG = "subtitle_rollout"
DEFAULT_REFERENCE_DIR_NAME = "reference_subtitles"
DEFAULT_REFERENCE_FILENAME_TEMPLATE = "episode-{episode:02d}.srt"
DEFAULT_OUTPUT_STEM_TEMPLATE = "episode-{episode:02d}"
DEFAULT_WORKER_WINDOW_SUFFIX = "Worker"
DEFAULT_SUPERVISOR_WINDOW_SUFFIX = "Supervisor"
DEFAULT_SEGMENT_DIR_NAME = "segments"
DEFAULT_PASS_OUTPUT_DIR_NAME = "raw_speech_only"
DEFAULT_MERGED_OUTPUT_DIR_NAME = "merged_speech_only"
DEFAULT_SEGMENT_FILENAME_TEMPLATE = "s01e{episode:02d}_seg{segment:02d}.mp4"
DEFAULT_PASS_OUTPUT_FILENAME_TEMPLATE = "s01e{episode:02d}_seg{segment:02d}.srt"
DEFAULT_PROMPT_PATH = Path("timer") / "worker_prompt_ko.txt"
DEFAULT_STRICT_PROMPT_PATH = Path("timer") / "worker_prompt_ko_strict.txt"
DEFAULT_MEDIA_EXTENSIONS = (".mp4", ".mkv", ".mov", ".m4v", ".avi", ".webm")
_LOADED_ENV_FILE: Path | None = None


def parse_env_assignments(text: str) -> dict[str, str]:
    assignments: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        value = raw_value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        assignments[key] = value
    return assignments


def load_env_file(path: Path, *, override: bool = False) -> dict[str, str]:
    assignments = parse_env_assignments(path.read_text(encoding="utf-8"))
    for key, value in assignments.items():
        if override or key not in os.environ:
            os.environ[key] = value
    return assignments


def resolve_config_path(value: str | Path, base_dir: Path) -> Path:
    candidate = Path(value).expanduser()
    return candidate if candidate.is_absolute() else (base_dir / candidate)


def env_value(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value is not None and value.strip():
            return value.strip()
    return None


def env_path(*names: str) -> Path | None:
    value = env_value(*names)
    return Path(value) if value else None


def env_config_path(base_dir: Path, *names: str) -> Path | None:
    value = env_value(*names)
    return resolve_config_path(value, base_dir) if value else None


def config_template_path(root_dir: Path = ROOT_DIR) -> Path:
    preferred = root_dir / "rollout.env.example"
    if preferred.exists():
        return preferred
    return root_dir / "deployment.env.example"


def default_env_file(root_dir: Path = ROOT_DIR) -> Path:
    configured = env_config_path(root_dir, "ROLLOUT_ENV_FILE")
    if configured is not None:
        return configured
    candidates = (
        root_dir / DEFAULT_ENV_FILENAME,
        root_dir / ".rollout.env",
        root_dir / ".env",
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return root_dir / DEFAULT_ENV_FILENAME


def loaded_env_file() -> Path | None:
    return _LOADED_ENV_FILE


def ensure_runtime_env_loaded(root_dir: Path = ROOT_DIR) -> Path | None:
    global _LOADED_ENV_FILE
    env_file = default_env_file(root_dir)
    if env_file.exists():
        load_env_file(env_file)
        _LOADED_ENV_FILE = env_file
    return _LOADED_ENV_FILE


def sanitize_slug(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip()).strip("._-")
    return normalized or DEFAULT_PROJECT_SLUG


def project_label() -> str:
    return env_value("ROLLOUT_PROJECT_LABEL") or DEFAULT_PROJECT_LABEL


def project_slug() -> str:
    return sanitize_slug(env_value("ROLLOUT_PROJECT_SLUG") or DEFAULT_PROJECT_SLUG)


def worker_window_title() -> str:
    return env_value("GEMINI_WORKER_WINDOW_TITLE", "ROLLOUT_WORKER_WINDOW_TITLE") or (
        f"{project_label()} {DEFAULT_WORKER_WINDOW_SUFFIX}"
    )


def supervisor_window_title() -> str:
    return env_value("ROLLOUT_SUPERVISOR_WINDOW_TITLE") or (
        f"{project_label()} {DEFAULT_SUPERVISOR_WINDOW_SUFFIX}"
    )


def workspace_root(root_dir: Path = ROOT_DIR) -> Path:
    configured = env_config_path(root_dir, "ROLLOUT_WORKSPACE_ROOT", "ROLLOUT_DATA_ROOT")
    return configured if configured is not None else root_dir


def worker_log_dir(root_dir: Path = ROOT_DIR) -> Path:
    configured = env_config_path(root_dir, "GEMINI_WORKER_LOG_DIR", "ROLLOUT_WORKER_LOG_DIR")
    if configured is not None:
        return configured
    base_dir = env_config_path(root_dir, "LOCALAPPDATA", "APPDATA", "TEMP", "TMP")
    if base_dir is None:
        base_dir = Path(tempfile.gettempdir())
    return base_dir / project_slug() / "worker_logs"


def stop_request_path(root_dir: Path = ROOT_DIR) -> Path:
    return env_config_path(root_dir, "GEMINI_SUPERVISOR_STOP_REQUEST_PATH") or (root_dir / "SUPERVISOR_STOP")


def prompt_path(root_dir: Path = ROOT_DIR, *, strict: bool = False) -> Path:
    configured_name = "ROLLOUT_STRICT_PROMPT_PATH" if strict else "ROLLOUT_PROMPT_PATH"
    configured = env_config_path(root_dir, configured_name)
    if configured is not None:
        return configured

    default_path = root_dir / (DEFAULT_STRICT_PROMPT_PATH if strict else DEFAULT_PROMPT_PATH)
    if default_path.exists():
        return default_path

    timer_dir = root_dir / "timer"
    if timer_dir.exists():
        if strict:
            patterns = ("*strict*.txt", "*prompt*.txt")
        else:
            patterns = ("*prompt*.txt", "*.txt")
        for pattern in patterns:
            candidates = sorted(
                timer_dir.glob(pattern),
                key=lambda path: (len(path.name), path.name.lower()),
            )
            if candidates:
                return candidates[0]
    return default_path


def manifest_dir(root_dir: Path = ROOT_DIR) -> Path:
    configured = env_config_path(root_dir, "ROLLOUT_MANIFEST_ROOT")
    return configured if configured is not None else workspace_root(root_dir)


def extract_trailing_number(value: str) -> int | None:
    for pattern in (
        r"(?i)episode[-_ ]*(\d{1,3})(?!.*\d)",
        r"(?i)\bep[-_ ]*(\d{1,3})(?!.*\d)",
        r"(?i)\be(\d{1,3})(?!.*\d)",
    ):
        match = re.search(pattern, value)
        if match:
            return int(match.group(1))
    generic_matches = re.findall(r"(\d{1,3})(?!.*\d)", value)
    if generic_matches:
        return int(generic_matches[-1])
    return None


def looks_like_episode_workspace(path: Path) -> bool:
    if not path.is_dir():
        return False
    if (path / DEFAULT_SEGMENT_DIR_NAME).exists():
        return True
    if (path / DEFAULT_PASS_OUTPUT_DIR_NAME).exists():
        return True
    if (path / "manifest.json").exists():
        return True
    if any(path.glob("*.manifest.json")):
        return True
    return False


def discover_episode_workspaces(root_dir: Path = ROOT_DIR) -> dict[int, Path]:
    base_dir = workspace_root(root_dir)
    candidates: list[Path] = []
    if looks_like_episode_workspace(base_dir):
        candidates.append(base_dir)
    if base_dir.exists():
        for child in sorted(base_dir.iterdir(), key=lambda path: path.name.lower()):
            if looks_like_episode_workspace(child):
                candidates.append(child)

    workspaces: dict[int, Path] = {}
    used_numbers: set[int] = set()
    fallback_episode = 1
    for candidate in candidates:
        inferred = extract_trailing_number(candidate.name)
        if inferred is None:
            manifest_candidate = next(iter(sorted(candidate.glob("*.manifest.json"))), None)
            if manifest_candidate is not None:
                inferred = extract_trailing_number(manifest_candidate.stem)
        if inferred is None or inferred in used_numbers:
            while fallback_episode in used_numbers:
                fallback_episode += 1
            inferred = fallback_episode
        used_numbers.add(inferred)
        workspaces[inferred] = candidate

    return dict(sorted(workspaces.items(), key=lambda item: item[0]))


def discover_episode_numbers(root_dir: Path = ROOT_DIR) -> list[int]:
    return sorted(discover_episode_workspaces(root_dir))


def episode_workspace_dir(root_dir: Path, episode: int) -> Path:
    discovered = discover_episode_workspaces(root_dir)
    if episode in discovered:
        return discovered[episode]

    configured_template = env_value("ROLLOUT_EPISODE_WORKDIR_TEMPLATE")
    base_dir = workspace_root(root_dir)
    if configured_template:
        return base_dir / configured_template.format(episode=episode)

    return base_dir / f"episode-{episode:02d}"


def unique_paths(paths: list[Path]) -> list[Path]:
    unique_candidates: list[Path] = []
    seen: set[str] = set()
    for candidate in paths:
        candidate_key = str(candidate).lower()
        if candidate_key in seen:
            continue
        seen.add(candidate_key)
        unique_candidates.append(candidate)
    return unique_candidates


def manifest_file_candidates(root_dir: Path, episode: int) -> list[Path]:
    work_dir = episode_workspace_dir(root_dir, episode)
    candidates: list[Path] = []
    configured_template = env_value("ROLLOUT_MANIFEST_FILENAME_TEMPLATE")
    if configured_template:
        candidates.append(work_dir / configured_template.format(episode=episode))
    candidates.extend(
        [
            work_dir / "manifest.json",
            work_dir / f"episode-{episode:02d}.manifest.json",
            work_dir / f"s01e{episode:02d}.manifest.json",
        ]
    )
    for pattern in ("*.manifest.json", "*manifest*.json"):
        candidates.extend(
            sorted(
                work_dir.glob(pattern),
                key=lambda path: (len(path.name), path.name.lower()),
            )
        )
    return unique_paths(candidates)


def resolve_manifest_path(root_dir: Path, episode: int) -> Path:
    candidates = manifest_file_candidates(root_dir, episode)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def load_manifest_payload(root_dir: Path, episode: int) -> dict[str, Any]:
    manifest_path = resolve_manifest_path(root_dir, episode)
    if not manifest_path.exists():
        return {}
    payload = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {}


def segment_dir_candidates(root_dir: Path, episode: int) -> list[Path]:
    work_dir = episode_workspace_dir(root_dir, episode)
    candidates: list[Path] = []
    configured = env_config_path(work_dir, "ROLLOUT_SEGMENT_DIR", "ROLLOUT_SEGMENTS_DIR")
    if configured is not None:
        candidates.append(configured)
    candidates.extend(
        [
            work_dir / (env_value("ROLLOUT_SEGMENT_DIR_NAME") or DEFAULT_SEGMENT_DIR_NAME),
            work_dir / "clips",
            work_dir / "videos",
            work_dir,
        ]
    )
    return unique_paths(candidates)


def manifest_segment_paths(root_dir: Path, episode: int, segment: int) -> list[Path]:
    payload = load_manifest_payload(root_dir, episode)
    segments = list(payload.get("segments") or [])
    if segment < 1 or segment > len(segments):
        return []

    entry = segments[segment - 1]
    manifest_path = resolve_manifest_path(root_dir, episode)
    work_dir = episode_workspace_dir(root_dir, episode)
    raw_values: list[str] = []
    if isinstance(entry, str):
        raw_values.append(entry)
    elif isinstance(entry, dict):
        for key in (
            "path",
            "file",
            "filename",
            "source",
            "src",
            "video",
            "videoPath",
            "video_path",
            "media",
            "mediaPath",
            "media_path",
        ):
            value = entry.get(key)
            if isinstance(value, str) and value.strip():
                raw_values.append(value.strip())

    candidates: list[Path] = []
    for raw_value in raw_values:
        raw_path = Path(raw_value)
        if raw_path.is_absolute():
            candidates.append(raw_path)
            continue
        candidates.append((manifest_path.parent / raw_path).resolve())
        candidates.append((work_dir / raw_path).resolve())
    return unique_paths(candidates)


def segment_filename_patterns(episode: int, segment: int) -> tuple[str, ...]:
    patterns = [
        f"*seg{segment:02d}.*",
        f"*segment*{segment:02d}.*",
        f"*clip*{segment:02d}.*",
        f"*part*{segment:02d}.*",
        f"*{segment:02d}.*",
    ]
    configured = env_value("ROLLOUT_SEGMENT_FILENAME_TEMPLATE")
    if configured:
        patterns.insert(0, configured.format(episode=episode, segment=segment))
    else:
        patterns.insert(0, DEFAULT_SEGMENT_FILENAME_TEMPLATE.format(episode=episode, segment=segment))
    return tuple(patterns)


def is_media_file(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in DEFAULT_MEDIA_EXTENSIONS


def segment_file_candidates(root_dir: Path, episode: int, segment: int) -> list[Path]:
    candidates: list[Path] = []
    candidates.extend(manifest_segment_paths(root_dir, episode, segment))

    for directory in segment_dir_candidates(root_dir, episode):
        if not directory.exists():
            continue
        for pattern in segment_filename_patterns(episode, segment):
            candidates.extend(
                [
                    path
                    for path in sorted(
                        directory.glob(pattern),
                        key=lambda path: (len(str(path.relative_to(directory))), path.name.lower()),
                    )
                    if is_media_file(path)
                ]
            )

    return unique_paths(candidates)


def resolve_segment_file(root_dir: Path, episode: int, segment: int) -> Path:
    candidates = segment_file_candidates(root_dir, episode, segment)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    if candidates:
        return candidates[0]
    first_segment_dir = segment_dir_candidates(root_dir, episode)[0]
    return first_segment_dir / DEFAULT_SEGMENT_FILENAME_TEMPLATE.format(episode=episode, segment=segment)


def infer_segment_number(path: Path) -> int | None:
    stem = path.stem
    for pattern in (
        r"(?i)seg(?:ment)?[-_ ]*(\d{1,3})(?!.*\d)",
        r"(?i)clip[-_ ]*(\d{1,3})(?!.*\d)",
        r"(?i)part[-_ ]*(\d{1,3})(?!.*\d)",
    ):
        match = re.search(pattern, stem)
        if match:
            return int(match.group(1))
    return extract_trailing_number(stem)


def discover_segment_numbers(root_dir: Path, episode: int) -> list[int]:
    payload = load_manifest_payload(root_dir, episode)
    manifest_segments = list(payload.get("segments") or [])
    if manifest_segments:
        return list(range(1, len(manifest_segments) + 1))

    numbers: set[int] = set()
    for directory in segment_dir_candidates(root_dir, episode):
        if not directory.exists():
            continue
        for path in directory.iterdir():
            if not is_media_file(path):
                continue
            number = infer_segment_number(path)
            if number is not None:
                numbers.add(number)

    pass_root = episode_workspace_dir(root_dir, episode) / (
        env_value("ROLLOUT_PASS_OUTPUT_DIR_NAME") or DEFAULT_PASS_OUTPUT_DIR_NAME
    )
    if pass_root.exists():
        for path in pass_root.rglob("*.srt"):
            number = infer_segment_number(path)
            if number is not None:
                numbers.add(number)

    return sorted(numbers)


def output_root_dir(root_dir: Path, episode: int) -> Path:
    work_dir = episode_workspace_dir(root_dir, episode)
    return work_dir / (env_value("ROLLOUT_PASS_OUTPUT_DIR_NAME") or DEFAULT_PASS_OUTPUT_DIR_NAME)


def pass_output_file_candidates(root_dir: Path, episode: int, pass_number: int, segment: int) -> list[Path]:
    output_dir = output_root_dir(root_dir, episode) / f"pass{pass_number}"
    candidates: list[Path] = []
    configured_template = env_value("ROLLOUT_PASS_OUTPUT_FILENAME_TEMPLATE")
    if configured_template:
        candidates.append(output_dir / configured_template.format(episode=episode, segment=segment, pass_number=pass_number))
    candidates.extend(
        [
            output_dir / DEFAULT_PASS_OUTPUT_FILENAME_TEMPLATE.format(episode=episode, segment=segment),
            output_dir / f"segment-{segment:02d}.srt",
            output_dir / f"episode-{episode:02d}-segment-{segment:02d}.srt",
        ]
    )

    segment_source = resolve_segment_file(root_dir, episode, segment)
    candidates.append(output_dir / f"{segment_source.stem}.srt")

    for pattern in (
        f"*seg{segment:02d}*.srt",
        f"*segment*{segment:02d}*.srt",
        f"*{segment:02d}*.srt",
    ):
        candidates.extend(
            sorted(
                output_dir.glob(pattern),
                key=lambda path: (len(path.name), path.name.lower()),
            )
        )
    return unique_paths(candidates)


def pass_output_path(root_dir: Path, episode: int, pass_number: int, segment: int) -> Path:
    candidates = pass_output_file_candidates(root_dir, episode, pass_number, segment)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def reference_dir(root_dir: Path, configured_dir: Path | None = None) -> Path:
    if configured_dir is not None:
        return configured_dir
    configured = env_config_path(workspace_root(root_dir), "ROLLOUT_REFERENCE_DIR", "SUBTITLE_REFERENCE_DIR")
    if configured is not None:
        return configured
    return workspace_root(root_dir) / (env_value("ROLLOUT_REFERENCE_DIR_NAME") or DEFAULT_REFERENCE_DIR_NAME)


def episode_output_stem(episode: int) -> str:
    template = env_value("ROLLOUT_EPISODE_OUTPUT_STEM_TEMPLATE") or DEFAULT_OUTPUT_STEM_TEMPLATE
    return template.format(episode=episode)


def discover_episode_output_file(output_dir: Path, episode: int, *, final: bool) -> Path | None:
    suffix = ".final.srt" if final else ".srt"
    patterns = (
        f"*episode*{episode:02d}*{suffix}",
        f"*E{episode:02d}*{suffix}",
        f"*{episode:02d}*{suffix}",
    )
    for pattern in patterns:
        candidates = [
            path
            for path in sorted(output_dir.glob(pattern), key=lambda path: (len(path.name), path.name.lower()))
            if path.name.endswith(suffix)
        ]
        if not final:
            candidates = [path for path in candidates if not path.name.endswith(".final.srt")]
        if candidates:
            return candidates[0]
    return None


def episode_output_paths(root_dir: Path, episode: int) -> tuple[Path, Path]:
    output_dir = episode_workspace_dir(root_dir, episode) / (
        env_value("ROLLOUT_MERGED_OUTPUT_DIR_NAME") or DEFAULT_MERGED_OUTPUT_DIR_NAME
    )
    stem = episode_output_stem(episode)
    configured_final = output_dir / f"{stem}.final.srt"
    configured_plain = output_dir / f"{stem}.srt"
    resolved_final = configured_final if configured_final.exists() else (
        discover_episode_output_file(output_dir, episode, final=True) or configured_final
    )
    resolved_plain = configured_plain if configured_plain.exists() else (
        discover_episode_output_file(output_dir, episode, final=False) or configured_plain
    )
    return (resolved_final, resolved_plain)


def reference_file_candidates(root_dir: Path, episode: int, configured_dir: Path | None = None) -> list[Path]:
    target_dir = reference_dir(root_dir, configured_dir)
    filename_template = env_value("ROLLOUT_REFERENCE_FILENAME_TEMPLATE") or DEFAULT_REFERENCE_FILENAME_TEMPLATE
    ordered_candidates: list[Path] = [target_dir / filename_template.format(episode=episode)]

    discovery_patterns = (
        f"*episode*{episode:02d}*.srt",
        f"*E{episode:02d}*.srt",
        f"*{episode:02d}*.srt",
    )
    for pattern in discovery_patterns:
        ordered_candidates.extend(
            sorted(
                target_dir.glob(pattern),
                key=lambda path: (len(path.name), path.name.lower()),
            )
        )

    return unique_paths(ordered_candidates)


def resolve_reference_file(root_dir: Path, episode: int, configured_dir: Path | None = None) -> Path:
    candidates = reference_file_candidates(root_dir, episode, configured_dir)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def chrome_launch_commands(url: str) -> tuple[list[str], ...]:
    configured = env_value("GEMINI_CHROME_PATH", "CHROME_PATH")
    candidates: list[str] = []
    if configured:
        candidates.append(configured)

    for env_name, suffix in (
        ("ProgramFiles", Path("Google") / "Chrome" / "Application" / "chrome.exe"),
        ("ProgramFiles(x86)", Path("Google") / "Chrome" / "Application" / "chrome.exe"),
        ("LOCALAPPDATA", Path("Google") / "Chrome" / "Application" / "chrome.exe"),
    ):
        root = env_value(env_name)
        if not root:
            continue
        candidate = str(Path(root) / suffix)
        if candidate not in candidates:
            candidates.append(candidate)

    commands = tuple([candidate, url] for candidate in candidates)
    return commands + (["cmd.exe", "/c", "start", "", "chrome", url],)


ensure_runtime_env_loaded()
