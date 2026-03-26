from __future__ import annotations

import json
import os
import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any


DEFAULT_WHISPER_DIR = Path(
    os.environ.get(
        "TEOGONIA_WHISPER_DIR",
        "/mnt/c/Users/Master/Desktop/codex/위스퍼 정렬처리 일본 자막",
    )
)

TIME_PATTERN = re.compile(
    r"(?m)^(\d{2}:\d{2}:\d{2},\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2},\d{3})$"
)
WHISPER_GARBLED_MIN_TEXT_LENGTH = 24
WHISPER_GARBLED_MAX_SIMILARITY = 0.35
WHISPER_GARBLED_MIN_DELTA = 0.05


@dataclass(frozen=True)
class SubtitleCue:
    start_ms: int
    end_ms: int
    text: str


@dataclass(frozen=True)
class SegmentAcceptanceDecision:
    episode: int
    segment: int
    status: str
    selected_pass_number: int | None
    retry_pass_number: int | None
    reason: str
    quality_state: str
    pairwise_similarity: dict[str, float]
    whisper_similarity: dict[str, float]
    whisper_excerpt: str
    selected_pair: tuple[int, int] | None = None
    selection_basis: str | None = None
    whisper_review: dict[str, Any] | None = None

    @property
    def segment_id(self) -> str:
        return f"E{self.episode:02d}-S{self.segment:02d}"

    def whisper_evidence(self) -> dict[str, Any]:
        return {
            "pairwise_similarity": self.pairwise_similarity,
            "whisper_similarity": self.whisper_similarity,
            "whisper_excerpt": self.whisper_excerpt,
            "reason": self.reason,
            "quality_state": self.quality_state,
            "selected_pair": list(self.selected_pair) if self.selected_pair else None,
            "selection_basis": self.selection_basis,
            "selected_pass_number": self.selected_pass_number,
            "retry_pass_number": self.retry_pass_number,
            "whisper_review": self.whisper_review,
        }


def parse_timestamp(value: str) -> int:
    parts = value.split(":")
    if len(parts) != 3:
        raise ValueError(f"Invalid timestamp: {value}")
    hours, minutes, rest = parts
    seconds, millis = rest.split(",")
    return (int(hours) * 3600 + int(minutes) * 60 + int(seconds)) * 1000 + int(millis)


def format_timestamp(value_ms: int) -> str:
    if value_ms < 0:
        value_ms = 0
    hours = value_ms // 3_600_000
    minutes = (value_ms % 3_600_000) // 60_000
    seconds = (value_ms % 60_000) // 1_000
    millis = value_ms % 1_000
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},{millis:03d}"


def parse_srt_text(text: str) -> list[SubtitleCue]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip("\ufeff").strip()
    if not normalized:
        return []

    cues: list[SubtitleCue] = []
    for block in re.split(r"\n\s*\n", normalized):
        lines = [line.strip("\ufeff") for line in block.split("\n") if line.strip()]
        if len(lines) < 2:
            continue
        timing_line_index = 1 if lines[0].isdigit() else 0
        if timing_line_index >= len(lines):
            continue
        timing_line = lines[timing_line_index].strip()
        match = TIME_PATTERN.match(timing_line)
        if not match:
            continue
        start_raw, end_raw = match.group(1), match.group(2)
        text_lines = lines[timing_line_index + 1 :]
        if not text_lines:
            continue
        cues.append(
            SubtitleCue(
                start_ms=parse_timestamp(start_raw),
                end_ms=parse_timestamp(end_raw),
                text="\n".join(text_lines).strip(),
            )
        )
    return cues


def read_srt(path: Path) -> list[SubtitleCue]:
    return parse_srt_text(path.read_text(encoding="utf-8"))


def normalize_dialogue(text: str) -> str:
    folded = unicodedata.normalize("NFKC", text.replace("\ufeff", " "))
    folded = re.sub(r"\s+", " ", folded)
    return folded.strip()


def cues_to_text(cues: list[SubtitleCue]) -> str:
    return normalize_dialogue(" ".join(cue.text for cue in cues if cue.text.strip()))


def similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return round(SequenceMatcher(None, left, right).ratio(), 4)


def is_japanese_character(char: str) -> bool:
    codepoint = ord(char)
    return (
        0x3040 <= codepoint <= 0x30FF
        or 0x3400 <= codepoint <= 0x4DBF
        or 0x4E00 <= codepoint <= 0x9FFF
        or 0xFF66 <= codepoint <= 0xFF9D
    )


def whisper_review(
    whisper_text: str,
    whisper_similarity: dict[str, float],
    candidate_passes: tuple[int, int],
) -> dict[str, Any]:
    normalized = normalize_dialogue(whisper_text)
    if not normalized:
        return {
            "status": "garbled",
            "reason": "Whisper segment text was empty after segment windowing.",
        }

    meaningful_chars = [char for char in normalized if not char.isspace()]
    japanese_chars = [char for char in meaningful_chars if is_japanese_character(char)]
    japanese_ratio = round(len(japanese_chars) / len(meaningful_chars), 4) if meaningful_chars else 0.0
    candidate_scores = [whisper_similarity.get(str(pass_number), 0.0) for pass_number in candidate_passes]
    best_score = max(candidate_scores, default=0.0)
    score_delta = round(abs(candidate_scores[0] - candidate_scores[1]), 4) if len(candidate_scores) == 2 else 0.0

    if len(normalized) < WHISPER_GARBLED_MIN_TEXT_LENGTH:
        return {
            "status": "garbled",
            "reason": f"Whisper segment was too short ({len(normalized)} chars) to break a Gemini tie reliably.",
            "japanese_ratio": japanese_ratio,
            "best_similarity": best_score,
            "candidate_similarity_delta": score_delta,
        }
    if japanese_ratio < 0.35:
        return {
            "status": "garbled",
            "reason": "Whisper segment contained too little Japanese script to trust as tie-break evidence.",
            "japanese_ratio": japanese_ratio,
            "best_similarity": best_score,
            "candidate_similarity_delta": score_delta,
        }
    if best_score < WHISPER_GARBLED_MAX_SIMILARITY and score_delta < WHISPER_GARBLED_MIN_DELTA:
        return {
            "status": "garbled",
            "reason": "Whisper segment was too far from both consistent Gemini candidates to decide between them.",
            "japanese_ratio": japanese_ratio,
            "best_similarity": best_score,
            "candidate_similarity_delta": score_delta,
        }

    return {
        "status": "usable",
        "reason": "Whisper segment is usable as secondary tie-break evidence.",
        "japanese_ratio": japanese_ratio,
        "best_similarity": best_score,
        "candidate_similarity_delta": score_delta,
    }


def work_dir(root_dir: Path, episode: int) -> Path:
    return root_dir / f"video_only_retry_s01e{episode:02d}_rerun2"


def manifest_path(root_dir: Path, episode: int) -> Path:
    return work_dir(root_dir, episode) / f"s01e{episode:02d}.manifest.json"


def pass_srt_path(root_dir: Path, episode: int, pass_number: int, segment: int) -> Path:
    return (
        work_dir(root_dir, episode)
        / "raw_speech_only"
        / f"pass{pass_number}"
        / f"s01e{episode:02d}_seg{segment:02d}.srt"
    )


def load_manifest(root_dir: Path, episode: int) -> list[dict[str, Any]]:
    payload = json.loads(manifest_path(root_dir, episode).read_text(encoding="utf-8-sig"))
    return list(payload.get("segments", []))


def segment_window_ms(root_dir: Path, episode: int, segment: int) -> tuple[int, int]:
    segments = load_manifest(root_dir, episode)
    offset_ms = 0
    for index, entry in enumerate(segments, start=1):
        duration_ms = int(entry["durationMs"])
        if index == segment:
            return offset_ms, offset_ms + duration_ms
        offset_ms += duration_ms
    raise KeyError(f"Segment {segment} not found in E{episode:02d} manifest")


def whisper_path(episode: int, whisper_dir: Path = DEFAULT_WHISPER_DIR) -> Path:
    return whisper_dir / f"[Judas] Teogonia - S01E{episode:02d}.srt"


def load_whisper_segment_text(root_dir: Path, episode: int, segment: int, whisper_dir: Path = DEFAULT_WHISPER_DIR) -> str:
    source = whisper_path(episode, whisper_dir)
    if not source.exists():
        return ""
    start_ms, end_ms = segment_window_ms(root_dir, episode, segment)
    cues = read_srt(source)
    window_cues = [
        cue
        for cue in cues
        if cue.end_ms > start_ms and cue.start_ms < end_ms
    ]
    shifted = [
        SubtitleCue(
            start_ms=max(cue.start_ms - start_ms, 0),
            end_ms=max(cue.end_ms - start_ms, 0),
            text=cue.text,
        )
        for cue in window_cues
    ]
    return cues_to_text(shifted)


def choose_retry_pass(
    pass_numbers: list[int],
    pairwise: dict[str, float],
    whisper_similarity: dict[str, float],
) -> int:
    per_pass_score: dict[int, float] = {}
    for pass_number in pass_numbers:
        neighbors = [value for key, value in pairwise.items() if f"{pass_number}" in key.split("-")]
        per_pass_score[pass_number] = sum(neighbors) / len(neighbors) if neighbors else 0.0
    worst_score = min(per_pass_score.values())
    worst_passes = [pass_number for pass_number, score in per_pass_score.items() if score == worst_score]
    return min(
        worst_passes,
        key=lambda pass_number: (
            whisper_similarity.get(str(pass_number), 0.0),
            -pass_number,
        ),
    )


def evaluate_segment_group(root_dir: Path, episode: int, segment: int, whisper_dir: Path = DEFAULT_WHISPER_DIR) -> SegmentAcceptanceDecision:
    pass_paths = {
        pass_number: pass_srt_path(root_dir, episode, pass_number, segment)
        for pass_number in (1, 2, 3)
    }
    existing_paths = {
        pass_number: path
        for pass_number, path in pass_paths.items()
        if path.exists() and path.stat().st_size > 0
    }
    if len(existing_paths) < 3:
        return SegmentAcceptanceDecision(
            episode=episode,
            segment=segment,
            status="pending_generation",
            selected_pass_number=None,
            retry_pass_number=None,
            reason="Three generation passes are not present yet.",
            quality_state="generation_pending",
            pairwise_similarity={},
            whisper_similarity={},
            whisper_excerpt="",
            whisper_review=None,
        )

    pass_texts = {
        pass_number: cues_to_text(read_srt(path))
        for pass_number, path in existing_paths.items()
    }
    whisper_text = load_whisper_segment_text(root_dir, episode, segment, whisper_dir=whisper_dir)
    pairwise: dict[str, float] = {}
    pass_numbers = sorted(pass_texts)
    for left_index, left_pass in enumerate(pass_numbers):
        for right_pass in pass_numbers[left_index + 1 :]:
            pairwise[f"{left_pass}-{right_pass}"] = similarity(pass_texts[left_pass], pass_texts[right_pass])
    whisper_similarity = {
        str(pass_number): similarity(text, whisper_text)
        for pass_number, text in pass_texts.items()
    }

    unique_texts = {text for text in pass_texts.values() if text}
    if len(unique_texts) == 1:
        selected = min(pass_numbers)
        return SegmentAcceptanceDecision(
            episode=episode,
            segment=segment,
            status="accepted",
            selected_pass_number=selected,
            retry_pass_number=None,
            reason="All three Gemini generations converged to the same result.",
            quality_state="accepted_unanimous",
            pairwise_similarity=pairwise,
            whisper_similarity=whisper_similarity,
            whisper_excerpt=whisper_text[:400],
            selection_basis="unanimous",
            whisper_review=whisper_review(whisper_text, whisper_similarity, (pass_numbers[0], pass_numbers[1])),
        )

    best_pair_key, best_pair_score = max(pairwise.items(), key=lambda item: item[1])
    left_pass, right_pass = [int(token) for token in best_pair_key.split("-")]
    if best_pair_score >= 0.80:
        review = whisper_review(whisper_text, whisper_similarity, (left_pass, right_pass))
        if review["status"] == "usable":
            selected = max(
                (left_pass, right_pass),
                key=lambda pass_number: (
                    whisper_similarity.get(str(pass_number), 0.0),
                    -pass_number,
                ),
            )
            reason = (
                f"Passes {left_pass} and {right_pass} were strongly consistent "
                f"(similarity {best_pair_score:.3f}) and pass {selected} had the stronger Whisper match."
            )
            selection_basis = "whisper_similarity"
        else:
            selected = min(left_pass, right_pass)
            reason = (
                f"Passes {left_pass} and {right_pass} were strongly consistent "
                f"(similarity {best_pair_score:.3f}). Whisper review marked the segment garbled, "
                f"so the coherent Gemini pair was retained and pass {selected} was chosen deterministically."
            )
            selection_basis = "gemini_consistency_preserved"
        return SegmentAcceptanceDecision(
            episode=episode,
            segment=segment,
            status="accepted",
            selected_pass_number=selected,
            retry_pass_number=None,
            reason=reason,
            quality_state="accepted_consistent",
            pairwise_similarity=pairwise,
            whisper_similarity=whisper_similarity,
            whisper_excerpt=whisper_text[:400],
            selected_pair=(left_pass, right_pass),
            selection_basis=selection_basis,
            whisper_review=review,
        )

    retry_pass = choose_retry_pass(pass_numbers, pairwise, whisper_similarity)
    return SegmentAcceptanceDecision(
        episode=episode,
        segment=segment,
        status="needs_regeneration",
        selected_pass_number=None,
        retry_pass_number=retry_pass,
        reason=(
            f"No pass pair was consistent enough for acceptance. Best pair {best_pair_key} "
            f"only reached similarity {best_pair_score:.3f}."
        ),
        quality_state="needs_regeneration",
        pairwise_similarity=pairwise,
        whisper_similarity=whisper_similarity,
        whisper_excerpt=whisper_text[:400],
        selected_pair=(left_pass, right_pass),
        selection_basis="regenerate_lowest_consensus",
        whisper_review=whisper_review(whisper_text, whisper_similarity, (left_pass, right_pass)),
    )


def render_srt(cues: list[SubtitleCue]) -> str:
    lines: list[str] = []
    for index, cue in enumerate(cues, start=1):
        lines.append(str(index))
        lines.append(f"{format_timestamp(cue.start_ms)} --> {format_timestamp(cue.end_ms)}")
        lines.append(cue.text)
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def merge_episode_final(root_dir: Path, episode: int, accepted_pass_by_segment: dict[int, int]) -> Path:
    manifest = load_manifest(root_dir, episode)
    merged_cues: list[SubtitleCue] = []
    offset_ms = 0

    for segment_index, entry in enumerate(manifest, start=1):
        pass_number = accepted_pass_by_segment[segment_index]
        cues = read_srt(pass_srt_path(root_dir, episode, pass_number, segment_index))
        for cue in cues:
            merged_cues.append(
                SubtitleCue(
                    start_ms=cue.start_ms + offset_ms,
                    end_ms=cue.end_ms + offset_ms,
                    text=cue.text,
                )
            )
        offset_ms += int(entry["durationMs"])

    output_dir = work_dir(root_dir, episode) / "merged_speech_only"
    output_dir.mkdir(parents=True, exist_ok=True)
    final_path = output_dir / f"[Judas] Teogonia - S01E{episode:02d}.final.srt"
    plain_path = output_dir / f"[Judas] Teogonia - S01E{episode:02d}.srt"
    rendered = render_srt(merged_cues)
    final_path.write_text(rendered, encoding="utf-8")
    plain_path.write_text(rendered, encoding="utf-8")
    return final_path
