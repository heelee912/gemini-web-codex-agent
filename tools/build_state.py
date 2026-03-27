from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from runtime_config import (
    discover_episode_numbers,
    discover_segment_numbers,
    episode_output_paths,
    pass_output_path,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
STATE_PATH = ROOT_DIR / ".codex" / "state.json"
ACCEPTED_QUALITY_STATES = {"accepted", "accepted_unanimous", "accepted_consistent"}
STATE_PRESERVE_FIELDS = {
    "supervisor_wait",
    "supervisor_wait_updated_at",
}
ITEM_PRESERVE_FIELDS = {
    "quality_state",
    "retry_count",
    "last_error",
    "accepted",
    "accepted_session_id",
    "accepted_at",
    "whisper_evidence",
    "recovery_attempts",
    "pending_recovery_action",
    "hard_blocked",
    "hard_block_reason",
    "hard_blocked_at",
    "supervisor_phase",
    "supervisor_phase_updated_at",
    "last_recovery_cause",
    "last_recovery_detail",
    "notes",
}


def pass_numbers() -> tuple[int, ...]:
    count = int(os.environ.get("ROLLOUT_PASS_COUNT", "3"))
    return tuple(range(1, count + 1))


def portable_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT_DIR))
    except ValueError:
        return str(path)


def normalize_quality_state(previous: dict[str, Any]) -> str:
    quality_state = str(previous.get("quality_state") or "unchecked")
    if quality_state != "accepted":
        return quality_state

    whisper_evidence = previous.get("whisper_evidence")
    if isinstance(whisper_evidence, dict):
        evidence_quality_state = str(whisper_evidence.get("quality_state") or "")
        if evidence_quality_state in ACCEPTED_QUALITY_STATES:
            return evidence_quality_state
    return quality_state


def load_existing_state(state_path: Path = STATE_PATH) -> dict[str, Any]:
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def item_path(episode: int, pass_number: int, segment: int) -> Path:
    return pass_output_path(ROOT_DIR, episode, pass_number, segment)


def final_paths(episode: int) -> tuple[Path, Path]:
    return episode_output_paths(ROOT_DIR, episode)


def build_item(previous: dict[str, Any], episode: int, pass_number: int, segment: int) -> dict[str, Any]:
    output_path = item_path(episode, pass_number, segment)
    exists = output_path.exists()
    size = output_path.stat().st_size if exists else 0
    generated = bool(exists and size > 0)
    item_id = f"E{episode:02d}-P{pass_number}-S{segment:02d}"

    item: dict[str, Any] = {
        "id": item_id,
        "episode": episode,
        "pass_number": pass_number,
        "segment": segment,
        "path": portable_path(output_path),
        "exists": exists,
        "size": size,
        "modified_at": datetime.fromtimestamp(output_path.stat().st_mtime).isoformat() if exists else None,
        "generated": generated,
        "verified_on_disk": generated,
        "selection_order": {
            "pass_number": pass_number,
            "episode": episode,
            "segment": segment,
        },
    }

    for field in ITEM_PRESERVE_FIELDS:
        if field in previous:
            item[field] = previous[field]

    if "retry_count" not in item:
        item["retry_count"] = 0
    if "last_error" not in item:
        item["last_error"] = None
    if "quality_state" not in item:
        item["quality_state"] = "unchecked"
    else:
        item["quality_state"] = normalize_quality_state(item)

    if "supervisor_phase" not in item:
        item["supervisor_phase"] = "generated" if generated else "generation_pending"

    if item.get("accepted") and not generated:
        item["accepted"] = False
        item.pop("accepted_session_id", None)
        item.pop("accepted_at", None)
        if item.get("quality_state") in ACCEPTED_QUALITY_STATES:
            item["quality_state"] = "unchecked"
        if item.get("supervisor_phase") == "accepted":
            item["supervisor_phase"] = "generation_pending"

    if item.get("accepted") and generated:
        item["supervisor_phase"] = "accepted"
    elif item.get("quality_state") == "hard_blocked":
        item["supervisor_phase"] = "hard_blocked"
    elif generated and item.get("supervisor_phase") == "generation_pending":
        item["supervisor_phase"] = "generated"

    return item


def build_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {"total": len(items), "generated": 0, "remaining": 0}
    pass_counts = defaultdict(lambda: {"total": 0, "generated": 0, "remaining": 0})
    episode_counts = defaultdict(lambda: {"total": 0, "generated": 0, "remaining": 0})

    for item in items:
        generated = bool(item["generated"])
        summary["generated"] += int(generated)
        summary["remaining"] += int(not generated)
        pass_key = f"pass{item['pass_number']}"
        episode_key = f"E{item['episode']:02d}"
        pass_counts[pass_key]["total"] += 1
        pass_counts[pass_key]["generated"] += int(generated)
        pass_counts[pass_key]["remaining"] += int(not generated)
        episode_counts[episode_key]["total"] += 1
        episode_counts[episode_key]["generated"] += int(generated)
        episode_counts[episode_key]["remaining"] += int(not generated)

    summary["passes"] = dict(pass_counts)
    summary["episodes"] = dict(episode_counts)
    return summary


def build_segment_groups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items_by_key = {(item["episode"], item["segment"], item["pass_number"]): item for item in items}
    groups: list[dict[str, Any]] = []
    episodes = sorted({item["episode"] for item in items})
    available_passes = pass_numbers()
    for episode in episodes:
        segments = sorted({item["segment"] for item in items if item["episode"] == episode})
        for segment in segments:
            group_items = [
                items_by_key[(episode, segment, pass_number)]
                for pass_number in available_passes
                if (episode, segment, pass_number) in items_by_key
            ]
            generated_passes = [item["pass_number"] for item in group_items if item["generated"]]
            accepted_items = [item for item in group_items if item.get("accepted") and item.get("generated")]
            accepted_item = accepted_items[0] if accepted_items else None
            whisper_evidence_count = sum(1 for item in group_items if item.get("whisper_evidence"))
            needs_regeneration = any(item.get("quality_state") == "needs_regeneration" for item in group_items)
            hard_blocked_items = [item["id"] for item in group_items if item.get("quality_state") == "hard_blocked"]
            expected_passes = len(available_passes)
            groups.append(
                {
                    "id": f"E{episode:02d}-S{segment:02d}",
                    "episode": episode,
                    "segment": segment,
                    "generation_count": len(generated_passes),
                    "generated_passes": generated_passes,
                    "has_all_passes": len(generated_passes) == expected_passes,
                    "accepted": bool(accepted_item),
                    "accepted_pass_number": accepted_item["pass_number"] if accepted_item else None,
                    "accepted_session_id": accepted_item.get("accepted_session_id") if accepted_item else None,
                    "accepted_at": accepted_item.get("accepted_at") if accepted_item else None,
                    "quality_state": "hard_blocked" if hard_blocked_items else (
                        accepted_item.get("quality_state", "accepted") if accepted_item else (
                            "needs_regeneration"
                            if needs_regeneration
                            else ("ready_for_acceptance" if len(generated_passes) == expected_passes else "generation_pending")
                        )
                    ),
                    "whisper_evidence_count": whisper_evidence_count,
                    "ready_for_acceptance": len(generated_passes) == expected_passes and not accepted_item and not needs_regeneration,
                    "needs_regeneration": needs_regeneration,
                    "hard_blocked": bool(hard_blocked_items),
                    "hard_blocked_items": hard_blocked_items,
                }
            )
    return groups


def build_episode_outputs(episodes: list[int]) -> list[dict[str, Any]]:
    outputs: list[dict[str, Any]] = []
    for episode in episodes:
        final_path, plain_path = final_paths(episode)
        final_exists = final_path.exists()
        plain_exists = plain_path.exists()
        final_size = final_path.stat().st_size if final_exists else 0
        plain_size = plain_path.stat().st_size if plain_exists else 0
        chosen = final_path if final_exists else plain_path
        outputs.append(
            {
                "episode": episode,
                "final_path": portable_path(final_path),
                "plain_path": portable_path(plain_path),
                "selected_path": portable_path(chosen),
                "exists": final_exists,
                "size": final_size,
                "final_exists": final_exists,
                "final_size": final_size,
                "plain_exists": plain_exists,
                "plain_size": plain_size,
            }
        )
    return outputs


def rebuild_state(state_path: Path = STATE_PATH) -> dict[str, Any]:
    previous = load_existing_state(state_path)
    previous_items = {
        item["id"]: item
        for item in previous.get("items", [])
        if isinstance(item, dict) and item.get("id")
    }

    episodes = discover_episode_numbers(ROOT_DIR)
    available_passes = pass_numbers()
    items: list[dict[str, Any]] = []
    for episode in episodes:
        segments = discover_segment_numbers(ROOT_DIR, episode)
        for pass_number in available_passes:
            for segment in segments:
                item_id = f"E{episode:02d}-P{pass_number}-S{segment:02d}"
                items.append(build_item(previous_items.get(item_id, {}), episode, pass_number, segment))

    segment_groups = build_segment_groups(items)
    episode_outputs = build_episode_outputs(episodes)
    summary = build_summary(items)
    summary["segment_groups_total"] = len(segment_groups)
    summary["segment_groups_complete"] = sum(1 for group in segment_groups if group["has_all_passes"])
    summary["segment_groups_accepted"] = sum(1 for group in segment_groups if group["accepted"])
    summary["segment_groups_accepted_unanimous"] = sum(
        1 for group in segment_groups if group["quality_state"] == "accepted_unanimous"
    )
    summary["segment_groups_accepted_consistent"] = sum(
        1 for group in segment_groups if group["quality_state"] == "accepted_consistent"
    )
    summary["segment_groups_ready_for_acceptance"] = sum(1 for group in segment_groups if group["ready_for_acceptance"])
    summary["segment_groups_needing_regeneration"] = sum(1 for group in segment_groups if group["needs_regeneration"])
    summary["segment_groups_hard_blocked"] = sum(1 for group in segment_groups if group.get("hard_blocked"))
    summary["episode_finals_total"] = len(episode_outputs)
    summary["episode_finals_complete"] = sum(
        1 for output in episode_outputs if output["final_exists"] and output["final_size"] > 0
    )

    state: dict[str, Any] = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "session_anchor": os.environ.get("CODEX_THREAD_ID"),
        "source_of_truth": "disk",
        "summary": summary,
        "items": items,
        "segment_groups": segment_groups,
        "episode_outputs": episode_outputs,
    }
    for field in STATE_PRESERVE_FIELDS:
        if field in previous:
            state[field] = previous[field]
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return state


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild .codex/state.json from on-disk subtitle outputs.")
    parser.add_argument("--state-path", type=Path, default=STATE_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state = rebuild_state(args.state_path)
    print(json.dumps(state["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
