from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
STATE_PATH = ROOT_DIR / ".codex" / "state.json"


@dataclass(frozen=True)
class StateItem:
    id: str
    episode: int
    pass_number: int
    segment: int
    path: str
    generated: bool
    exists: bool
    size: int
    quality_state: str


def load_state(state_path: Path = STATE_PATH) -> dict:
    return json.loads(state_path.read_text(encoding="utf-8"))


def state_items(data: dict) -> list[StateItem]:
    return [
        StateItem(
            id=item["id"],
            episode=item["episode"],
            pass_number=item["pass_number"],
            segment=item["segment"],
            path=item["path"],
            generated=bool(item["generated"]),
            exists=bool(item.get("exists", False)),
            size=int(item.get("size", 0)),
            quality_state=str(item.get("quality_state", "unchecked")),
        )
        for item in data.get("items", [])
    ]


def needs_generation(item: StateItem) -> bool:
    return (not item.generated) and item.quality_state != "hard_blocked"


def needs_regeneration(item: StateItem) -> bool:
    return item.generated and item.quality_state == "needs_regeneration"


def pending_items(items: list[StateItem]) -> list[StateItem]:
    missing_generation = sorted(
        (item for item in items if needs_generation(item)),
        key=lambda item: (item.pass_number, item.episode, item.segment),
    )
    if missing_generation:
        return missing_generation
    return sorted(
        (item for item in items if needs_regeneration(item)),
        key=lambda item: (item.pass_number, item.episode, item.segment),
    )


def choose_next_batch(items: list[StateItem], batch_size: int = 3) -> list[StateItem]:
    pending = pending_items(items)
    if not pending:
        return []

    first = pending[0]
    batch: list[StateItem] = []
    for item in pending:
        if item.pass_number != first.pass_number or item.episode != first.episode:
            if batch:
                break
            continue
        batch.append(item)
        if len(batch) >= batch_size:
            break
    return batch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Select the next rollout batch from .codex/state.json.")
    parser.add_argument("--state-path", type=Path, default=STATE_PATH)
    parser.add_argument("--batch-size", type=int, default=3)
    parser.add_argument("--ids-only", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    batch = choose_next_batch(state_items(load_state(args.state_path)), batch_size=args.batch_size)
    if args.ids_only:
        for item in batch:
            print(item.id)
        return 0
    print(json.dumps([asdict(item) for item in batch], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
