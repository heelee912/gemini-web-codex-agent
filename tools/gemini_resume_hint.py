from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable


TIME_CANDIDATE_RE = re.compile(
    r"(\d+\s*(?:시간|분|hours?|minutes?)|\b\d{1,2}:\d{2}\b|오전\s*\d{1,2}|오후\s*\d{1,2}|tomorrow|today|내일|오늘)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ResumeHint:
    matched_text: str
    wait_seconds: int | None
    resume_at_iso: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "matched_text": self.matched_text,
            "wait_seconds": self.wait_seconds,
            "resume_at_iso": self.resume_at_iso,
        }


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def looks_like_resume_hint_text(text: str) -> bool:
    normalized = normalize_text(text)
    if not normalized:
        return False
    lowered = normalized.lower()
    keywords = (
        "usage limit",
        "rate limit",
        "quota",
        "try again later",
        "come back",
        "after",
        "available again",
        "잠시 후 다시",
        "나중에 다시",
        "다시 시도",
        "이후",
        "한도",
        "제한",
        "pro",
    )
    return any(keyword in lowered for keyword in keywords) or bool(TIME_CANDIDATE_RE.search(normalized))


def candidate_texts(lines: Iterable[str], limit: int = 6) -> list[str]:
    picked: list[str] = []
    seen: set[str] = set()
    for raw in lines:
        normalized = normalize_text(raw)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        if looks_like_resume_hint_text(normalized):
            picked.append(normalized)
        if len(picked) >= limit:
            break
    return picked


def _duration_hint(text: str, now: datetime) -> ResumeHint | None:
    normalized = normalize_text(text)
    lowered = normalized.lower()

    total_seconds = 0
    matched = False

    korean = re.search(r"(?:(\d+)\s*시간)?\s*(?:(\d+)\s*분)?\s*(?:후|뒤)", normalized)
    if korean and (korean.group(1) or korean.group(2)):
        total_seconds = int(korean.group(1) or 0) * 3600 + int(korean.group(2) or 0) * 60
        matched = True

    english = re.search(
        r"\b(?:in|after)\b\s*(?:(\d+)\s*hours?)?\s*(?:(\d+)\s*minutes?)?",
        lowered,
        flags=re.IGNORECASE,
    )
    if english and (english.group(1) or english.group(2)):
        total_seconds = int(english.group(1) or 0) * 3600 + int(english.group(2) or 0) * 60
        matched = True

    if not matched or total_seconds <= 0:
        return None

    resume_at = now + timedelta(seconds=total_seconds)
    return ResumeHint(
        matched_text=normalized,
        wait_seconds=total_seconds,
        resume_at_iso=resume_at.isoformat(),
    )


def _clock_hint(text: str, now: datetime) -> ResumeHint | None:
    normalized = normalize_text(text)

    korean = re.search(
        r"(?:(오늘|내일)\s*)?(오전|오후)\s*(\d{1,2})(?::|시)?(\d{2})?",
        normalized,
    )
    if korean:
        day_marker, meridiem, hour_raw, minute_raw = korean.groups()
        hour = int(hour_raw)
        minute = int(minute_raw or 0)
        if meridiem == "오후" and hour < 12:
            hour += 12
        if meridiem == "오전" and hour == 12:
            hour = 0
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if day_marker == "내일":
            target += timedelta(days=1)
        elif target <= now:
            target += timedelta(days=1)
        wait_seconds = int((target - now).total_seconds())
        return ResumeHint(
            matched_text=normalized,
            wait_seconds=wait_seconds,
            resume_at_iso=target.isoformat(),
        )

    english = re.search(
        r"(?:(today|tomorrow)\s+)?(\d{1,2})(?::(\d{2}))\s*(am|pm)",
        normalized,
        flags=re.IGNORECASE,
    )
    if english:
        day_marker, hour_raw, minute_raw, meridiem = english.groups()
        hour = int(hour_raw)
        minute = int(minute_raw or 0)
        meridiem = meridiem.lower()
        if meridiem == "pm" and hour < 12:
            hour += 12
        if meridiem == "am" and hour == 12:
            hour = 0
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if day_marker and day_marker.lower() == "tomorrow":
            target += timedelta(days=1)
        elif target <= now:
            target += timedelta(days=1)
        wait_seconds = int((target - now).total_seconds())
        return ResumeHint(
            matched_text=normalized,
            wait_seconds=wait_seconds,
            resume_at_iso=target.isoformat(),
        )

    twenty_four = re.search(r"\b(\d{1,2}):(\d{2})\b", normalized)
    if twenty_four and any(token in normalized.lower() for token in ("after", "available", "다시", "이후", "부터")):
        hour = int(twenty_four.group(1))
        minute = int(twenty_four.group(2))
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        wait_seconds = int((target - now).total_seconds())
        return ResumeHint(
            matched_text=normalized,
            wait_seconds=wait_seconds,
            resume_at_iso=target.isoformat(),
        )

    return None


def extract_resume_hint(lines: Iterable[str], now: datetime | None = None) -> ResumeHint | None:
    reference_time = now or datetime.now().astimezone()
    candidates = candidate_texts(lines)
    for text in candidates:
        duration = _duration_hint(text, reference_time)
        if duration:
            return duration
    for text in candidates:
        clock = _clock_hint(text, reference_time)
        if clock:
            return clock
    return None
