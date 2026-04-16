"""Deterministic normalization helpers for ingestion records."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone, tzinfo
import hashlib
from html import unescape
from html.parser import HTMLParser
import re
from typing import Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .models import NoticeRecord, SourceMode

_FIELD_SEPARATOR = "\u241f"
_WHITESPACE_RE = re.compile(r"\s+")
_GERMAN_DATE_RE = re.compile(
    r"(?P<day>\d{1,2})\.(?P<month>\d{1,2})\.(?P<year>\d{4})(?:\s+(?P<hour>\d{1,2}):(?P<minute>\d{2}))?"
)
_GERMAN_DATE_NAMED_MONTH_RE = re.compile(
    r"(?P<day>\d{1,2})\.\s*(?P<month_name>januar|februar|märz|maerz|april|mai|juni|juli|august|september|oktober|november|dezember)\s+(?P<year>\d{4})(?:\s+(?P<hour>\d{1,2}):(?P<minute>\d{2}))?",
    re.IGNORECASE,
)
_GERMAN_MONTH_NAME_TO_INT = {
    "januar": 1,
    "februar": 2,
    "märz": 3,
    "maerz": 3,
    "april": 4,
    "mai": 5,
    "juni": 6,
    "juli": 7,
    "august": 8,
    "september": 9,
    "oktober": 10,
    "november": 11,
    "dezember": 12,
}


class _TextExtractor(HTMLParser):
    """Small HTML to text extractor without external dependencies."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data:
            self._parts.append(data)

    def as_text(self) -> str:
        return " ".join(self._parts)


def canonical_text(value: str | None) -> str:
    """Normalize text to stable whitespace and entity-decoded content."""

    if value is None:
        return ""
    decoded = unescape(value).replace("\xa0", " ")
    return _WHITESPACE_RE.sub(" ", decoded).strip()


def html_to_text(value: str | None) -> str:
    """Convert HTML fragments into stable plain text."""

    if not value:
        return ""
    parser = _TextExtractor()
    parser.feed(value)
    parser.close()
    return canonical_text(parser.as_text())


def parse_datetime(value: str | datetime, *, default_tz: ZoneInfo | tzinfo = UTC) -> datetime:
    """Parse datetimes into UTC-aware values for deterministic comparisons."""

    if isinstance(value, datetime):
        dt = value
    else:
        cleaned = canonical_text(value)
        if not cleaned:
            raise ValueError("datetime value is required")
        if cleaned.endswith("Z"):
            cleaned = f"{cleaned[:-1]}+00:00"
        dt = datetime.fromisoformat(cleaned)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=default_tz)

    return dt.astimezone(UTC)


def _last_sunday(year: int, month: int) -> date:
    """Return the last Sunday for a given month."""

    if month == 12:
        cursor = date(year, 12, 31)
    else:
        cursor = date(year, month + 1, 1) - timedelta(days=1)

    while cursor.weekday() != 6:
        cursor -= timedelta(days=1)
    return cursor


def _berlin_fallback_tz(dt: datetime) -> timezone:
    """Approximate Europe/Berlin offset when IANA tzdata is unavailable."""

    dst_start = _last_sunday(dt.year, 3)
    dst_end = _last_sunday(dt.year, 10)
    offset_hours = 2 if dst_start <= dt.date() < dst_end else 1
    return timezone(timedelta(hours=offset_hours), name="Europe/Berlin")


def parse_german_date(value: str, *, tz_name: str = "Europe/Berlin") -> datetime:
    """Parse German date labels like '15.04.2026 – anhaltend' or '15. April 2026 9:27'."""

    cleaned = canonical_text(html_to_text(value))
    match = _GERMAN_DATE_RE.search(cleaned)

    if match is not None:
        naive = datetime(
            year=int(match.group("year")),
            month=int(match.group("month")),
            day=int(match.group("day")),
            hour=int(match.group("hour") or 0),
            minute=int(match.group("minute") or 0),
        )
    else:
        named_match = _GERMAN_DATE_NAMED_MONTH_RE.search(cleaned)
        if named_match is None:
            raise ValueError(f"could not parse German date from {cleaned!r}")

        month_name = canonical_text(named_match.group("month_name")).lower()
        month = _GERMAN_MONTH_NAME_TO_INT.get(month_name)
        if month is None:
            raise ValueError(f"unsupported German month name {month_name!r}")

        naive = datetime(
            year=int(named_match.group("year")),
            month=month,
            day=int(named_match.group("day")),
            hour=int(named_match.group("hour") or 0),
            minute=int(named_match.group("minute") or 0),
        )

    try:
        local_tz: tzinfo = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        local_tz = _berlin_fallback_tz(naive) if tz_name == "Europe/Berlin" else UTC

    return naive.replace(tzinfo=local_tz).astimezone(UTC)


def stable_hash(parts: Iterable[str]) -> str:
    """Produce a deterministic SHA-256 hash from canonicalized parts."""

    canonical_parts = [canonical_text(part) for part in parts]
    joined = _FIELD_SEPARATOR.join(canonical_parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def build_source_id(
    *,
    source_mode: SourceMode,
    source_id: str | int | None,
    notice_type: str,
    title: str,
    published_at: datetime,
) -> str:
    """Return explicit source IDs when available, otherwise synthesize one deterministically."""

    explicit_id = canonical_text(str(source_id)) if source_id is not None else ""
    if explicit_id:
        return explicit_id

    digest = stable_hash(
        [
            source_mode.value,
            canonical_text(notice_type).lower(),
            canonical_text(title).lower(),
            published_at.isoformat(),
        ]
    )
    return f"{source_mode.value}:{digest}"


def build_revision_token(
    *,
    notice_type: str,
    title: str,
    content: str,
    published_at: datetime,
    modified_at: datetime,
) -> str:
    """Generate deterministic revision fingerprints from material normalized fields."""

    return stable_hash(
        [
            canonical_text(notice_type).lower(),
            canonical_text(title),
            canonical_text(content),
            published_at.isoformat(),
            modified_at.isoformat(),
        ]
    )


def normalize_notice(
    *,
    source_mode: SourceMode,
    source_id: str | int | None,
    notice_type: str,
    title: str,
    content: str,
    source_link: str,
    published_at: str | datetime,
    modified_at: str | datetime | None = None,
    content_is_html: bool = True,
) -> NoticeRecord:
    """Normalize source-specific values into a deterministic ``NoticeRecord``."""

    normalized_type = canonical_text(notice_type)
    normalized_title = canonical_text(title)
    normalized_content = html_to_text(content) if content_is_html else canonical_text(content)
    normalized_link = canonical_text(source_link)

    if not normalized_type:
        raise ValueError("notice_type is required")
    if not normalized_title:
        raise ValueError("title is required")
    if not normalized_content:
        raise ValueError("content is required")
    if not normalized_link:
        raise ValueError("source_link is required")

    normalized_published_at = parse_datetime(published_at)
    normalized_modified_at = (
        parse_datetime(modified_at) if modified_at not in (None, "") else normalized_published_at
    )

    normalized_source_id = build_source_id(
        source_mode=source_mode,
        source_id=source_id,
        notice_type=normalized_type,
        title=normalized_title,
        published_at=normalized_published_at,
    )

    revision_token = build_revision_token(
        notice_type=normalized_type,
        title=normalized_title,
        content=normalized_content,
        published_at=normalized_published_at,
        modified_at=normalized_modified_at,
    )

    return NoticeRecord(
        source_mode=source_mode,
        source_id=normalized_source_id,
        type=normalized_type,
        title=normalized_title,
        content=normalized_content,
        source_link=normalized_link,
        published_at=normalized_published_at,
        modified_at=normalized_modified_at,
        revision_token=revision_token,
    )
