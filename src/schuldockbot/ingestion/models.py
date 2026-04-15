"""Normalized ingestion models shared by JSON and HTML sources."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class SourceMode(StrEnum):
    """How a notice was ingested."""

    JSON = "json"
    HTML_FALLBACK = "html_fallback"


@dataclass(frozen=True, slots=True)
class NoticeRecord:
    """Canonical notice representation used by downstream processing."""

    source_mode: SourceMode
    source_id: str
    type: str
    title: str
    content: str
    source_link: str
    published_at: datetime
    modified_at: datetime
    revision_token: str

    def to_safe_dict(self) -> dict[str, str]:
        """Return a log-safe string representation for diagnostics and tests."""

        return {
            "source_mode": self.source_mode.value,
            "source_id": self.source_id,
            "type": self.type,
            "title": self.title,
            "source_link": self.source_link,
            "published_at": self.published_at.isoformat(),
            "modified_at": self.modified_at.isoformat(),
            "revision_token": self.revision_token,
        }
