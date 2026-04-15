"""JSON-first ingestion source selection with HTML fallback diagnostics."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .html_source import HTML_FALLBACK_URL, HtmlSourceError, fetch_and_parse_html
from .json_source import JsonFailureClass, JsonSourceError, fetch_and_parse_issues
from .models import NoticeRecord, SourceMode


@dataclass(frozen=True, slots=True)
class SourceSelectionResult:
    """Normalized records plus selector-level diagnostics for one ingestion attempt."""

    records: list[NoticeRecord]
    source_mode: SourceMode
    fallback_reason: str | None
    json_failure_class: JsonFailureClass | None = None
    json_failure: dict[str, str] | None = None

    @property
    def record_count(self) -> int:
        """Number of normalized records produced in this attempt."""

        return len(self.records)

    def to_safe_dict(self) -> dict[str, Any]:
        """Return redaction-safe selector diagnostics for logs/tests."""

        return {
            "source_mode": self.source_mode.value,
            "fallback_reason": self.fallback_reason,
            "record_count": self.record_count,
            "json_failure_class": (
                self.json_failure_class.value if self.json_failure_class is not None else None
            ),
            "json_failure": dict(self.json_failure) if self.json_failure is not None else None,
        }


class SourceSelectionError(RuntimeError):
    """Raised when JSON primary and HTML fallback both fail in one cycle."""

    def __init__(self, *, json_error: JsonSourceError, html_error: HtmlSourceError) -> None:
        super().__init__("JSON source failed and HTML fallback failed")
        self.json_error = json_error
        self.html_error = html_error

    def to_safe_dict(self) -> dict[str, Any]:
        """Return classified dual-source failure diagnostics without payload leakage."""

        return {
            "source_mode": SourceMode.HTML_FALLBACK.value,
            "fallback_reason": self.json_error.failure_class.value,
            "json_failure": self.json_error.to_safe_dict(),
            "html_failure": self.html_error.to_safe_dict(),
        }


def fetch_notices(
    json_fetcher: Callable[[], str | bytes | bytearray],
    html_fetcher: Callable[[], str | bytes | bytearray],
    *,
    html_base_url: str = HTML_FALLBACK_URL,
) -> SourceSelectionResult:
    """Fetch notices with JSON-first control flow and same-cycle HTML fallback."""

    try:
        json_records = fetch_and_parse_issues(json_fetcher)
    except JsonSourceError as json_error:
        fallback_reason = json_error.failure_class.value
        json_failure = json_error.to_safe_dict()

        try:
            html_records = fetch_and_parse_html(html_fetcher, base_url=html_base_url)
        except HtmlSourceError as html_error:
            raise SourceSelectionError(json_error=json_error, html_error=html_error) from html_error

        return SourceSelectionResult(
            records=html_records,
            source_mode=SourceMode.HTML_FALLBACK,
            fallback_reason=fallback_reason,
            json_failure_class=json_error.failure_class,
            json_failure=json_failure,
        )

    return SourceSelectionResult(
        records=json_records,
        source_mode=SourceMode.JSON,
        fallback_reason=None,
    )


__all__ = [
    "SourceSelectionResult",
    "SourceSelectionError",
    "fetch_notices",
]
