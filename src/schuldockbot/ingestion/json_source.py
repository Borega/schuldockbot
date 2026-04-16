"""JSON source parsing for Schuldock issue ingestion."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from enum import StrEnum
import json

from .models import NoticeRecord, SourceMode
from .normalize import canonical_text, normalize_notice

ISSUES_ENDPOINT = "wp-json/schuldock/v1/issues"


class JsonFailureClass(StrEnum):
    """Classified JSON failure categories used by source selection."""

    FETCH = "fetch"
    DECODE = "decode"
    SCHEMA = "schema"


class JsonSourceError(RuntimeError):
    """Base JSON ingestion exception with safe diagnostic metadata."""

    failure_class: JsonFailureClass = JsonFailureClass.SCHEMA

    def __init__(self, message: str, *, context: Mapping[str, str] | None = None) -> None:
        super().__init__(message)
        self.context = dict(context or {})

    def to_safe_dict(self) -> dict[str, str]:
        """Return log-safe diagnostics without embedding raw payload content."""

        diagnostic = {
            "failure_class": self.failure_class.value,
            "message": str(self),
        }
        diagnostic.update(self.context)
        return diagnostic


class JsonFetchError(JsonSourceError):
    """Raised when obtaining the JSON payload fails."""

    failure_class = JsonFailureClass.FETCH


class JsonDecodeError(JsonSourceError):
    """Raised when JSON payload decoding fails."""

    failure_class = JsonFailureClass.DECODE


class JsonSchemaError(JsonSourceError):
    """Raised when a decoded JSON payload does not match required fields."""

    failure_class = JsonFailureClass.SCHEMA


def _require_mapping(
    value: object,
    *,
    path: str,
    issue_index: int,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise JsonSchemaError(
            f"Expected object at '{path}'",
            context={"path": path, "issue_index": str(issue_index)},
        )
    return value


def _require_key(mapping: Mapping[str, object], *, key: str, path: str, issue_index: int) -> object:
    if key not in mapping:
        raise JsonSchemaError(
            f"Missing required key '{path}.{key}'",
            context={"path": f"{path}.{key}", "issue_index": str(issue_index)},
        )
    return mapping[key]


def _require_text(
    mapping: Mapping[str, object],
    *,
    key: str,
    path: str,
    issue_index: int,
) -> str:
    value = _require_key(mapping, key=key, path=path, issue_index=issue_index)
    if not isinstance(value, str):
        raise JsonSchemaError(
            f"Expected text at '{path}.{key}'",
            context={"path": f"{path}.{key}", "issue_index": str(issue_index)},
        )

    cleaned = canonical_text(value)
    if not cleaned:
        raise JsonSchemaError(
            f"Field '{path}.{key}' must not be empty",
            context={"path": f"{path}.{key}", "issue_index": str(issue_index)},
        )
    return value


def _require_source_id(issue: Mapping[str, object], *, issue_index: int) -> str | int:
    source_id = _require_key(issue, key="id", path="issue", issue_index=issue_index)
    if isinstance(source_id, bool) or not isinstance(source_id, (str, int)):
        raise JsonSchemaError(
            "Expected string/int at 'issue.id'",
            context={"path": "issue.id", "issue_index": str(issue_index)},
        )

    if isinstance(source_id, str) and not canonical_text(source_id):
        raise JsonSchemaError(
            "Field 'issue.id' must not be empty",
            context={"path": "issue.id", "issue_index": str(issue_index)},
        )
    return source_id


def _require_issue_type_text(details: Mapping[str, object], *, issue_index: int) -> str:
    raw_issue_type = _require_key(details, key="issue_type", path="issue.details", issue_index=issue_index)

    if isinstance(raw_issue_type, str):
        cleaned = canonical_text(raw_issue_type)
        if cleaned:
            return cleaned
        raise JsonSchemaError(
            "Field 'issue.details.issue_type' must not be empty",
            context={"path": "issue.details.issue_type", "issue_index": str(issue_index)},
        )

    if isinstance(raw_issue_type, Mapping):
        return _require_text(
            raw_issue_type,
            key="name",
            path="issue.details.issue_type",
            issue_index=issue_index,
        )

    raise JsonSchemaError(
        "Expected text/object at 'issue.details.issue_type'",
        context={"path": "issue.details.issue_type", "issue_index": str(issue_index)},
    )


def parse_issue_record(issue: Mapping[str, object], *, issue_index: int) -> NoticeRecord:
    """Parse one decoded issue object into a normalized notice record."""

    source_id = _require_source_id(issue, issue_index=issue_index)
    published_at = _require_text(issue, key="date", path="issue", issue_index=issue_index)
    source_link = _require_text(issue, key="link", path="issue", issue_index=issue_index)

    raw_modified = issue.get("modified")
    modified_at: str | None
    if raw_modified in (None, ""):
        modified_at = None
    elif isinstance(raw_modified, str):
        modified_at = raw_modified
    else:
        raise JsonSchemaError(
            "Expected text at 'issue.modified'",
            context={"path": "issue.modified", "issue_index": str(issue_index)},
        )

    title = _require_mapping(
        _require_key(issue, key="title", path="issue", issue_index=issue_index),
        path="issue.title",
        issue_index=issue_index,
    )
    content = _require_mapping(
        _require_key(issue, key="content", path="issue", issue_index=issue_index),
        path="issue.content",
        issue_index=issue_index,
    )
    details = _require_mapping(
        _require_key(issue, key="details", path="issue", issue_index=issue_index),
        path="issue.details",
        issue_index=issue_index,
    )

    return normalize_notice(
        source_mode=SourceMode.JSON,
        source_id=source_id,
        notice_type=_require_issue_type_text(details, issue_index=issue_index),
        title=_require_text(title, key="rendered", path="issue.title", issue_index=issue_index),
        content=_require_text(content, key="rendered", path="issue.content", issue_index=issue_index),
        source_link=source_link,
        published_at=published_at,
        modified_at=modified_at,
        content_is_html=True,
    )


def parse_issues_payload(payload: object) -> list[NoticeRecord]:
    """Validate a decoded payload and return normalized notice records."""

    if not isinstance(payload, list):
        raise JsonSchemaError(
            "Expected payload to be a JSON array",
            context={"path": "payload"},
        )

    records: list[NoticeRecord] = []
    for issue_index, raw_issue in enumerate(payload):
        issue = _require_mapping(raw_issue, path="issue", issue_index=issue_index)
        records.append(parse_issue_record(issue, issue_index=issue_index))

    return records


def decode_issues_payload(payload: str | bytes | bytearray) -> list[NoticeRecord]:
    """Decode JSON payload text/bytes and map it into notice records."""

    try:
        decoded = json.loads(payload)
    except (TypeError, ValueError, UnicodeDecodeError) as exc:
        raise JsonDecodeError(
            "Failed to decode schuldock issues payload",
            context={"endpoint": ISSUES_ENDPOINT},
        ) from exc

    return parse_issues_payload(decoded)


def fetch_and_parse_issues(fetcher: Callable[[], str | bytes | bytearray]) -> list[NoticeRecord]:
    """Fetch a JSON payload via dependency injection and parse normalized records."""

    try:
        payload = fetcher()
    except JsonSourceError:
        raise
    except Exception as exc:  # pragma: no cover - guarded by tests via a synthetic fetcher.
        raise JsonFetchError(
            "Failed to fetch schuldock issues payload",
            context={"endpoint": ISSUES_ENDPOINT},
        ) from exc

    return decode_issues_payload(payload)


__all__ = [
    "ISSUES_ENDPOINT",
    "JsonFailureClass",
    "JsonSourceError",
    "JsonFetchError",
    "JsonDecodeError",
    "JsonSchemaError",
    "parse_issue_record",
    "parse_issues_payload",
    "decode_issues_payload",
    "fetch_and_parse_issues",
]
