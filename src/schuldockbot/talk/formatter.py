"""Talk message rendering for NEW/UPDATE notice changes."""

from __future__ import annotations

from datetime import datetime

from schuldockbot.state.models import ChangeKind, NoticeChange

DEFAULT_MAX_MESSAGE_LENGTH = 3500
_TRUNCATION_MARKER = "\n\n_(content truncated)_"


class TalkFormatterInputError(ValueError):
    """Raised when a change event cannot be rendered into an unambiguous Talk message."""


def render_notice_change(
    change: NoticeChange,
    *,
    max_length: int = DEFAULT_MAX_MESSAGE_LENGTH,
) -> str:
    """Render one ``NoticeChange`` into markdown-first Talk message text.

    The output remains plain-text legible and always includes a source link.
    """

    if max_length < 1:
        raise TalkFormatterInputError("max_length must be >= 1")

    kind = _normalize_change_kind(getattr(change, "kind", None))
    notice = getattr(change, "notice", None)
    if notice is None:
        raise TalkFormatterInputError("change.notice is required")

    notice_type = _require_text("notice.type", getattr(notice, "type", None))
    title = _require_text("notice.title", getattr(notice, "title", None))
    content = _require_text("notice.content", getattr(notice, "content", None))
    source_link = _require_text("notice.source_link", getattr(notice, "source_link", None))
    published_at = _require_datetime("notice.published_at", getattr(notice, "published_at", None))
    modified_at = _require_datetime("notice.modified_at", getattr(notice, "modified_at", None))

    published_text = published_at.isoformat()
    modified_text = modified_at.isoformat()

    prefix = (
        f"### {kind.value}: {notice_type}\n"
        f"**{title}**\n"
        f"Published: `{published_text}`\n"
        f"Modified: `{modified_text}`\n\n"
    )
    suffix = f"\n\nSource: [Open notice]({source_link})"

    return _truncate_with_source_link(
        prefix=prefix,
        content=content,
        suffix=suffix,
        max_length=max_length,
    )


def _truncate_with_source_link(*, prefix: str, content: str, suffix: str, max_length: int) -> str:
    """Truncate deterministically while preserving prefix context and source link."""

    full_message = f"{prefix}{content}{suffix}"
    if len(full_message) <= max_length:
        return full_message

    if len(suffix) > max_length:
        raise TalkFormatterInputError("max_length too small to retain source link")

    prefix_and_content_budget = max_length - len(suffix)
    if len(prefix) > prefix_and_content_budget:
        raise TalkFormatterInputError(
            "max_length too small to preserve message context and source link"
        )

    content_budget = prefix_and_content_budget - len(prefix)
    if content_budget <= 0:
        return f"{prefix}{suffix}"

    if content_budget > len(_TRUNCATION_MARKER):
        clipped_content_length = content_budget - len(_TRUNCATION_MARKER)
        clipped_content = content[:clipped_content_length]
        return f"{prefix}{clipped_content}{_TRUNCATION_MARKER}{suffix}"

    return f"{prefix}{content[:content_budget]}{suffix}"


def _normalize_change_kind(kind: object) -> ChangeKind:
    """Normalize raw change kind values into ``ChangeKind``."""

    if isinstance(kind, ChangeKind):
        return kind

    if not isinstance(kind, str):
        raise TalkFormatterInputError("kind must be a non-empty NEW/UPDATE value")

    normalized = kind.strip().upper()
    if not normalized:
        raise TalkFormatterInputError("kind must be a non-empty NEW/UPDATE value")

    try:
        return ChangeKind(normalized)
    except ValueError as exc:
        raise TalkFormatterInputError("kind must be NEW or UPDATE") from exc


def _require_text(field_name: str, value: object) -> str:
    """Validate required text values and trim surrounding whitespace."""

    if not isinstance(value, str):
        raise TalkFormatterInputError(f"{field_name} must be a non-empty string")

    normalized = value.strip()
    if not normalized:
        raise TalkFormatterInputError(f"{field_name} must be a non-empty string")

    return normalized


def _require_datetime(field_name: str, value: object) -> datetime:
    """Validate required datetime values."""

    if not isinstance(value, datetime):
        raise TalkFormatterInputError(f"{field_name} must be a datetime")
    return value
