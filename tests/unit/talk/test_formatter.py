from __future__ import annotations

from datetime import UTC, datetime

import pytest

from schuldockbot.ingestion.models import NoticeRecord, SourceMode
from schuldockbot.state.models import ChangeKind, NoticeChange
from schuldockbot.talk import TalkFormatterInputError, render_notice_change


def _build_notice(*, content: str = "School closes early today") -> NoticeRecord:
    published_at = datetime(2026, 4, 15, 10, 30, tzinfo=UTC)
    modified_at = datetime(2026, 4, 15, 11, 0, tzinfo=UTC)

    return NoticeRecord(
        source_mode=SourceMode.JSON,
        source_id="notice-123",
        type="Betriebsmitteilung",
        title="Kurzfristige Schulschließung",
        content=content,
        source_link="https://schuldock.example/notices/123",
        published_at=published_at,
        modified_at=modified_at,
        revision_token="rev-123",
    )


def test_render_notice_change_new_contains_required_sections() -> None:
    change = NoticeChange(kind=ChangeKind.NEW, notice=_build_notice())

    rendered = render_notice_change(change)

    assert "### NEW: Betriebsmitteilung" in rendered
    assert "**Kurzfristige Schulschließung**" in rendered
    assert "Published: `2026-04-15T10:30:00+00:00`" in rendered
    assert "Modified: `2026-04-15T11:00:00+00:00`" in rendered
    assert "School closes early today" in rendered
    assert "Source: [Open notice](https://schuldock.example/notices/123)" in rendered


def test_render_notice_change_update_uses_explicit_update_label() -> None:
    change = NoticeChange(kind=ChangeKind.UPDATE, notice=_build_notice())

    rendered = render_notice_change(change)

    assert rendered.startswith("### UPDATE: Betriebsmitteilung")
    assert "### NEW:" not in rendered


def test_render_notice_change_is_plain_text_legible_without_markdown_rendering() -> None:
    change = NoticeChange(kind=ChangeKind.NEW, notice=_build_notice())

    rendered = render_notice_change(change)

    for snippet in (
        "NEW",
        "Betriebsmitteilung",
        "Kurzfristige Schulschließung",
        "Published:",
        "Modified:",
        "School closes early today",
        "Source:",
    ):
        assert snippet in rendered


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("type", " "),
        ("title", ""),
        ("content", "\n\t"),
        ("source_link", ""),
    ],
)
def test_render_notice_change_rejects_empty_required_notice_fields(
    field_name: str,
    value: str,
) -> None:
    notice = _build_notice()
    mutated_notice = NoticeRecord(
        source_mode=notice.source_mode,
        source_id=notice.source_id,
        type=value if field_name == "type" else notice.type,
        title=value if field_name == "title" else notice.title,
        content=value if field_name == "content" else notice.content,
        source_link=value if field_name == "source_link" else notice.source_link,
        published_at=notice.published_at,
        modified_at=notice.modified_at,
        revision_token=notice.revision_token,
    )

    with pytest.raises(TalkFormatterInputError, match=field_name):
        render_notice_change(NoticeChange(kind=ChangeKind.NEW, notice=mutated_notice))


def test_render_notice_change_rejects_unknown_kind() -> None:
    change = NoticeChange(kind="unknown", notice=_build_notice())  # type: ignore[arg-type]

    with pytest.raises(TalkFormatterInputError, match="kind must be NEW or UPDATE"):
        render_notice_change(change)


def test_render_notice_change_exact_threshold_is_not_truncated() -> None:
    change = NoticeChange(kind=ChangeKind.NEW, notice=_build_notice(content="abcde"))

    full = render_notice_change(change, max_length=10_000)
    exact = render_notice_change(change, max_length=len(full))

    assert exact == full
    assert "_(content truncated)_" not in exact


def test_render_notice_change_over_limit_by_one_truncates_deterministically_and_keeps_source() -> None:
    content = "A" * 200
    change = NoticeChange(kind=ChangeKind.UPDATE, notice=_build_notice(content=content))

    full = render_notice_change(change, max_length=10_000)
    threshold = len(full) - 1

    baseline = render_notice_change(change, max_length=threshold)
    second = render_notice_change(change, max_length=threshold)

    assert baseline == second
    assert len(baseline) <= threshold
    assert "_(content truncated)_" in baseline
    assert "Source: [Open notice](https://schuldock.example/notices/123)" in baseline
