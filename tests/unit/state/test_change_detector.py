from __future__ import annotations

from datetime import datetime, timezone

import pytest

from schuldockbot.ingestion.models import NoticeRecord, SourceMode
from schuldockbot.state import ChangeKind, InvalidProcessedNoticeError, detect_notice_changes


def _notice(
    *,
    source_id: object = "notice-1",
    revision_token: object = "rev-1",
    source_link: str = "https://schuldock.invalid/notices/1",
) -> NoticeRecord:
    timestamp = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    return NoticeRecord(
        source_mode=SourceMode.JSON,
        source_id=source_id,  # type: ignore[arg-type]
        type="announcement",
        title="Title",
        content="Body",
        source_link=source_link,
        published_at=timestamp,
        modified_at=timestamp,
        revision_token=revision_token,  # type: ignore[arg-type]
    )


def test_unseen_source_emits_new_change() -> None:
    lookup_calls: list[str] = []

    def lookup(source_id: str) -> str | None:
        lookup_calls.append(source_id)
        return None

    changes = detect_notice_changes([_notice()], revision_lookup=lookup)

    assert len(changes) == 1
    assert changes[0].kind is ChangeKind.NEW
    assert changes[0].notice.source_id == "notice-1"
    assert changes[0].previous_revision_token is None
    assert lookup_calls == ["notice-1"]


def test_seen_changed_revision_emits_update_change() -> None:
    changes = detect_notice_changes(
        [_notice(source_id="notice-9", revision_token="rev-new")],
        revision_lookup=lambda _: "rev-old",
    )

    assert len(changes) == 1
    assert changes[0].kind is ChangeKind.UPDATE
    assert changes[0].notice.source_id == "notice-9"
    assert changes[0].previous_revision_token == "rev-old"


def test_seen_unchanged_revision_is_suppressed() -> None:
    changes = detect_notice_changes(
        [_notice(source_id="notice-1", revision_token="rev-1")],
        revision_lookup=lambda _: "rev-1",
    )

    assert changes == []


def test_empty_notice_batch_returns_no_changes_and_no_lookups() -> None:
    lookup_calls: list[str] = []

    def lookup(source_id: str) -> str | None:
        lookup_calls.append(source_id)
        return None

    changes = detect_notice_changes([], revision_lookup=lookup)

    assert changes == []
    assert lookup_calls == []


def test_invalid_notice_identity_fields_fail_fast() -> None:
    with pytest.raises(InvalidProcessedNoticeError, match="source_id"):
        detect_notice_changes([_notice(source_id="   ")], revision_lookup=lambda _: None)

    with pytest.raises(InvalidProcessedNoticeError, match="revision_token"):
        detect_notice_changes([_notice(revision_token=None)], revision_lookup=lambda _: None)


def test_duplicate_records_in_batch_are_safely_deduped_when_identical() -> None:
    lookup_calls: list[str] = []

    def lookup(source_id: str) -> str | None:
        lookup_calls.append(source_id)
        return None

    changes = detect_notice_changes(
        [
            _notice(source_id="notice-1", revision_token="rev-1"),
            _notice(source_id="notice-1", revision_token="rev-1"),
        ],
        revision_lookup=lookup,
    )

    assert len(changes) == 1
    assert changes[0].kind is ChangeKind.NEW
    assert lookup_calls == ["notice-1"]


def test_duplicate_records_in_batch_with_conflicting_revision_are_rejected() -> None:
    with pytest.raises(InvalidProcessedNoticeError, match="conflicting revision_token"):
        detect_notice_changes(
            [
                _notice(source_id="notice-1", revision_token="rev-1"),
                _notice(source_id="notice-1", revision_token="rev-2"),
            ],
            revision_lookup=lambda _: None,
        )


def test_lookup_failures_propagate_and_abort_cycle() -> None:
    def lookup(source_id: str) -> str | None:
        if source_id == "notice-2":
            raise RuntimeError("db-down")
        return None

    with pytest.raises(RuntimeError, match="db-down"):
        detect_notice_changes(
            [
                _notice(source_id="notice-1", revision_token="rev-1"),
                _notice(source_id="notice-2", revision_token="rev-2"),
            ],
            revision_lookup=lookup,
        )


def test_malformed_lookup_values_are_rejected() -> None:
    with pytest.raises(InvalidProcessedNoticeError, match="non-string token"):
        detect_notice_changes(
            [_notice(source_id="notice-1", revision_token="rev-1")],
            revision_lookup=lambda _: 123,
        )

    with pytest.raises(InvalidProcessedNoticeError, match="empty token"):
        detect_notice_changes(
            [_notice(source_id="notice-1", revision_token="rev-1")],
            revision_lookup=lambda _: "   ",
        )


def test_change_to_safe_dict_exposes_only_safe_diagnostics() -> None:
    changes = detect_notice_changes(
        [_notice(source_id="notice-1", revision_token="rev-2")],
        revision_lookup=lambda _: "rev-1",
    )

    safe = changes[0].to_safe_dict()

    assert safe == {
        "kind": "UPDATE",
        "source_id": "notice-1",
        "revision_token": "rev-2",
        "source_link": "https://schuldock.invalid/notices/1",
        "previous_revision_token": "rev-1",
    }
    assert "content" not in safe
    assert "title" not in safe
