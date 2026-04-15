from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from schuldockbot.ingestion.models import NoticeRecord, SourceMode
from schuldockbot.state.models import ChangeKind, NoticeChange
from schuldockbot.talk import (
    TalkDeliveryAckError,
    TalkFailureClass,
    TalkPostError,
    TalkPostResult,
    deliver_notice_changes,
)


@dataclass(slots=True)
class _FakeTalkClient:
    failures_by_source_id: dict[str, Exception] | None = None
    calls: list[tuple[str, str]] | None = None

    def __post_init__(self) -> None:
        self.calls = []
        if self.failures_by_source_id is None:
            self.failures_by_source_id = {}

    def post_message(self, *, source_id: str, message: str) -> TalkPostResult:
        self.calls.append((source_id, message))

        maybe_error = self.failures_by_source_id.get(source_id)
        if maybe_error is not None:
            raise maybe_error

        return TalkPostResult(
            source_id=source_id,
            status_code=201,
            endpoint_path="/ocs/v2.php/apps/spreed/api/v1/chat/abc***23",
            ocs_status="ok",
            ocs_status_code=100,
        )


@dataclass(slots=True)
class _RecordingStore:
    raise_on_ack: Exception | None = None
    calls: list[list[object]] | None = None

    def __post_init__(self) -> None:
        self.calls = []

    def mark_processed_batch(self, notices) -> int:
        materialized = list(notices)
        self.calls.append(materialized)

        if self.raise_on_ack is not None:
            raise self.raise_on_ack

        return len(materialized)


def _build_notice(index: int, *, revision_suffix: str = "A") -> NoticeRecord:
    published_at = datetime(2026, 4, 15, 9, 0, tzinfo=UTC)
    modified_at = published_at + timedelta(minutes=index)

    return NoticeRecord(
        source_mode=SourceMode.JSON,
        source_id=f"notice-{index}",
        type="Betriebsmitteilung",
        title=f"Notice {index}",
        content=f"Content for notice {index}",
        source_link=f"https://schuldock.example/notices/{index}",
        published_at=published_at,
        modified_at=modified_at,
        revision_token=f"rev-{index}-{revision_suffix}",
    )


def _build_change(index: int, *, kind: ChangeKind = ChangeKind.NEW) -> NoticeChange:
    return NoticeChange(kind=kind, notice=_build_notice(index))


def _post_error(
    *,
    source_id: str,
    failure_class: TalkFailureClass,
    retryable: bool,
    status_code: int | None,
    detail: str,
) -> TalkPostError:
    return TalkPostError(
        failure_class=failure_class,
        source_id=source_id,
        endpoint_path="/ocs/v2.php/apps/spreed/api/v1/chat/abc***23",
        detail=detail,
        status_code=status_code,
        retryable=retryable,
    )


def test_delivery_all_success_posts_in_order_and_acks_notice_objects() -> None:
    changes = [
        _build_change(1, kind=ChangeKind.NEW),
        _build_change(2, kind=ChangeKind.UPDATE),
    ]
    client = _FakeTalkClient()
    store = _RecordingStore()

    summary = deliver_notice_changes(
        changes,
        talk_client=client,
        processed_store=store,
    )

    assert [source_id for source_id, _ in client.calls] == ["notice-1", "notice-2"]
    assert len(store.calls) == 1
    assert store.calls[0] == [change.notice for change in changes]
    assert all(isinstance(entry, NoticeRecord) for entry in store.calls[0])

    assert summary.to_safe_dict() == {
        "attempted": 2,
        "posted": 2,
        "failed": 0,
        "acked": 2,
        "failed_source_ids": [],
        "failures": [],
    }


def test_delivery_partial_post_failure_acks_only_posted_subset() -> None:
    changes = [_build_change(1), _build_change(2), _build_change(3)]
    client = _FakeTalkClient(
        failures_by_source_id={
            "notice-2": _post_error(
                source_id="notice-2",
                failure_class=TalkFailureClass.RATE_LIMITED,
                retryable=True,
                status_code=429,
                detail="Talk post failed with HTTP 429",
            )
        }
    )
    store = _RecordingStore()

    summary = deliver_notice_changes(
        changes,
        talk_client=client,
        processed_store=store,
    )

    safe = summary.to_safe_dict()
    assert safe["attempted"] == 3
    assert safe["posted"] == 2
    assert safe["failed"] == 1
    assert safe["acked"] == 2
    assert safe["failed_source_ids"] == ["notice-2"]
    assert safe["failures"] == [
        {
            "source_id": "notice-2",
            "stage": "post",
            "error_class": "rate_limited",
            "detail": "Talk post failed with HTTP 429",
            "retryable": True,
            "status_code": 429,
        }
    ]

    assert len(store.calls) == 1
    acked_source_ids = [notice.source_id for notice in store.calls[0]]
    assert acked_source_ids == ["notice-1", "notice-3"]


def test_delivery_total_post_failure_skips_ack_entirely() -> None:
    changes = [_build_change(1), _build_change(2)]
    client = _FakeTalkClient(
        failures_by_source_id={
            "notice-1": _post_error(
                source_id="notice-1",
                failure_class=TalkFailureClass.AUTH,
                retryable=False,
                status_code=403,
                detail="Talk post failed with HTTP 403",
            ),
            "notice-2": _post_error(
                source_id="notice-2",
                failure_class=TalkFailureClass.TIMEOUT,
                retryable=True,
                status_code=None,
                detail="Talk post timed out",
            ),
        }
    )
    store = _RecordingStore()

    summary = deliver_notice_changes(
        changes,
        talk_client=client,
        processed_store=store,
    )

    safe = summary.to_safe_dict()
    assert safe["attempted"] == 2
    assert safe["posted"] == 0
    assert safe["failed"] == 2
    assert safe["acked"] == 0
    assert safe["failed_source_ids"] == ["notice-1", "notice-2"]
    assert [entry["stage"] for entry in safe["failures"]] == ["post", "post"]

    assert store.calls == []


def test_delivery_ack_failure_raises_with_posted_context_for_retry() -> None:
    changes = [_build_change(1), _build_change(2)]
    client = _FakeTalkClient()
    store = _RecordingStore(raise_on_ack=RuntimeError("database is locked"))

    with pytest.raises(TalkDeliveryAckError) as exc_info:
        deliver_notice_changes(
            changes,
            talk_client=client,
            processed_store=store,
        )

    safe = exc_info.value.to_safe_dict()
    assert safe["stage"] == "ack"
    assert safe["error_class"] == "RuntimeError"
    assert safe["posted_source_ids"] == ["notice-1", "notice-2"]

    summary = safe["summary"]
    assert summary["attempted"] == 2
    assert summary["posted"] == 2
    assert summary["failed"] == 0
    assert summary["acked"] == 0
    assert summary["failed_source_ids"] == []
    assert summary["ack_error_class"] == "RuntimeError"
    assert summary["ack_error_detail"] == "Ack persistence failed"
    assert summary["ack_pending_source_ids"] == ["notice-1", "notice-2"]

    assert len(store.calls) == 1
    assert store.calls[0] == [change.notice for change in changes]


def test_delivery_malformed_change_entry_fails_render_and_keeps_other_entries_flowing() -> None:
    changes: list[object] = [_build_change(1), object(), _build_change(3)]
    client = _FakeTalkClient()
    store = _RecordingStore()

    summary = deliver_notice_changes(
        changes,
        talk_client=client,
        processed_store=store,
    )

    safe = summary.to_safe_dict()
    assert safe["attempted"] == 3
    assert safe["posted"] == 2
    assert safe["failed"] == 1
    assert safe["acked"] == 2
    assert safe["failed_source_ids"] == ["invalid-change-1"]
    assert safe["failures"] == [
        {
            "source_id": "invalid-change-1",
            "stage": "render",
            "error_class": "TalkDeliveryInputError",
            "detail": "batch entries must be NoticeChange objects",
            "retryable": False,
            "status_code": None,
        }
    ]

    assert [source_id for source_id, _ in client.calls] == ["notice-1", "notice-3"]
    assert len(store.calls) == 1
    assert [notice.source_id for notice in store.calls[0]] == ["notice-1", "notice-3"]


def test_delivery_empty_batch_is_noop() -> None:
    client = _FakeTalkClient()
    store = _RecordingStore()

    summary = deliver_notice_changes([], talk_client=client, processed_store=store)

    assert summary.to_safe_dict() == {
        "attempted": 0,
        "posted": 0,
        "failed": 0,
        "acked": 0,
        "failed_source_ids": [],
        "failures": [],
    }
    assert client.calls == []
    assert store.calls == []


def test_delivery_rejects_rendered_messages_that_drop_source_link() -> None:
    changes = [_build_change(1)]
    client = _FakeTalkClient()
    store = _RecordingStore()

    def render_without_source_link(_: NoticeChange) -> str:
        return "### NEW: Betriebsmitteilung\nThis message forgot the source link"

    summary = deliver_notice_changes(
        changes,
        talk_client=client,
        processed_store=store,
        render_change=render_without_source_link,
    )

    safe = summary.to_safe_dict()
    assert safe["attempted"] == 1
    assert safe["posted"] == 0
    assert safe["failed"] == 1
    assert safe["acked"] == 0
    assert safe["failed_source_ids"] == ["notice-1"]
    assert safe["failures"] == [
        {
            "source_id": "notice-1",
            "stage": "render",
            "error_class": "TalkDeliveryInvariantError",
            "detail": "rendered message must include notice.source_link",
            "retryable": False,
            "status_code": None,
        }
    ]

    assert client.calls == []
    assert store.calls == []
