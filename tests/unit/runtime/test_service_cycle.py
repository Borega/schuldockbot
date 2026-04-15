from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import pytest

from schuldockbot.ingestion.models import NoticeRecord, SourceMode
from schuldockbot.ingestion.source_selector import SourceSelectionResult
from schuldockbot.runtime.service import RuntimeCycleError, run_poll_cycle
from schuldockbot.state.models import ChangeKind, NoticeChange, StateStoreLockTimeoutError
from schuldockbot.talk import TalkFailureClass, TalkPostError, TalkPostResult

TESTS_DIR = Path(__file__).resolve().parents[2]
FIXTURE_DIR = TESTS_DIR / "fixtures" / "schuldock"


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
class _FakeStore:
    revisions: dict[str, str] | None = None
    raise_on_ack: Exception | None = None
    ack_calls: list[list[object]] | None = None

    def __post_init__(self) -> None:
        if self.revisions is None:
            self.revisions = {}
        self.ack_calls = []

    def get_revision_token(self, source_id: str) -> str | None:
        return self.revisions.get(source_id)

    def mark_processed_batch(self, notices) -> int:
        materialized = list(notices)
        self.ack_calls.append(materialized)
        if self.raise_on_ack is not None:
            raise self.raise_on_ack
        for notice in materialized:
            self.revisions[notice.source_id] = notice.revision_token
        return len(materialized)


def _load_json_fixture_text() -> str:
    return (FIXTURE_DIR / "issues.json").read_text(encoding="utf-8")


def _load_html_fixture_text() -> str:
    return (FIXTURE_DIR / "aktuelle-meldungen.html").read_text(encoding="utf-8")


def _build_notice(index: int, *, content: str | None = None, revision_suffix: str = "A") -> NoticeRecord:
    published_at = datetime(2026, 4, 15, 9, 0, tzinfo=UTC)
    modified_at = published_at + timedelta(minutes=index)

    return NoticeRecord(
        source_mode=SourceMode.JSON,
        source_id=f"notice-{index}",
        type="Betriebsmitteilung",
        title=f"Notice {index}",
        content=content or f"Content for notice {index}",
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


def test_cycle_success_json_source_emits_machine_readable_counters() -> None:
    talk_client = _FakeTalkClient()
    store = _FakeStore()

    result = run_poll_cycle(
        json_fetcher=_load_json_fixture_text,
        html_fetcher=lambda: (_ for _ in ()).throw(AssertionError("fallback should not run")),
        talk_client=talk_client,
        processed_store=store,
    )

    safe = result.to_safe_dict()
    assert safe["source_mode"] == "json"
    assert safe["fallback_reason"] is None
    assert safe["fetched"] == 2
    assert safe["detected"] == 2
    assert safe["attempted"] == 2
    assert safe["posted"] == 2
    assert safe["failed"] == 0
    assert safe["acked"] == 2

    assert [source_id for source_id, _ in talk_client.calls] == ["84572", "84573"]
    assert len(store.ack_calls) == 1
    assert [notice.source_id for notice in store.ack_calls[0]] == ["84572", "84573"]


def test_cycle_falls_back_to_html_when_json_decode_fails() -> None:
    talk_client = _FakeTalkClient()
    store = _FakeStore()

    result = run_poll_cycle(
        json_fetcher=lambda: "{",
        html_fetcher=_load_html_fixture_text,
        talk_client=talk_client,
        processed_store=store,
    )

    safe = result.to_safe_dict()
    assert safe["source_mode"] == "html_fallback"
    assert safe["fallback_reason"] == "decode"
    assert safe["fetched"] == 2
    assert safe["detected"] == 2
    assert safe["attempted"] == 2
    assert safe["posted"] == 2
    assert safe["failed"] == 0
    assert safe["acked"] == 2


def test_cycle_raises_structured_error_for_partial_delivery_failures() -> None:
    notices = [_build_notice(1), _build_notice(2), _build_notice(3)]
    selection = SourceSelectionResult(
        records=notices,
        source_mode=SourceMode.JSON,
        fallback_reason=None,
    )

    talk_client = _FakeTalkClient(
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
    store = _FakeStore()

    with pytest.raises(RuntimeCycleError) as exc_info:
        run_poll_cycle(
            json_fetcher=lambda: "unused",
            html_fetcher=lambda: "unused",
            talk_client=talk_client,
            processed_store=store,
            fetch_notices_fn=lambda _json_fetcher, _html_fetcher: selection,
        )

    error = exc_info.value
    safe = error.to_safe_dict()

    assert safe["phase"] == "deliver"
    assert safe["failure_class"] == "deliver_rate_limited"
    assert safe["retryable"] is True
    assert safe["pending_source_ids"] == ["notice-2"]
    assert safe["attempted"] == 3
    assert safe["posted"] == 2
    assert safe["failed"] == 1
    assert safe["acked"] == 2

    assert len(store.ack_calls) == 1
    assert [notice.source_id for notice in store.ack_calls[0]] == ["notice-1", "notice-3"]


def test_cycle_surfaces_ack_failure_with_pending_source_context() -> None:
    notices = [_build_notice(1), _build_notice(2)]
    selection = SourceSelectionResult(
        records=notices,
        source_mode=SourceMode.JSON,
        fallback_reason=None,
    )

    talk_client = _FakeTalkClient()
    store = _FakeStore(raise_on_ack=StateStoreLockTimeoutError("database is locked"))

    with pytest.raises(RuntimeCycleError) as exc_info:
        run_poll_cycle(
            json_fetcher=lambda: "unused",
            html_fetcher=lambda: "unused",
            talk_client=talk_client,
            processed_store=store,
            fetch_notices_fn=lambda _json_fetcher, _html_fetcher: selection,
        )

    safe = exc_info.value.to_safe_dict()
    assert safe["phase"] == "ack"
    assert safe["failure_class"] == "ack_timeout"
    assert safe["retryable"] is True
    assert safe["pending_source_ids"] == ["notice-1", "notice-2"]
    assert safe["attempted"] == 2
    assert safe["posted"] == 2
    assert safe["failed"] == 0
    assert safe["acked"] == 0


def test_cycle_surfaces_malformed_change_entries_as_input_failures() -> None:
    notices = [_build_notice(1)]
    selection = SourceSelectionResult(
        records=notices,
        source_mode=SourceMode.JSON,
        fallback_reason=None,
    )

    talk_client = _FakeTalkClient()
    store = _FakeStore()

    def malformed_detect(*_args, **_kwargs) -> list[object]:
        return [object()]

    with pytest.raises(RuntimeCycleError) as exc_info:
        run_poll_cycle(
            json_fetcher=lambda: "unused",
            html_fetcher=lambda: "unused",
            talk_client=talk_client,
            processed_store=store,
            fetch_notices_fn=lambda _json_fetcher, _html_fetcher: selection,
            detect_changes_fn=malformed_detect,
        )

    safe = exc_info.value.to_safe_dict()
    assert safe["phase"] == "deliver"
    assert safe["failure_class"] == "deliver_input"
    assert safe["retryable"] is False
    assert safe["pending_source_ids"] == ["invalid-change-0"]
    assert safe["attempted"] == 1
    assert safe["posted"] == 0
    assert safe["failed"] == 1
    assert safe["acked"] == 0


def test_cycle_zero_changes_keeps_counters_and_skips_ack_write() -> None:
    notice = _build_notice(1, revision_suffix="A")
    selection = SourceSelectionResult(
        records=[notice],
        source_mode=SourceMode.JSON,
        fallback_reason=None,
    )

    talk_client = _FakeTalkClient()
    store = _FakeStore(revisions={"notice-1": "rev-1-A"})

    result = run_poll_cycle(
        json_fetcher=lambda: "unused",
        html_fetcher=lambda: "unused",
        talk_client=talk_client,
        processed_store=store,
        fetch_notices_fn=lambda _json_fetcher, _html_fetcher: selection,
    )

    assert result.to_safe_dict() == {
        "source_mode": "json",
        "fallback_reason": None,
        "fetched": 1,
        "detected": 0,
        "attempted": 0,
        "posted": 0,
        "failed": 0,
        "acked": 0,
    }
    assert talk_client.calls == []
    assert store.ack_calls == []


def test_cycle_dual_source_failure_returns_fetch_phase_error() -> None:
    with pytest.raises(RuntimeCycleError) as exc_info:
        run_poll_cycle(
            json_fetcher=lambda: "{",
            html_fetcher=lambda: "<html><body><main><p>broken</p></main></body></html>",
            talk_client=_FakeTalkClient(),
            processed_store=_FakeStore(),
        )

    safe = exc_info.value.to_safe_dict()
    assert safe["phase"] == "fetch"
    assert safe["failure_class"] == "fetch_malformed"
    assert safe["retryable"] is False
    assert safe["source_mode"] == "html_fallback"
    assert safe["fallback_reason"] == "decode"
    assert safe["fetched"] == 0
    assert safe["detected"] == 0
    assert safe["attempted"] == 0
    assert safe["posted"] == 0
    assert safe["failed"] == 0
    assert safe["acked"] == 0


def test_cycle_safe_diagnostics_do_not_leak_notice_body_or_auth_markers() -> None:
    secret_body = "TOP_SECRET_NOTICE_BODY"
    notice = _build_notice(1, content=secret_body)
    selection = SourceSelectionResult(
        records=[notice],
        source_mode=SourceMode.JSON,
        fallback_reason=None,
    )

    result = run_poll_cycle(
        json_fetcher=lambda: "unused",
        html_fetcher=lambda: "unused",
        talk_client=_FakeTalkClient(),
        processed_store=_FakeStore(),
        fetch_notices_fn=lambda _json_fetcher, _html_fetcher: selection,
    )

    serialized = json.dumps(result.to_safe_dict())
    assert secret_body not in serialized
    assert "Authorization" not in serialized
    assert "app_password" not in serialized
