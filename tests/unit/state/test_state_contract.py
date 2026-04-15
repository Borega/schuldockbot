from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from schuldockbot.ingestion.models import NoticeRecord, SourceMode
from schuldockbot.ingestion.normalize import normalize_notice
from schuldockbot.ingestion.source_selector import SourceSelectionError, fetch_notices
from schuldockbot.state import ChangeKind, SqliteProcessedNoticeStore, detect_notice_changes

TESTS_DIR = Path(__file__).resolve().parents[2]
FIXTURE_DIR = TESTS_DIR / "fixtures" / "schuldock"
TEST_DB_ROOT = Path(".pytest_tmp_state")


def _db_path() -> Path:
    TEST_DB_ROOT.mkdir(parents=True, exist_ok=True)
    return TEST_DB_ROOT / f"state-contract-{uuid4().hex}.sqlite3"


def _load_json_fixture_text() -> str:
    return (FIXTURE_DIR / "issues.json").read_text(encoding="utf-8")


def _fixture_notices() -> list[NoticeRecord]:
    def _html_fetcher_not_expected() -> str:
        raise AssertionError("HTML fallback should not be used when JSON fixture is valid")

    result = fetch_notices(lambda: _load_json_fixture_text(), _html_fetcher_not_expected)
    assert result.source_mode is SourceMode.JSON
    assert result.record_count > 0
    return result.records


def _persist_changes(store: SqliteProcessedNoticeStore, *, notices: list[NoticeRecord]) -> int:
    changes = detect_notice_changes(notices, revision_lookup=store.get_revision_token)
    return store.mark_processed_batch(change.notice for change in changes)


def _build_revised_notice(original: NoticeRecord) -> NoticeRecord:
    return normalize_notice(
        source_mode=original.source_mode,
        source_id=original.source_id,
        notice_type=original.type,
        title=original.title,
        content=original.content,
        source_link=original.source_link,
        published_at=original.published_at,
        modified_at=original.modified_at + timedelta(minutes=5),
        content_is_html=False,
    )


def test_contract_first_cycle_emits_new_and_second_cycle_is_suppressed() -> None:
    store = SqliteProcessedNoticeStore(_db_path())

    try:
        notices = _fixture_notices()

        first_cycle = detect_notice_changes(notices, revision_lookup=store.get_revision_token)

        assert len(first_cycle) == len(notices), (
            "Expected one NEW event per fixture notice on cycle-1; "
            f"got {[change.to_safe_dict() for change in first_cycle]}"
        )
        assert all(change.kind is ChangeKind.NEW for change in first_cycle), (
            "Expected all cycle-1 events to be NEW; "
            f"got {[change.to_safe_dict() for change in first_cycle]}"
        )

        persisted_rows = store.mark_processed_batch(change.notice for change in first_cycle)
        assert persisted_rows == len(notices)

        second_cycle = detect_notice_changes(notices, revision_lookup=store.get_revision_token)
        assert second_cycle == [], (
            "Expected cycle-2 duplicate suppression for unchanged revisions; "
            f"got {[change.to_safe_dict() for change in second_cycle]}"
        )
        assert store.get_processed_count() == len({notice.source_id for notice in notices})
    finally:
        store.close()


def test_contract_revision_change_emits_single_update_once() -> None:
    store = SqliteProcessedNoticeStore(_db_path())

    try:
        notices = _fixture_notices()
        assert _persist_changes(store, notices=notices) == len(notices)

        original = notices[0]
        revised_once = _build_revised_notice(original)
        revised_twice = _build_revised_notice(original)

        assert revised_once.source_id == original.source_id
        assert revised_once.revision_token != original.revision_token
        assert revised_once.revision_token == revised_twice.revision_token

        revised_cycle = [revised_once, *notices[1:]]
        third_cycle = detect_notice_changes(revised_cycle, revision_lookup=store.get_revision_token)

        assert len(third_cycle) == 1, (
            "Expected exactly one UPDATE on revision change for known source_id; "
            f"got {[change.to_safe_dict() for change in third_cycle]}"
        )

        update = third_cycle[0]
        assert update.kind is ChangeKind.UPDATE
        assert update.notice.source_id == original.source_id
        assert update.notice.revision_token == revised_once.revision_token
        assert update.previous_revision_token == original.revision_token

        store.mark_processed_batch(change.notice for change in third_cycle)

        fourth_cycle = detect_notice_changes(revised_cycle, revision_lookup=store.get_revision_token)
        assert fourth_cycle == [], (
            "Expected revision-change UPDATE to emit once after ack; "
            f"got {[change.to_safe_dict() for change in fourth_cycle]}"
        )
    finally:
        store.close()


def test_contract_restart_preserves_dedupe_state_for_processed_revisions() -> None:
    db_path = _db_path()
    notices = _fixture_notices()

    writer = SqliteProcessedNoticeStore(db_path)
    try:
        assert _persist_changes(writer, notices=notices) == len(notices)

        revised = _build_revised_notice(notices[0])
        revised_cycle = [revised, *notices[1:]]

        update_cycle = detect_notice_changes(revised_cycle, revision_lookup=writer.get_revision_token)
        assert len(update_cycle) == 1
        assert update_cycle[0].kind is ChangeKind.UPDATE

        writer.mark_processed_batch(change.notice for change in update_cycle)
    finally:
        writer.close()

    reopened = SqliteProcessedNoticeStore(db_path)
    try:
        post_restart_cycle = detect_notice_changes(
            revised_cycle,
            revision_lookup=reopened.get_revision_token,
        )

        assert post_restart_cycle == [], (
            "Expected post-restart duplicate suppression for already-acked revisions; "
            f"got {[change.to_safe_dict() for change in post_restart_cycle]}"
        )
        assert reopened.get_processed_count() == len({notice.source_id for notice in notices})
        assert reopened.get_revision_token(revised.source_id) == revised.revision_token
    finally:
        reopened.close()


def test_contract_malformed_ingestion_payload_fails_before_state_changes() -> None:
    store = SqliteProcessedNoticeStore(_db_path())

    try:
        with pytest.raises(SourceSelectionError) as exc_info:
            fetch_notices(
                lambda: "{",
                lambda: "<html><body><main><p>broken</p></main></body></html>",
            )

        diagnostics = exc_info.value.to_safe_dict()
        assert diagnostics["fallback_reason"] == "decode"
        assert diagnostics["json_failure"]["failure_class"] == "decode"
        assert store.get_processed_count() == 0
    finally:
        store.close()


def test_contract_empty_cycle_has_no_phantom_changes() -> None:
    store = SqliteProcessedNoticeStore(_db_path())

    try:
        notices = _fixture_notices()
        assert _persist_changes(store, notices=notices) == len(notices)
        before_count = store.get_processed_count()

        lookup_calls: list[str] = []

        def lookup(source_id: str) -> str | None:
            lookup_calls.append(source_id)
            return store.get_revision_token(source_id)

        assert detect_notice_changes([], revision_lookup=lookup) == []
        assert lookup_calls == []
        assert store.get_processed_count() == before_count
    finally:
        store.close()


def test_contract_persistence_failure_rolls_back_current_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = SqliteProcessedNoticeStore(_db_path())

    try:
        notices = _fixture_notices()
        assert _persist_changes(store, notices=notices) == len(notices)

        baseline_tokens = {
            notice.source_id: store.get_revision_token(notice.source_id)
            for notice in notices
        }

        revised = _build_revised_notice(notices[0])
        brand_new = SimpleNamespace(
            source_id=f"{notices[-1].source_id}-new",
            revision_token="contract-new-revision-token",
            source_link=f"{notices[-1].source_link}?variant=new",
        )
        cycle_with_failure = [revised, *notices[1:], brand_new]

        pending_changes = detect_notice_changes(
            cycle_with_failure,
            revision_lookup=store.get_revision_token,
        )

        assert {change.kind for change in pending_changes} == {ChangeKind.NEW, ChangeKind.UPDATE}

        original_upsert = store._upsert_processed_notice
        calls = 0

        def failing_upsert(cursor, notice):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("injected-cycle-failure")
            return original_upsert(cursor, notice)

        monkeypatch.setattr(store, "_upsert_processed_notice", failing_upsert)

        with pytest.raises(RuntimeError, match="injected-cycle-failure"):
            store.mark_processed_batch(change.notice for change in pending_changes)

        for source_id, expected_token in baseline_tokens.items():
            assert store.get_revision_token(source_id) == expected_token, (
                "Cycle rollback mutated already-committed state for "
                f"source_id={source_id!r}, expected_token={expected_token!r}"
            )

        assert store.get_revision_token(brand_new.source_id) is None
        assert store.get_processed_count() == len(baseline_tokens)
    finally:
        store.close()
