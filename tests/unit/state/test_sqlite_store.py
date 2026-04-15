from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from schuldockbot.state import (
    InvalidProcessedNoticeError,
    SqliteProcessedNoticeStore,
    StateStoreSchemaError,
)

_TEST_DB_ROOT = Path(".pytest_tmp_state")


def _db_path() -> Path:
    _TEST_DB_ROOT.mkdir(parents=True, exist_ok=True)
    return _TEST_DB_ROOT / f"processed-state-{uuid4().hex}.sqlite3"


def test_first_write_persists_and_is_queryable() -> None:
    db_path = _db_path()
    store = SqliteProcessedNoticeStore(db_path)

    store.mark_processed(
        source_id="notice-1",
        revision_token="rev-1",
        source_link="https://schuldock.invalid/notices/1",
    )

    persisted = store.get_processed_notice("notice-1")
    assert persisted is not None
    assert persisted.source_id == "notice-1"
    assert persisted.revision_token == "rev-1"
    assert store.get_revision_token("notice-1") == "rev-1"
    assert store.get_processed_count() == 1

    with sqlite3.connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT source_id, revision_token, source_link
            FROM processed_notices
            WHERE source_id = ?
            """,
            ("notice-1",),
        ).fetchone()

    assert row == ("notice-1", "rev-1", "https://schuldock.invalid/notices/1")

    store.close()


def test_mark_processed_upsert_is_idempotent_and_updates_token() -> None:
    store = SqliteProcessedNoticeStore(_db_path())

    store.mark_processed(
        source_id="notice-1",
        revision_token="rev-1",
        source_link="https://schuldock.invalid/notices/1",
    )
    first = store.get_processed_notice("notice-1")
    assert first is not None

    store.mark_processed(
        source_id="notice-1",
        revision_token="rev-2",
        source_link="https://schuldock.invalid/notices/1?updated=1",
    )

    updated = store.get_processed_notice("notice-1")
    assert updated is not None
    assert updated.revision_token == "rev-2"
    assert updated.source_link.endswith("updated=1")
    assert updated.processed_at >= first.processed_at
    assert store.get_processed_count() == 1

    store.close()


def test_mark_processed_batch_is_atomic_on_injected_mid_batch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = SqliteProcessedNoticeStore(_db_path())

    store.mark_processed(
        source_id="seed",
        revision_token="rev-seed",
        source_link="https://schuldock.invalid/notices/seed",
    )

    original_upsert = store._upsert_processed_notice
    calls = 0

    def failing_upsert(cursor, notice):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected-failure")
        return original_upsert(cursor, notice)

    monkeypatch.setattr(store, "_upsert_processed_notice", failing_upsert)

    with pytest.raises(RuntimeError, match="injected-failure"):
        store.mark_processed_batch(
            [
                SimpleNamespace(
                    source_id="notice-a",
                    revision_token="rev-a",
                    source_link="https://schuldock.invalid/notices/a",
                ),
                SimpleNamespace(
                    source_id="notice-b",
                    revision_token="rev-b",
                    source_link="https://schuldock.invalid/notices/b",
                ),
            ]
        )

    assert store.get_revision_token("seed") == "rev-seed"
    assert store.get_revision_token("notice-a") is None
    assert store.get_revision_token("notice-b") is None
    assert store.get_processed_count() == 1

    store.close()


def test_reopen_preserves_processed_revisions() -> None:
    db_path = _db_path()

    writer = SqliteProcessedNoticeStore(db_path)
    writer.mark_processed(
        source_id="notice-1",
        revision_token="rev-1",
        source_link="https://schuldock.invalid/notices/1",
    )
    writer.close()

    reopened = SqliteProcessedNoticeStore(db_path)
    assert reopened.get_revision_token("notice-1") == "rev-1"
    assert reopened.get_processed_count() == 1
    reopened.close()


def test_empty_batch_is_noop() -> None:
    store = SqliteProcessedNoticeStore(_db_path())

    assert store.mark_processed_batch([]) == 0
    assert store.get_processed_count() == 0

    store.close()


def test_batch_duplicate_source_id_is_idempotent() -> None:
    store = SqliteProcessedNoticeStore(_db_path())

    processed_rows = [
        SimpleNamespace(
            source_id="notice-1",
            revision_token="rev-1",
            source_link="https://schuldock.invalid/notices/1",
            processed_at=datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc),
        ),
        SimpleNamespace(
            source_id="notice-1",
            revision_token="rev-1",
            source_link="https://schuldock.invalid/notices/1",
            processed_at=datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc),
        ),
    ]

    assert store.mark_processed_batch(processed_rows) == 2
    assert store.get_processed_count() == 1
    assert store.get_revision_token("notice-1") == "rev-1"

    store.close()


def test_malformed_inputs_are_rejected_before_persistence() -> None:
    store = SqliteProcessedNoticeStore(_db_path())

    with pytest.raises(InvalidProcessedNoticeError, match="source_id"):
        store.mark_processed(
            source_id="",
            revision_token="rev-1",
            source_link="https://schuldock.invalid/notices/1",
        )

    with pytest.raises(InvalidProcessedNoticeError, match="revision_token"):
        store.mark_processed(
            source_id="notice-1",
            revision_token="  ",
            source_link="https://schuldock.invalid/notices/1",
        )

    with pytest.raises(InvalidProcessedNoticeError, match="source_id"):
        store.mark_processed_batch(
            [
                SimpleNamespace(
                    source_id="notice-valid",
                    revision_token="rev-valid",
                    source_link="https://schuldock.invalid/notices/valid",
                ),
                SimpleNamespace(
                    source_id="",
                    revision_token="rev-invalid",
                    source_link="https://schuldock.invalid/notices/invalid",
                ),
            ]
        )

    assert store.get_processed_count() == 0

    store.close()


def test_reopen_rejects_unsupported_schema_version() -> None:
    db_path = _db_path()

    with sqlite3.connect(db_path) as connection:
        connection.execute("PRAGMA user_version = 999")

    with pytest.raises(StateStoreSchemaError, match="Unsupported SQLite schema"):
        SqliteProcessedNoticeStore(db_path)


def test_reopen_rejects_malformed_schema_shape() -> None:
    db_path = _db_path()

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE processed_notices (
                source_id TEXT PRIMARY KEY,
                revision_token TEXT NOT NULL
            )
            """
        )
        connection.execute("PRAGMA user_version = 1")

    with pytest.raises(StateStoreSchemaError, match="missing columns"):
        SqliteProcessedNoticeStore(db_path)
