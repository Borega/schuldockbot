"""SQLite persistence adapter for processed notice revisions."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

from .models import (
    InvalidProcessedNoticeError,
    ProcessedNoticeState,
    StateStoreInitializationError,
    StateStoreLockTimeoutError,
    StateStorePersistenceError,
    StateStoreSchemaError,
    build_processed_notice_state,
)

SCHEMA_VERSION = 1


class SqliteProcessedNoticeStore:
    """Store processed notice revisions in a durable SQLite file."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        timeout_seconds: float = 5.0,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._db_path = str(db_path)
        self._timeout_seconds = timeout_seconds
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._connection: sqlite3.Connection | None = None

        self._initialize_connection_and_schema()

    def close(self) -> None:
        """Close the underlying SQLite connection."""

        connection = self._connection
        if connection is not None:
            connection.close()
            self._connection = None

    def _require_connection(self) -> sqlite3.Connection:
        """Return an active SQLite connection or raise if already closed."""

        if self._connection is None:
            raise StateStorePersistenceError("SQLite store connection is closed")
        return self._connection

    def get_revision_token(self, source_id: str) -> str | None:
        """Read the stored revision token for a source id, if it exists."""

        normalized_source_id = _validate_source_id(source_id)
        connection = self._require_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT revision_token FROM processed_notices WHERE source_id = ?",
                (normalized_source_id,),
            )
            row = cursor.fetchone()
        finally:
            cursor.close()

        if row is None:
            return None
        return str(row["revision_token"])

    def mark_processed(
        self,
        *,
        source_id: str,
        revision_token: str,
        source_link: str,
        processed_at: datetime | None = None,
    ) -> None:
        """Persist one processed notice revision (idempotent upsert)."""

        notice = build_processed_notice_state(
            source_id=source_id,
            revision_token=revision_token,
            source_link=source_link,
            processed_at=processed_at or self._clock(),
        )
        self.mark_processed_batch([notice])

    def mark_processed_batch(self, notices: Iterable[object]) -> int:
        """Persist processed revisions atomically as one transaction.

        Returns the number of processed rows persisted (0 for an empty batch).
        """

        validated_notices = [self._coerce_notice(notice) for notice in notices]
        if not validated_notices:
            return 0

        connection = self._require_connection()
        cursor = connection.cursor()
        try:
            cursor.execute("BEGIN")
            for notice in validated_notices:
                self._upsert_processed_notice(cursor, notice)
            connection.commit()
        except sqlite3.OperationalError as exc:
            connection.rollback()
            if _is_lock_timeout_error(exc):
                raise StateStoreLockTimeoutError(
                    "SQLite write lock timeout while persisting processed revisions"
                ) from exc
            raise StateStorePersistenceError(
                "SQLite persistence failure while writing processed revisions"
            ) from exc
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()

        return len(validated_notices)

    def get_processed_notice(self, source_id: str) -> ProcessedNoticeState | None:
        """Return persisted state for one source id, if present."""

        normalized_source_id = _validate_source_id(source_id)
        connection = self._require_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                SELECT source_id, revision_token, source_link, processed_at
                FROM processed_notices
                WHERE source_id = ?
                """,
                (normalized_source_id,),
            )
            row = cursor.fetchone()
        finally:
            cursor.close()

        if row is None:
            return None

        processed_at = datetime.fromisoformat(str(row["processed_at"]))
        if processed_at.tzinfo is None:
            processed_at = processed_at.replace(tzinfo=timezone.utc)

        return ProcessedNoticeState(
            source_id=str(row["source_id"]),
            revision_token=str(row["revision_token"]),
            source_link=str(row["source_link"]),
            processed_at=processed_at,
        )

    def get_processed_count(self) -> int:
        """Return the number of persisted processed-notice rows."""

        connection = self._require_connection()
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT COUNT(*) AS total FROM processed_notices")
            row = cursor.fetchone()
        finally:
            cursor.close()

        return int(row["total"])

    def _initialize_connection_and_schema(self) -> None:
        """Open SQLite connection and bootstrap/validate schema."""

        if self._db_path != ":memory:":
            Path(self._db_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)

        try:
            connection = sqlite3.connect(self._db_path, timeout=self._timeout_seconds)
            connection.row_factory = sqlite3.Row
            self._connection = connection
            self._bootstrap_schema()
        except StateStoreInitializationError:
            if self._connection is not None:
                self.close()
            raise
        except (sqlite3.Error, OSError) as exc:
            if self._connection is not None:
                self.close()
            raise StateStoreInitializationError(
                f"Failed to initialize SQLite processed-notice store at {self._db_path!r}"
            ) from exc

    def _bootstrap_schema(self) -> None:
        """Create or validate schema and enforce supported user_version."""

        connection = self._require_connection()
        cursor = connection.cursor()
        try:
            cursor.execute("PRAGMA user_version")
            current_version = int(cursor.fetchone()[0])
        finally:
            cursor.close()

        if current_version == 0:
            with connection:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS processed_notices (
                        source_id TEXT PRIMARY KEY,
                        revision_token TEXT NOT NULL,
                        source_link TEXT NOT NULL,
                        processed_at TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_processed_notices_revision_token
                    ON processed_notices (revision_token)
                    """
                )
                connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        elif current_version != SCHEMA_VERSION:
            raise StateStoreSchemaError(
                f"Unsupported SQLite schema user_version={current_version}; expected {SCHEMA_VERSION}"
            )

        self._assert_schema_shape()

    def _assert_schema_shape(self) -> None:
        """Validate schema shape and fail closed when malformed."""

        connection = self._require_connection()
        cursor = connection.cursor()
        try:
            cursor.execute("PRAGMA table_info(processed_notices)")
            rows = cursor.fetchall()
        finally:
            cursor.close()

        if not rows:
            raise StateStoreSchemaError("Missing required table: processed_notices")

        expected_columns = {"source_id", "revision_token", "source_link", "processed_at"}
        actual_columns = {str(row["name"]) for row in rows}
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise StateStoreSchemaError(
                f"Malformed processed_notices schema; missing columns: {missing}"
            )

        source_id_column = next((row for row in rows if row["name"] == "source_id"), None)
        if source_id_column is None or int(source_id_column["pk"]) != 1:
            raise StateStoreSchemaError("Malformed processed_notices schema; source_id must be PRIMARY KEY")

    def _coerce_notice(self, notice: object) -> ProcessedNoticeState:
        """Accept ProcessedNoticeState or objects exposing source_id/revision_token/source_link."""

        if isinstance(notice, ProcessedNoticeState):
            return notice

        source_id = getattr(notice, "source_id", None)
        revision_token = getattr(notice, "revision_token", None)
        source_link = getattr(notice, "source_link", None)

        if source_link is None:
            source_link = ""

        if source_id is None or revision_token is None:
            raise InvalidProcessedNoticeError(
                "Batch entries must expose non-empty source_id and revision_token"
            )

        processed_at = getattr(notice, "processed_at", None)
        if processed_at is None:
            processed_at = self._clock()

        return build_processed_notice_state(
            source_id=source_id,
            revision_token=revision_token,
            source_link=source_link,
            processed_at=processed_at,
        )

    def _upsert_processed_notice(self, cursor: sqlite3.Cursor, notice: ProcessedNoticeState) -> None:
        """Upsert one processed notice state row using bound parameters."""

        cursor.execute(
            """
            INSERT INTO processed_notices (
                source_id,
                revision_token,
                source_link,
                processed_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
                revision_token = excluded.revision_token,
                source_link = excluded.source_link,
                processed_at = excluded.processed_at
            """,
            (
                notice.source_id,
                notice.revision_token,
                notice.source_link,
                notice.processed_at.isoformat(),
            ),
        )


def _is_lock_timeout_error(exc: sqlite3.OperationalError) -> bool:
    """Detect lock-contention operational errors."""

    message = str(exc).lower()
    return (
        "database is locked" in message
        or "database table is locked" in message
        or "database schema is locked" in message
    )


def _validate_source_id(source_id: object) -> str:
    """Validate source id input for read APIs."""

    if not isinstance(source_id, str):
        raise InvalidProcessedNoticeError("source_id must be a non-empty string")

    normalized = source_id.strip()
    if not normalized:
        raise InvalidProcessedNoticeError("source_id must be a non-empty string")
    return normalized
