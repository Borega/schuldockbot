"""State-layer models and exceptions for durable processed-revision persistence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


class StateStoreError(RuntimeError):
    """Base error for state persistence failures."""


class StateStoreInitializationError(StateStoreError):
    """Raised when a SQLite store cannot be opened or initialized."""


class StateStoreSchemaError(StateStoreInitializationError):
    """Raised when the on-disk schema is unsupported or malformed."""


class StateStorePersistenceError(StateStoreError):
    """Raised when a persistence operation cannot be completed."""


class StateStoreLockTimeoutError(StateStorePersistenceError):
    """Raised when SQLite lock contention exceeds the configured timeout."""


class InvalidProcessedNoticeError(StateStorePersistenceError):
    """Raised when required processed-notice fields are missing or invalid."""


@dataclass(frozen=True, slots=True)
class ProcessedNoticeState:
    """Persisted processed-revision state for one source notice."""

    source_id: str
    revision_token: str
    source_link: str
    processed_at: datetime

    def to_safe_dict(self) -> dict[str, str]:
        """Return diagnostics-safe values for logs and tests."""

        return {
            "source_id": self.source_id,
            "revision_token": self.revision_token,
            "source_link": self.source_link,
            "processed_at": self.processed_at.isoformat(),
        }


def build_processed_notice_state(
    *,
    source_id: object,
    revision_token: object,
    source_link: object,
    processed_at: datetime | None = None,
) -> ProcessedNoticeState:
    """Validate and normalize processed-notice input into a typed state model."""

    normalized_source_id = _validate_required_text("source_id", source_id)
    normalized_revision_token = _validate_required_text("revision_token", revision_token)

    if processed_at is None:
        normalized_processed_at = datetime.now(timezone.utc)
    elif processed_at.tzinfo is None:
        normalized_processed_at = processed_at.replace(tzinfo=timezone.utc)
    else:
        normalized_processed_at = processed_at

    normalized_source_link = "" if source_link is None else str(source_link)

    return ProcessedNoticeState(
        source_id=normalized_source_id,
        revision_token=normalized_revision_token,
        source_link=normalized_source_link,
        processed_at=normalized_processed_at,
    )


def _validate_required_text(field_name: str, value: object) -> str:
    """Validate required text fields and trim surrounding whitespace."""

    if not isinstance(value, str):
        raise InvalidProcessedNoticeError(f"{field_name} must be a non-empty string")

    normalized = value.strip()
    if not normalized:
        raise InvalidProcessedNoticeError(f"{field_name} must be a non-empty string")
    return normalized
