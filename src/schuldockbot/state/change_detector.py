"""Deterministic NEW/UPDATE change detection over persisted revisions."""

from __future__ import annotations

from collections.abc import Callable, Iterable

from schuldockbot.ingestion.models import NoticeRecord

from .models import ChangeKind, InvalidProcessedNoticeError, NoticeChange


def detect_notice_changes(
    notices: Iterable[NoticeRecord],
    *,
    revision_lookup: Callable[[str], object | None],
) -> list[NoticeChange]:
    """Classify ingested notices against persisted state.

    The function is intentionally pure:
    - it performs read-only revision lookups via ``revision_lookup``;
    - it returns typed ``NoticeChange`` events for NEW/UPDATE items;
    - it never persists acknowledgements (downstream must call store persistence explicitly).

    Raises ``InvalidProcessedNoticeError`` when notice identity data or lookup responses are malformed.
    Propagates lookup exceptions so the poll cycle can fail/retry without false emissions.
    """

    seen_in_batch: dict[str, str] = {}
    changes: list[NoticeChange] = []

    for notice in notices:
        source_id = _validate_required_text("source_id", getattr(notice, "source_id", None))
        revision_token = _validate_required_text(
            "revision_token",
            getattr(notice, "revision_token", None),
        )

        prior_batch_token = seen_in_batch.get(source_id)
        if prior_batch_token is not None:
            if prior_batch_token == revision_token:
                continue
            raise InvalidProcessedNoticeError(
                f"duplicate source_id {source_id!r} with conflicting revision_token values"
            )

        seen_in_batch[source_id] = revision_token

        persisted_token_raw = revision_lookup(source_id)
        persisted_token = _normalize_persisted_revision_token(
            source_id=source_id,
            persisted_token=persisted_token_raw,
        )

        if persisted_token is None:
            changes.append(
                NoticeChange(
                    kind=ChangeKind.NEW,
                    notice=notice,
                )
            )
            continue

        if persisted_token != revision_token:
            changes.append(
                NoticeChange(
                    kind=ChangeKind.UPDATE,
                    notice=notice,
                    previous_revision_token=persisted_token,
                )
            )

    return changes


def _normalize_persisted_revision_token(*, source_id: str, persisted_token: object | None) -> str | None:
    """Validate persisted lookup values and normalize empty/invalid forms."""

    if persisted_token is None:
        return None

    if not isinstance(persisted_token, str):
        raise InvalidProcessedNoticeError(
            f"revision lookup for source_id {source_id!r} returned non-string token"
        )

    normalized = persisted_token.strip()
    if not normalized:
        raise InvalidProcessedNoticeError(
            f"revision lookup for source_id {source_id!r} returned an empty token"
        )

    return normalized


def _validate_required_text(field_name: str, value: object) -> str:
    """Validate required text fields and trim surrounding whitespace."""

    if not isinstance(value, str):
        raise InvalidProcessedNoticeError(f"{field_name} must be a non-empty string")

    normalized = value.strip()
    if not normalized:
        raise InvalidProcessedNoticeError(f"{field_name} must be a non-empty string")

    return normalized
