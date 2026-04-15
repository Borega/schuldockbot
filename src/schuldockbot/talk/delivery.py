"""Detect→deliver→ack orchestration for Talk notice changes."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Protocol

from schuldockbot.state.models import NoticeChange

from .formatter import TalkFormatterInputError, render_notice_change
from .models import TalkPostError, TalkPostResult


class TalkClientLike(Protocol):
    """Minimal client contract required by the delivery orchestrator."""

    def post_message(self, *, source_id: str, message: str) -> TalkPostResult: ...


class ProcessedNoticeStoreLike(Protocol):
    """Minimal processed-store contract required by the delivery orchestrator."""

    def mark_processed_batch(self, notices: Iterable[object]) -> int: ...


class TalkDeliveryInputError(ValueError):
    """Raised when a delivery batch contains malformed change entries."""


class TalkDeliveryInvariantError(RuntimeError):
    """Raised when rendered output violates delivery invariants."""


@dataclass(frozen=True, slots=True)
class TalkDeliveryFailure:
    """Safe per-change failure diagnostics for delivery retries."""

    source_id: str
    stage: str
    error_class: str
    detail: str
    retryable: bool
    status_code: int | None = None

    def to_safe_dict(self) -> dict[str, object]:
        """Return diagnostics-safe failure metadata."""

        return {
            "source_id": self.source_id,
            "stage": self.stage,
            "error_class": self.error_class,
            "detail": self.detail,
            "retryable": self.retryable,
            "status_code": self.status_code,
        }


@dataclass(frozen=True, slots=True)
class TalkDeliverySummary:
    """Safe summary diagnostics for one delivery batch attempt."""

    attempted: int
    posted: int
    failed: int
    acked: int
    failed_source_ids: tuple[str, ...]
    failures: tuple[TalkDeliveryFailure, ...] = ()
    ack_error_class: str | None = None
    ack_error_detail: str | None = None
    ack_pending_source_ids: tuple[str, ...] = ()

    def to_safe_dict(self) -> dict[str, object]:
        """Return diagnostics-safe aggregate delivery metadata."""

        payload: dict[str, object] = {
            "attempted": self.attempted,
            "posted": self.posted,
            "failed": self.failed,
            "acked": self.acked,
            "failed_source_ids": list(self.failed_source_ids),
            "failures": [failure.to_safe_dict() for failure in self.failures],
        }
        if self.ack_error_class is not None:
            payload["ack_error_class"] = self.ack_error_class
            payload["ack_error_detail"] = self.ack_error_detail
            payload["ack_pending_source_ids"] = list(self.ack_pending_source_ids)
        return payload


@dataclass(slots=True)
class TalkDeliveryAckError(RuntimeError):
    """Raised when post success cannot be acked in persistence storage."""

    error_class: str
    posted_source_ids: tuple[str, ...]
    summary: TalkDeliverySummary

    def __post_init__(self) -> None:
        RuntimeError.__init__(self, "Failed to ack posted notice revisions")

    def to_safe_dict(self) -> dict[str, object]:
        """Return diagnostics-safe ack-stage failure metadata."""

        return {
            "stage": "ack",
            "error_class": self.error_class,
            "posted_source_ids": list(self.posted_source_ids),
            "summary": self.summary.to_safe_dict(),
        }


NoticeChangeRenderer = Callable[[NoticeChange], str]


def deliver_notice_changes(
    changes: Iterable[object],
    *,
    talk_client: TalkClientLike,
    processed_store: ProcessedNoticeStoreLike,
    render_change: NoticeChangeRenderer = render_notice_change,
) -> TalkDeliverySummary:
    """Deliver a batch of changes and ack only successfully posted revisions."""

    materialized_changes = list(changes)
    posted_changes: list[NoticeChange] = []
    failures: list[TalkDeliveryFailure] = []

    for index, raw_change in enumerate(materialized_changes):
        source_id = _extract_source_id(raw_change, index=index)

        try:
            change = _coerce_notice_change(raw_change)
            rendered = render_change(change)
            _assert_rendered_message_contains_source_link(change=change, rendered=rendered)
        except Exception as exc:
            failures.append(_build_render_failure(source_id=source_id, exc=exc))
            continue

        try:
            talk_client.post_message(source_id=source_id, message=rendered)
        except TalkPostError as exc:
            failures.append(
                TalkDeliveryFailure(
                    source_id=source_id,
                    stage="post",
                    error_class=exc.failure_class.value,
                    detail=exc.detail,
                    retryable=exc.retryable,
                    status_code=exc.status_code,
                )
            )
        except Exception as exc:
            failures.append(
                TalkDeliveryFailure(
                    source_id=source_id,
                    stage="post",
                    error_class=exc.__class__.__name__,
                    detail="Unexpected Talk post failure",
                    retryable=True,
                    status_code=None,
                )
            )
        else:
            posted_changes.append(change)

    posted_source_ids = tuple(change.notice.source_id for change in posted_changes)

    if not posted_changes:
        return _build_summary(
            attempted=len(materialized_changes),
            posted=0,
            acked=0,
            failures=failures,
        )

    notices_to_ack = [change.notice for change in posted_changes]

    try:
        acked = int(processed_store.mark_processed_batch(notices_to_ack))
    except Exception as exc:
        summary = _build_summary(
            attempted=len(materialized_changes),
            posted=len(posted_changes),
            acked=0,
            failures=failures,
            ack_error_class=exc.__class__.__name__,
            ack_error_detail="Ack persistence failed",
            ack_pending_source_ids=posted_source_ids,
        )
        raise TalkDeliveryAckError(
            error_class=exc.__class__.__name__,
            posted_source_ids=posted_source_ids,
            summary=summary,
        ) from exc

    return _build_summary(
        attempted=len(materialized_changes),
        posted=len(posted_changes),
        acked=acked,
        failures=failures,
    )


def _build_summary(
    *,
    attempted: int,
    posted: int,
    acked: int,
    failures: list[TalkDeliveryFailure],
    ack_error_class: str | None = None,
    ack_error_detail: str | None = None,
    ack_pending_source_ids: tuple[str, ...] = (),
) -> TalkDeliverySummary:
    failed_source_ids = tuple(failure.source_id for failure in failures)
    return TalkDeliverySummary(
        attempted=attempted,
        posted=posted,
        failed=len(failures),
        acked=acked,
        failed_source_ids=failed_source_ids,
        failures=tuple(failures),
        ack_error_class=ack_error_class,
        ack_error_detail=ack_error_detail,
        ack_pending_source_ids=ack_pending_source_ids,
    )


def _extract_source_id(change: object, *, index: int) -> str:
    notice = getattr(change, "notice", None)
    raw_source_id = getattr(notice, "source_id", None)
    if isinstance(raw_source_id, str):
        normalized = raw_source_id.strip()
        if normalized:
            return normalized
    return f"invalid-change-{index}"


def _coerce_notice_change(change: object) -> NoticeChange:
    if not isinstance(change, NoticeChange):
        raise TalkDeliveryInputError("batch entries must be NoticeChange objects")
    return change


def _assert_rendered_message_contains_source_link(*, change: NoticeChange, rendered: object) -> None:
    if not isinstance(rendered, str) or not rendered.strip():
        raise TalkDeliveryInvariantError("rendered message must be a non-empty string")

    source_link = getattr(change.notice, "source_link", None)
    if not isinstance(source_link, str) or not source_link.strip():
        raise TalkDeliveryInvariantError("notice.source_link must be a non-empty string")

    normalized_source_link = source_link.strip()
    if normalized_source_link not in rendered:
        raise TalkDeliveryInvariantError("rendered message must include notice.source_link")


def _build_render_failure(*, source_id: str, exc: Exception) -> TalkDeliveryFailure:
    detail: str
    if isinstance(exc, (TalkFormatterInputError, TalkDeliveryInputError, TalkDeliveryInvariantError)):
        detail = str(exc)
    else:
        detail = "Failed to render Talk message"

    return TalkDeliveryFailure(
        source_id=source_id,
        stage="render",
        error_class=exc.__class__.__name__,
        detail=detail,
        retryable=False,
        status_code=None,
    )
