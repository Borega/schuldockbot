"""One-cycle runtime orchestration for fetch → detect → deliver execution."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
import socket
from typing import Any, Protocol

from schuldockbot.ingestion import NoticeRecord, SourceSelectionError, fetch_notices
from schuldockbot.ingestion.json_source import JsonFailureClass
from schuldockbot.state import InvalidProcessedNoticeError, NoticeChange, StateStoreLockTimeoutError, detect_notice_changes
from schuldockbot.talk import (
    TalkClientLike,
    TalkDeliveryAckError,
    TalkDeliveryFailure,
    TalkDeliverySummary,
    deliver_notice_changes,
)


class RuntimeProcessedNoticeStoreLike(Protocol):
    """Combined read/write store contract needed for one runtime cycle."""

    def get_revision_token(self, source_id: str) -> str | None: ...

    def mark_processed_batch(self, notices: Iterable[object]) -> int: ...


@dataclass(frozen=True, slots=True)
class RuntimeCycleResult:
    """Structured cycle counters and source-selection summary."""

    source_mode: str | None
    fallback_reason: str | None
    fetched: int
    detected: int
    attempted: int
    posted: int
    failed: int
    acked: int

    def to_safe_dict(self) -> dict[str, object]:
        """Return diagnostics-safe machine-readable cycle counters."""

        return {
            "source_mode": self.source_mode,
            "fallback_reason": self.fallback_reason,
            "fetched": self.fetched,
            "detected": self.detected,
            "attempted": self.attempted,
            "posted": self.posted,
            "failed": self.failed,
            "acked": self.acked,
        }


@dataclass(slots=True)
class RuntimeCycleError(RuntimeError):
    """Typed cycle failure with redaction-safe diagnostics for retry/fatal branching."""

    phase: str
    failure_class: str
    retryable: bool
    detail: str
    result: RuntimeCycleResult
    pending_source_ids: tuple[str, ...] = ()
    failure_context: dict[str, object] | None = None

    def __post_init__(self) -> None:
        RuntimeError.__init__(self, _sanitize_text(self.detail))
        self.detail = _sanitize_text(self.detail)

    def to_safe_dict(self) -> dict[str, object]:
        """Return diagnostics-safe payload for cycle-level observability."""

        payload = self.result.to_safe_dict()
        payload.update(
            {
                "phase": self.phase,
                "failure_class": self.failure_class,
                "retryable": self.retryable,
                "detail": self.detail,
                "pending_source_ids": list(self.pending_source_ids),
            }
        )
        if self.failure_context is not None:
            payload["failure_context"] = _sanitize_value(self.failure_context)
        return payload


FetchNoticesFn = Callable[
    [Callable[[], str | bytes | bytearray], Callable[[], str | bytes | bytearray]],
    Any,
]
DetectChangesFn = Callable[..., list[NoticeChange]]
DeliverChangesFn = Callable[..., TalkDeliverySummary]


def run_poll_cycle(
    *,
    json_fetcher: Callable[[], str | bytes | bytearray],
    html_fetcher: Callable[[], str | bytes | bytearray],
    talk_client: TalkClientLike,
    processed_store: RuntimeProcessedNoticeStoreLike,
    fetch_notices_fn: FetchNoticesFn = fetch_notices,
    detect_changes_fn: DetectChangesFn = detect_notice_changes,
    deliver_changes_fn: DeliverChangesFn = deliver_notice_changes,
) -> RuntimeCycleResult:
    """Execute one deterministic polling cycle and raise typed errors on failure."""

    try:
        selection = fetch_notices_fn(json_fetcher, html_fetcher)
    except SourceSelectionError as exc:
        source_mode, fallback_reason = _extract_source_mode_and_reason(exc.to_safe_dict())
        failure_class, retryable = _classify_fetch_failure(exc, fallback_reason=fallback_reason)
        raise RuntimeCycleError(
            phase="fetch",
            failure_class=failure_class,
            retryable=retryable,
            detail="Failed to fetch notices from JSON/HTML sources",
            result=RuntimeCycleResult(
                source_mode=source_mode,
                fallback_reason=fallback_reason,
                fetched=0,
                detected=0,
                attempted=0,
                posted=0,
                failed=0,
                acked=0,
            ),
            failure_context=exc.to_safe_dict(),
        ) from exc

    source_diagnostics = selection.to_safe_dict()
    source_mode, fallback_reason = _extract_source_mode_and_reason(source_diagnostics)

    try:
        notices = _coerce_notice_records(selection.records)
        changes = detect_changes_fn(notices, revision_lookup=processed_store.get_revision_token)
    except InvalidProcessedNoticeError as exc:
        raise RuntimeCycleError(
            phase="detect",
            failure_class="detect_invalid_state",
            retryable=False,
            detail=str(exc),
            result=RuntimeCycleResult(
                source_mode=source_mode,
                fallback_reason=fallback_reason,
                fetched=len(selection.records),
                detected=0,
                attempted=0,
                posted=0,
                failed=0,
                acked=0,
            ),
            failure_context={"error_class": exc.__class__.__name__, "detail": str(exc)},
        ) from exc
    except Exception as exc:
        failure_class, retryable = _classify_detect_exception(exc)
        raise RuntimeCycleError(
            phase="detect",
            failure_class=failure_class,
            retryable=retryable,
            detail="Failed to detect notice changes",
            result=RuntimeCycleResult(
                source_mode=source_mode,
                fallback_reason=fallback_reason,
                fetched=len(selection.records),
                detected=0,
                attempted=0,
                posted=0,
                failed=0,
                acked=0,
            ),
            failure_context={"error_class": exc.__class__.__name__},
        ) from exc

    try:
        delivery_summary = deliver_changes_fn(
            changes,
            talk_client=talk_client,
            processed_store=processed_store,
        )
    except TalkDeliveryAckError as exc:
        failure_class, retryable = _classify_ack_failure(exc)
        raise RuntimeCycleError(
            phase="ack",
            failure_class=failure_class,
            retryable=retryable,
            detail="Failed to persist processed-notice acknowledgements",
            result=RuntimeCycleResult(
                source_mode=source_mode,
                fallback_reason=fallback_reason,
                fetched=len(selection.records),
                detected=len(changes),
                attempted=exc.summary.attempted,
                posted=exc.summary.posted,
                failed=exc.summary.failed,
                acked=exc.summary.acked,
            ),
            pending_source_ids=exc.posted_source_ids,
            failure_context=exc.to_safe_dict(),
        ) from exc

    result = RuntimeCycleResult(
        source_mode=source_mode,
        fallback_reason=fallback_reason,
        fetched=len(selection.records),
        detected=len(changes),
        attempted=delivery_summary.attempted,
        posted=delivery_summary.posted,
        failed=delivery_summary.failed,
        acked=delivery_summary.acked,
    )

    if delivery_summary.failed > 0:
        failure_class, retryable = _classify_delivery_failures(delivery_summary.failures)
        raise RuntimeCycleError(
            phase="deliver",
            failure_class=failure_class,
            retryable=retryable,
            detail="One or more Talk posts failed",
            result=result,
            pending_source_ids=delivery_summary.failed_source_ids,
            failure_context=delivery_summary.to_safe_dict(),
        )

    return result


def _coerce_notice_records(records: list[NoticeRecord]) -> list[NoticeRecord]:
    coerced: list[NoticeRecord] = []
    for index, record in enumerate(records):
        if not isinstance(record, NoticeRecord):
            raise InvalidProcessedNoticeError(
                f"source selection returned non-NoticeRecord entry at index {index}"
            )
        coerced.append(record)
    return coerced


def _extract_source_mode_and_reason(diagnostics: dict[str, object]) -> tuple[str | None, str | None]:
    source_mode = diagnostics.get("source_mode")
    fallback_reason = diagnostics.get("fallback_reason")

    normalized_source_mode = source_mode if isinstance(source_mode, str) else None
    normalized_fallback_reason = fallback_reason if isinstance(fallback_reason, str) else None

    return normalized_source_mode, normalized_fallback_reason


def _classify_fetch_failure(
    error: SourceSelectionError,
    *,
    fallback_reason: str | None,
) -> tuple[str, bool]:
    if _is_timeout_exception(error):
        return "fetch_timeout", True

    if fallback_reason == JsonFailureClass.FETCH.value:
        return "fetch_error", True

    if fallback_reason in {JsonFailureClass.DECODE.value, JsonFailureClass.SCHEMA.value}:
        return "fetch_malformed", False

    return "fetch_error", True


def _classify_detect_exception(error: Exception) -> tuple[str, bool]:
    if isinstance(error, StateStoreLockTimeoutError) or _is_timeout_exception(error):
        return "detect_timeout", True
    return "detect_failure", True


def _classify_ack_failure(error: TalkDeliveryAckError) -> tuple[str, bool]:
    cause = error.__cause__

    if isinstance(cause, StateStoreLockTimeoutError):
        return "ack_timeout", True

    if isinstance(cause, InvalidProcessedNoticeError):
        return "ack_invalid_state", False

    if _is_timeout_exception(error):
        return "ack_timeout", True

    return "ack_failure", False


def _classify_delivery_failures(failures: tuple[TalkDeliveryFailure, ...]) -> tuple[str, bool]:
    if any(failure.error_class == "rate_limited" for failure in failures):
        return "deliver_rate_limited", True

    if any(failure.error_class == "timeout" for failure in failures):
        return "deliver_timeout", True

    if any(
        failure.error_class
        in {
            "TalkDeliveryInputError",
            "TalkFormatterInputError",
            "TalkDeliveryInvariantError",
        }
        for failure in failures
    ):
        return "deliver_input", False

    retryable = any(failure.retryable for failure in failures)
    if retryable:
        return "deliver_retryable", True
    return "deliver_failure", False


def _is_timeout_exception(error: BaseException) -> bool:
    for current in _exception_chain(error):
        if isinstance(current, (TimeoutError, socket.timeout, StateStoreLockTimeoutError)):
            return True

        message = str(current).lower()
        if "timeout" in message or "timed out" in message:
            return True
        if "database is locked" in message or "database table is locked" in message:
            return True

    return False


def _exception_chain(error: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    seen: set[int] = set()
    cursor: BaseException | None = error

    while cursor is not None and id(cursor) not in seen:
        seen.add(id(cursor))
        chain.append(cursor)
        cursor = cursor.__cause__ or cursor.__context__

    return chain


_SENSITIVE_MARKERS = (
    "authorization",
    "app_password",
    "room_token",
)


def _sanitize_text(value: str) -> str:
    lowered = value.lower()
    if any(marker in lowered for marker in _SENSITIVE_MARKERS):
        return "[REDACTED]"
    return value


def _sanitize_value(value: object) -> object:
    if isinstance(value, str):
        return _sanitize_text(value)
    if isinstance(value, dict):
        return {str(key): _sanitize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_value(item) for item in value]
    return value


__all__ = [
    "RuntimeCycleError",
    "RuntimeCycleResult",
    "RuntimeProcessedNoticeStoreLike",
    "run_poll_cycle",
]
