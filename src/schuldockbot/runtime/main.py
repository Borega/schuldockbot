"""Executable runtime entrypoint for unattended Schuldock polling."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
import signal
import sys
from time import monotonic, sleep
from types import FrameType
from typing import TextIO

from schuldockbot.runtime.config import RuntimeStartupConfigError, load_runtime_config
from schuldockbot.runtime.http import build_schuldock_fetchers
from schuldockbot.runtime.service import (
    RuntimeCycleError,
    RuntimeLoopInvariantError,
    RuntimeLoopSummary,
    run_poll_cycle,
    run_polling_loop,
)
from schuldockbot.state import SqliteProcessedNoticeStore
from schuldockbot.talk import NextcloudTalkClient


@dataclass(slots=True)
class _StopLatch:
    """Signal-aware stop latch consumed by the long-running loop."""

    requested: bool = False
    signals: list[str] = field(default_factory=list)

    def request(self, signal_name: str | None = None) -> None:
        self.requested = True
        if signal_name is not None:
            self.signals.append(signal_name)

    def is_requested(self) -> bool:
        return self.requested


@dataclass(slots=True)
class RuntimeEventEmitter:
    """JSON-line event emitter for journald/systemd ingestion."""

    stream: TextIO

    def emit(self, event: str, payload: Mapping[str, object] | None = None) -> None:
        envelope: dict[str, object] = {
            "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "event": event,
        }
        if payload:
            envelope.update(_sanitize_payload(payload))

        self.stream.write(json.dumps(envelope, ensure_ascii=False, sort_keys=True) + "\n")
        self.stream.flush()


def build_argument_parser() -> argparse.ArgumentParser:
    """Build CLI parser for runtime invocation modes."""

    parser = argparse.ArgumentParser(
        prog="schuldockbot-runtime",
        description="Run Schuldock polling with bounded retries and structured logs.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one successful cycle then exit.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate startup wiring and emit lifecycle logs without polling.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint returning process exit code for systemd visibility."""

    parser = build_argument_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        exit_code = int(exc.code) if isinstance(exc.code, int) else 2
        if exit_code == 0:
            return 0

        emitter = RuntimeEventEmitter(sys.stdout)
        emitter.emit(
            "startup_validation_failed",
            {
                "phase": "cli",
                "error_class": "ArgumentParsingError",
                "category": "invalid_cli",
                "detail": "Invalid runtime CLI arguments",
            },
        )
        return exit_code

    return run_runtime(
        once=args.once,
        dry_run=args.dry_run,
    )


def run_runtime(
    *,
    once: bool,
    dry_run: bool,
    env: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
    load_config_fn: Callable[[Mapping[str, str] | None], object] = load_runtime_config,
    build_fetchers_fn: Callable[..., tuple[Callable[[], bytes], Callable[[], bytes]]] = build_schuldock_fetchers,
    run_cycle_fn: Callable[..., object] = run_poll_cycle,
    run_loop_fn: Callable[..., RuntimeLoopSummary] = run_polling_loop,
    store_factory: Callable[..., SqliteProcessedNoticeStore] = SqliteProcessedNoticeStore,
    talk_client_factory: Callable[..., NextcloudTalkClient] = NextcloudTalkClient,
    monotonic_fn: Callable[[], float] = monotonic,
    sleep_fn: Callable[[float], None] = sleep,
) -> int:
    """Run runtime bootstrapping + loop orchestration with explicit exit codes."""

    emitter = RuntimeEventEmitter(sys.stdout if stdout is None else stdout)
    runtime_env = env

    try:
        config = load_config_fn(runtime_env)
    except RuntimeStartupConfigError as exc:
        emitter.emit("startup_validation_failed", exc.to_safe_dict())
        return 2
    except Exception as exc:
        emitter.emit(
            "startup_validation_failed",
            {
                "phase": "config",
                "error_class": exc.__class__.__name__,
                "category": "startup_exception",
                "detail": "Unhandled startup configuration failure",
            },
        )
        return 1

    emitter.emit(
        "startup",
        {
            "mode": "once" if once else "loop",
            "dry_run": dry_run,
            "config": config.to_safe_dict(),
        },
    )

    store: SqliteProcessedNoticeStore | None = None
    restore_signal_handlers: Callable[[], None] | None = None

    try:
        store = store_factory(
            config.state.sqlite_db_path,
            timeout_seconds=config.state.sqlite_timeout_seconds,
        )
        talk_client = talk_client_factory(config.talk)
        json_fetcher, html_fetcher = build_fetchers_fn(
            json_url=config.source.json_url,
            html_url=config.source.html_url,
            timeout_seconds=config.talk.timeout_seconds,
        )
    except Exception as exc:
        emitter.emit(
            "startup_validation_failed",
            {
                "phase": "startup",
                "error_class": exc.__class__.__name__,
                "category": "startup_dependency",
                "detail": "Failed to initialize runtime dependencies",
            },
        )
        _close_store_quietly(store, emitter=emitter)
        return 1

    if dry_run:
        emitter.emit(
            "shutdown",
            {
                "exit_reason": "dry_run",
                "cycles_completed": 0,
                "total_attempts": 0,
                "retries_performed": 0,
            },
        )
        _close_store_quietly(store, emitter=emitter)
        return 0

    stop_latch = _StopLatch()
    restore_signal_handlers = _install_signal_handlers(stop_latch)

    def _cycle_runner() -> object:
        if store is None:  # pragma: no cover - defensive guard
            raise RuntimeLoopInvariantError("Processed store must be initialized before polling")

        return run_cycle_fn(
            json_fetcher=json_fetcher,
            html_fetcher=html_fetcher,
            talk_client=talk_client,
            processed_store=store,
        )

    shutdown_payload: dict[str, object]

    try:
        summary = run_loop_fn(
            cycle_runner=_cycle_runner,
            poll_interval_seconds=config.poll_interval_seconds,
            retry_max_attempts=config.retry.max_attempts,
            retry_backoff_seconds=config.retry.backoff_seconds,
            once=once,
            should_stop=stop_latch.is_requested,
            monotonic_fn=monotonic_fn,
            sleep_fn=sleep_fn,
            event_sink=emitter.emit,
        )
    except RuntimeCycleError as exc:
        return_code = 1
        failure = exc.to_safe_dict()
        shutdown_payload = {
            "exit_reason": "cycle_fatal",
            "failure_class": failure.get("failure_class"),
            "phase": failure.get("phase"),
            "retryable": failure.get("retryable"),
        }
    except RuntimeLoopInvariantError as exc:
        emitter.emit(
            "cycle_fatal",
            {
                "phase": "loop",
                "failure_class": "loop_invariant",
                "retryable": False,
                "detail": str(exc),
            },
        )
        return_code = 1
        shutdown_payload = {
            "exit_reason": "loop_invariant",
            "failure_class": "loop_invariant",
            "phase": "loop",
            "retryable": False,
        }
    except ValueError as exc:
        emitter.emit(
            "startup_validation_failed",
            {
                "phase": "loop_config",
                "error_class": exc.__class__.__name__,
                "category": "invalid_runtime_policy",
                "detail": str(exc),
            },
        )
        return_code = 2
        shutdown_payload = {
            "exit_reason": "startup_validation_failed",
            "phase": "loop_config",
            "failure_class": "invalid_runtime_policy",
        }
    except Exception as exc:
        emitter.emit(
            "cycle_fatal",
            {
                "phase": "loop",
                "failure_class": "loop_unhandled",
                "retryable": False,
                "detail": "Unhandled runtime loop exception",
                "error_class": exc.__class__.__name__,
            },
        )
        return_code = 1
        shutdown_payload = {
            "exit_reason": "loop_unhandled",
            "failure_class": "loop_unhandled",
            "phase": "loop",
            "retryable": False,
        }
    else:
        shutdown_payload = summary.to_safe_dict()
        return_code = 0

    if stop_latch.signals:
        shutdown_payload["signals"] = stop_latch.signals
    shutdown_payload["exit_code"] = return_code
    emitter.emit("shutdown", shutdown_payload)

    if restore_signal_handlers is not None:
        restore_signal_handlers()
    _close_store_quietly(store, emitter=emitter)

    return return_code


def entrypoint() -> None:
    """Console-script wrapper that exits with ``main`` return code."""

    raise SystemExit(main())


def _install_signal_handlers(stop_latch: _StopLatch) -> Callable[[], None]:
    previous_handlers: dict[signal.Signals, signal.Handlers] = {}

    def _handle_signal(signum: int, _frame: FrameType | None) -> None:
        try:
            signal_name = signal.Signals(signum).name
        except ValueError:
            signal_name = f"SIG{signum}"
        stop_latch.request(signal_name)

    candidates: list[signal.Signals] = []
    for maybe_signal in (signal.SIGINT, signal.SIGTERM):
        if maybe_signal is not None:
            candidates.append(maybe_signal)

    for sig in candidates:
        try:
            previous_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, _handle_signal)
        except (ValueError, OSError, RuntimeError):
            continue

    def _restore() -> None:
        for sig, previous in previous_handlers.items():
            try:
                signal.signal(sig, previous)
            except (ValueError, OSError, RuntimeError):
                continue

    return _restore


def _close_store_quietly(
    store: SqliteProcessedNoticeStore | None,
    *,
    emitter: RuntimeEventEmitter,
) -> None:
    if store is None:
        return

    try:
        store.close()
    except Exception as exc:
        emitter.emit(
            "shutdown_warning",
            {
                "phase": "shutdown",
                "failure_class": "store_close_failed",
                "detail": "Failed to close state store cleanly",
                "error_class": exc.__class__.__name__,
            },
        )


def _sanitize_payload(payload: Mapping[str, object]) -> dict[str, object]:
    return {str(key): _sanitize_value(str(key), value) for key, value in payload.items()}


def _sanitize_value(key: str, value: object) -> object:
    lower_key = key.lower()
    if any(marker in lower_key for marker in _SENSITIVE_KEY_MARKERS):
        return "[REDACTED]"

    if isinstance(value, str):
        if any(marker in value.lower() for marker in _SENSITIVE_VALUE_MARKERS):
            return "[REDACTED]"
        return value

    if isinstance(value, Mapping):
        return {str(child_key): _sanitize_value(str(child_key), child_value) for child_key, child_value in value.items()}

    if isinstance(value, list):
        return [_sanitize_value(key, item) for item in value]

    if isinstance(value, tuple):
        return [_sanitize_value(key, item) for item in value]

    return value


_SENSITIVE_KEY_MARKERS = (
    "authorization",
    "password",
    "token",
)

_SENSITIVE_VALUE_MARKERS = (
    "authorization",
    "app_password",
    "room_token",
)


if __name__ == "__main__":
    raise SystemExit(main())
