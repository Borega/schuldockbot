from __future__ import annotations

from dataclasses import dataclass
from io import StringIO
import json
from pathlib import Path
import shutil
import tempfile
from collections.abc import Iterator

import pytest

from schuldockbot.runtime.config import RuntimeConfig, RuntimeRetryPolicy, RuntimeSourceConfig, RuntimeStateConfig
from schuldockbot.runtime.main import run_runtime
from schuldockbot.runtime.service import RuntimeCycleError, RuntimeCycleResult
from schuldockbot.talk import build_talk_client_config


@pytest.fixture
def workspace_tmp_path() -> Iterator[Path]:
    root = Path(".pytest-tmp")
    root.mkdir(parents=True, exist_ok=True)

    path = Path(tempfile.mkdtemp(prefix="runtime-main-", dir=root))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


@dataclass(slots=True)
class _FakeStore:
    closed: bool = False

    def close(self) -> None:
        self.closed = True


class _StepClock:
    def __init__(self, step: float = 0.25) -> None:
        self._current = -step
        self._step = step

    def __call__(self) -> float:
        self._current += self._step
        return self._current


def _build_runtime_config(
    tmp_path: Path,
    *,
    poll_interval_seconds: int = 60,
    retry_max_attempts: int = 3,
    retry_backoff_seconds: float = 0.5,
) -> RuntimeConfig:
    talk = build_talk_client_config(
        base_url="https://cloud.example/",
        room_token="roomToken123",
        username="bot-user",
        app_password="secret-app-password",
        timeout_seconds=5.0,
    )

    return RuntimeConfig(
        source=RuntimeSourceConfig(
            json_url="https://schuldock.hamburg/wp-json/schuldock/v1/issues",
            html_url="https://schuldock.hamburg/aktuelle-meldungen",
        ),
        poll_interval_seconds=poll_interval_seconds,
        retry=RuntimeRetryPolicy(
            max_attempts=retry_max_attempts,
            backoff_seconds=retry_backoff_seconds,
        ),
        state=RuntimeStateConfig(
            sqlite_db_path=str(tmp_path / "state.db"),
            sqlite_timeout_seconds=2.0,
        ),
        talk=talk,
    )


def _parse_events(stream: StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]


def test_run_runtime_once_executes_single_cycle_and_emits_structured_logs(
    workspace_tmp_path: Path,
) -> None:
    output = StringIO()
    config = _build_runtime_config(workspace_tmp_path, poll_interval_seconds=30)
    store = _FakeStore()

    def _run_cycle(**_kwargs) -> RuntimeCycleResult:
        return RuntimeCycleResult(
            source_mode="json",
            fallback_reason=None,
            fetched=2,
            detected=2,
            attempted=2,
            posted=2,
            failed=0,
            acked=2,
        )

    exit_code = run_runtime(
        once=True,
        dry_run=False,
        stdout=output,
        load_config_fn=lambda _env: config,
        build_fetchers_fn=lambda **_kwargs: (lambda: b"{}", lambda: b"<html></html>"),
        run_cycle_fn=_run_cycle,
        store_factory=lambda *_args, **_kwargs: store,
        talk_client_factory=lambda _cfg: object(),
        monotonic_fn=_StepClock(step=0.25),
        sleep_fn=lambda _seconds: None,
    )

    assert exit_code == 0
    assert store.closed is True

    events = _parse_events(output)
    names = [event["event"] for event in events]
    assert names == ["startup", "cycle_started", "cycle_completed", "shutdown"]

    cycle_completed = events[2]
    assert cycle_completed["cycle_index"] == 1
    assert cycle_completed["retry_attempt"] == 1
    assert cycle_completed["retry_max_attempts"] == 3
    assert cycle_completed["fetched"] == 2
    assert cycle_completed["posted"] == 2
    assert cycle_completed["sleep_seconds"] == 29.75

    shutdown = events[3]
    assert shutdown["exit_reason"] == "once"
    assert shutdown["cycles_completed"] == 1
    assert shutdown["exit_code"] == 0

    serialized = output.getvalue().lower()
    assert "secret-app-password" not in serialized
    assert "authorization" not in serialized


def test_run_runtime_retries_retryable_cycle_error_then_exits_with_fatal_status(
    workspace_tmp_path: Path,
) -> None:
    output = StringIO()
    config = _build_runtime_config(
        workspace_tmp_path,
        poll_interval_seconds=30,
        retry_max_attempts=2,
        retry_backoff_seconds=0.5,
    )
    store = _FakeStore()
    sleep_calls: list[float] = []

    attempts = {"count": 0}

    def _run_cycle(**_kwargs) -> RuntimeCycleResult:
        attempts["count"] += 1
        raise RuntimeCycleError(
            phase="fetch",
            failure_class="fetch_timeout",
            retryable=True,
            detail="Upstream timed out",
            result=RuntimeCycleResult(
                source_mode="json",
                fallback_reason=None,
                fetched=0,
                detected=0,
                attempted=0,
                posted=0,
                failed=0,
                acked=0,
            ),
        )

    exit_code = run_runtime(
        once=False,
        dry_run=False,
        stdout=output,
        load_config_fn=lambda _env: config,
        build_fetchers_fn=lambda **_kwargs: (lambda: b"{}", lambda: b"<html></html>"),
        run_cycle_fn=_run_cycle,
        store_factory=lambda *_args, **_kwargs: store,
        talk_client_factory=lambda _cfg: object(),
        monotonic_fn=_StepClock(step=0.1),
        sleep_fn=lambda seconds: sleep_calls.append(seconds),
    )

    assert exit_code == 1
    assert attempts["count"] == 2
    assert sleep_calls == [0.5]

    events = _parse_events(output)
    names = [event["event"] for event in events]
    assert names == [
        "startup",
        "cycle_started",
        "cycle_retry",
        "cycle_started",
        "cycle_fatal",
        "shutdown",
    ]

    retry_event = events[2]
    assert retry_event["failure_class"] == "fetch_timeout"
    assert retry_event["retry_attempt"] == 1
    assert retry_event["retry_max_attempts"] == 2
    assert retry_event["sleep_seconds"] == 0.5

    fatal_event = events[4]
    assert fatal_event["failure_class"] == "fetch_timeout"
    assert fatal_event["retry_attempt"] == 2
    assert fatal_event["retryable"] is True

    shutdown = events[5]
    assert shutdown["exit_reason"] == "cycle_fatal"
    assert shutdown["failure_class"] == "fetch_timeout"
    assert shutdown["exit_code"] == 1


def test_run_runtime_rejects_invalid_loop_policy_before_cycle_start(
    workspace_tmp_path: Path,
) -> None:
    output = StringIO()
    config = _build_runtime_config(
        workspace_tmp_path,
        poll_interval_seconds=0,
        retry_max_attempts=3,
        retry_backoff_seconds=1.0,
    )

    exit_code = run_runtime(
        once=False,
        dry_run=False,
        stdout=output,
        load_config_fn=lambda _env: config,
        build_fetchers_fn=lambda **_kwargs: (lambda: b"{}", lambda: b"<html></html>"),
        run_cycle_fn=lambda **_kwargs: pytest.fail("cycle should not run when policy is invalid"),
        store_factory=lambda *_args, **_kwargs: _FakeStore(),
        talk_client_factory=lambda _cfg: object(),
        monotonic_fn=_StepClock(step=0.1),
        sleep_fn=lambda _seconds: None,
    )

    assert exit_code == 2

    events = _parse_events(output)
    names = [event["event"] for event in events]
    assert names == ["startup", "startup_validation_failed", "shutdown"]

    validation = events[1]
    assert validation["phase"] == "loop_config"
    assert validation["category"] == "invalid_runtime_policy"

    shutdown = events[2]
    assert shutdown["exit_reason"] == "startup_validation_failed"
    assert shutdown["exit_code"] == 2
