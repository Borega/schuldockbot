from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
import json
from pathlib import Path
import shutil
import sqlite3
from tempfile import mkdtemp
from threading import Thread
from typing import Any
from urllib.parse import urlsplit

import pytest

from schuldockbot.ingestion.json_source import decode_issues_payload
from schuldockbot.runtime.main import run_runtime
from schuldockbot.state import SqliteProcessedNoticeStore

TESTS_DIR = Path(__file__).resolve().parents[1]
FIXTURE_DIR = TESTS_DIR / "fixtures" / "schuldock"


@dataclass(slots=True)
class _TalkRequest:
    path: str
    message: str
    ocs_api_request: str | None
    authorization_scheme: str | None
    content_type: str | None


@dataclass(slots=True)
class _FixtureServerState:
    json_payload: bytes
    html_payload: bytes
    talk_room_token: str
    talk_status_code: int = 201
    talk_payload: bytes = field(default_factory=lambda: _ocs_payload())
    talk_requests: list[_TalkRequest] = field(default_factory=list)


class _FixtureHttpServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        request_handler: type[BaseHTTPRequestHandler],
        *,
        state: _FixtureServerState,
    ) -> None:
        super().__init__(server_address, request_handler)
        self.state = state


class _FixtureRequestHandler(BaseHTTPRequestHandler):
    server: _FixtureHttpServer

    def do_GET(self) -> None:  # noqa: N802
        state = self.server.state
        path = urlsplit(self.path).path

        if path == "/schuldock/v1/issues":
            self._send_response(200, state.json_payload, content_type="application/json")
            return

        if path in {"/aktuelle-meldungen", "/aktuelle-meldungen/"}:
            self._send_response(200, state.html_payload, content_type="text/html; charset=utf-8")
            return

        self._send_response(404, b"not found", content_type="text/plain; charset=utf-8")

    def do_POST(self) -> None:  # noqa: N802
        state = self.server.state
        path = urlsplit(self.path).path
        expected_path = f"/ocs/v2.php/apps/spreed/api/v1/chat/{state.talk_room_token}"

        if path != expected_path:
            self._send_response(404, b"not found", content_type="text/plain; charset=utf-8")
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        request_body = self.rfile.read(content_length)

        try:
            parsed = json.loads(request_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._send_response(400, b'{"ocs": {"meta": {"status": "failure", "statuscode": 400, "message": "invalid json"}}}')
            return

        message = parsed.get("message")
        if not isinstance(message, str) or not message.strip():
            self._send_response(400, b'{"ocs": {"meta": {"status": "failure", "statuscode": 400, "message": "missing message"}}}')
            return

        authorization = self.headers.get("Authorization")
        authorization_scheme = None
        if authorization is not None:
            authorization_scheme = authorization.split(" ", maxsplit=1)[0]

        state.talk_requests.append(
            _TalkRequest(
                path=path,
                message=message,
                ocs_api_request=self.headers.get("OCS-APIRequest"),
                authorization_scheme=authorization_scheme,
                content_type=self.headers.get("Content-Type"),
            )
        )

        self._send_response(
            state.talk_status_code,
            state.talk_payload,
            content_type="application/json; charset=utf-8",
        )

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        del format, args

    def _send_response(self, status_code: int, payload: bytes, *, content_type: str = "application/json") -> None:
        self.send_response(status_code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def _ocs_payload(*, status: str = "ok", statuscode: int = 100, message: str = "OK") -> bytes:
    return json.dumps(
        {
            "ocs": {
                "meta": {
                    "status": status,
                    "statuscode": statuscode,
                    "message": message,
                }
            }
        },
        separators=(",", ":"),
    ).encode("utf-8")


@pytest.fixture
def integration_tmp_path() -> Path:
    root = Path(".pytest-tmp")
    root.mkdir(parents=True, exist_ok=True)

    path = Path(mkdtemp(prefix="runtime-e2e-", dir=root))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


@contextmanager
def _running_fixture_server(state: _FixtureServerState):
    server = _FixtureHttpServer(("127.0.0.1", 0), _FixtureRequestHandler, state=state)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()

    host, port = server.server_address
    base_url = f"http://{host}:{port}"

    try:
        yield base_url
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _load_fixture_bytes(filename: str) -> bytes:
    return (FIXTURE_DIR / filename).read_bytes()


def _build_runtime_env(
    *,
    base_url: str,
    db_path: Path,
    room_token: str,
    sqlite_timeout_seconds: float = 2.0,
) -> dict[str, str]:
    return {
        "SCHULDOCKBOT_SOURCE_JSON_URL": f"{base_url}/schuldock/v1/issues",
        "SCHULDOCKBOT_SOURCE_HTML_URL": f"{base_url}/aktuelle-meldungen",
        "SCHULDOCKBOT_POLL_INTERVAL_SECONDS": "10",
        "SCHULDOCKBOT_RETRY_MAX_ATTEMPTS": "1",
        "SCHULDOCKBOT_RETRY_BACKOFF_SECONDS": "1.0",
        "SCHULDOCKBOT_STATE_DB_PATH": str(db_path),
        "SCHULDOCKBOT_SQLITE_TIMEOUT_SECONDS": str(sqlite_timeout_seconds),
        "SCHULDOCKBOT_TALK_BASE_URL": base_url,
        "SCHULDOCKBOT_TALK_ROOM_TOKEN": room_token,
        "SCHULDOCKBOT_TALK_USERNAME": "bot-user",
        "SCHULDOCKBOT_TALK_APP_PASSWORD": "integration-app-password",
        "SCHULDOCKBOT_TALK_TIMEOUT_SECONDS": "2.0",
    }


def _run_runtime_once(env: dict[str, str]) -> tuple[int, list[dict[str, Any]], str]:
    output = StringIO()
    exit_code = run_runtime(
        once=True,
        dry_run=False,
        env=env,
        stdout=output,
    )
    raw_output = output.getvalue()
    events = [json.loads(line) for line in raw_output.splitlines() if line.strip()]
    return exit_code, events, raw_output


def _event_names(events: list[dict[str, Any]]) -> list[str]:
    return [str(event["event"]) for event in events]


def _read_revision_tokens(*, db_path: Path, source_ids: list[str]) -> dict[str, str | None]:
    store = SqliteProcessedNoticeStore(db_path)
    try:
        return {source_id: store.get_revision_token(source_id) for source_id in source_ids}
    finally:
        store.close()


def _read_processed_count(db_path: Path) -> int:
    store = SqliteProcessedNoticeStore(db_path)
    try:
        return store.get_processed_count()
    finally:
        store.close()


def test_runtime_end_to_end_posts_new_then_update_with_persisted_state(
    integration_tmp_path: Path,
) -> None:
    room_token = "integration-room"
    cycle1_json_payload = _load_fixture_bytes("issues.json")
    cycle2_json_payload = _load_fixture_bytes("issues-cycle2.json")

    cycle1_records = decode_issues_payload(cycle1_json_payload)
    cycle2_records = decode_issues_payload(cycle2_json_payload)

    cycle1_revision_tokens = {record.source_id: record.revision_token for record in cycle1_records}
    cycle2_revision_tokens = {record.source_id: record.revision_token for record in cycle2_records}

    assert cycle1_revision_tokens["84572"] != cycle2_revision_tokens["84572"]
    assert cycle1_revision_tokens["84573"] == cycle2_revision_tokens["84573"]

    state = _FixtureServerState(
        json_payload=cycle1_json_payload,
        html_payload=_load_fixture_bytes("aktuelle-meldungen.html"),
        talk_room_token=room_token,
    )

    db_path = integration_tmp_path / "runtime-e2e.sqlite3"

    with _running_fixture_server(state) as base_url:
        env = _build_runtime_env(base_url=base_url, db_path=db_path, room_token=room_token)

        exit_code_cycle1, events_cycle1, raw_cycle1 = _run_runtime_once(env)

        assert exit_code_cycle1 == 0
        assert _event_names(events_cycle1) == ["startup", "cycle_started", "cycle_completed", "shutdown"]

        cycle_completed_1 = events_cycle1[2]
        assert cycle_completed_1["source_mode"] == "json"
        assert cycle_completed_1["fallback_reason"] is None
        assert cycle_completed_1["fetched"] == 2
        assert cycle_completed_1["detected"] == 2
        assert cycle_completed_1["attempted"] == 2
        assert cycle_completed_1["posted"] == 2
        assert cycle_completed_1["failed"] == 0
        assert cycle_completed_1["acked"] == 2

        assert len(state.talk_requests) == 2
        first_cycle_messages = [request.message for request in state.talk_requests]
        assert all(message.startswith("### NEW:") for message in first_cycle_messages)
        assert all("Source: [Open notice](" in message for message in first_cycle_messages)
        assert any("https://schuldock.hamburg/issues/84572/" in message for message in first_cycle_messages)
        assert any("https://schuldock.hamburg/issues/84573/" in message for message in first_cycle_messages)
        assert all(request.ocs_api_request == "true" for request in state.talk_requests)
        assert all(request.authorization_scheme == "Basic" for request in state.talk_requests)
        assert all(request.content_type == "application/json; charset=utf-8" for request in state.talk_requests)

        assert _read_revision_tokens(db_path=db_path, source_ids=["84572", "84573"]) == cycle1_revision_tokens

        state.json_payload = cycle2_json_payload

        exit_code_cycle2, events_cycle2, raw_cycle2 = _run_runtime_once(env)

        assert exit_code_cycle2 == 0
        assert _event_names(events_cycle2) == ["startup", "cycle_started", "cycle_completed", "shutdown"]

        cycle_completed_2 = events_cycle2[2]
        assert cycle_completed_2["source_mode"] == "json"
        assert cycle_completed_2["fallback_reason"] is None
        assert cycle_completed_2["fetched"] == 2
        assert cycle_completed_2["detected"] == 1
        assert cycle_completed_2["attempted"] == 1
        assert cycle_completed_2["posted"] == 1
        assert cycle_completed_2["failed"] == 0
        assert cycle_completed_2["acked"] == 1

        assert len(state.talk_requests) == 3
        update_message = state.talk_requests[-1].message
        assert update_message.startswith("### UPDATE:")
        assert "https://schuldock.hamburg/issues/84572/" in update_message
        assert "https://schuldock.hamburg/issues/84573/" not in update_message

        assert _read_revision_tokens(db_path=db_path, source_ids=["84572", "84573"]) == cycle2_revision_tokens
        assert _read_processed_count(db_path) == 2

    combined_runtime_output = f"{raw_cycle1}\n{raw_cycle2}"
    assert "integration-app-password" not in combined_runtime_output
    assert room_token not in combined_runtime_output
    assert "Authorization" not in combined_runtime_output
    assert "app_password" not in combined_runtime_output


def test_runtime_end_to_end_malformed_source_payload_does_not_post_or_ack(
    integration_tmp_path: Path,
) -> None:
    state = _FixtureServerState(
        json_payload=b"{",
        html_payload=b"<html><body><main><p>broken</p></main></body></html>",
        talk_room_token="integration-room",
    )

    db_path = integration_tmp_path / "malformed-source.sqlite3"

    with _running_fixture_server(state) as base_url:
        env = _build_runtime_env(
            base_url=base_url,
            db_path=db_path,
            room_token=state.talk_room_token,
        )
        exit_code, events, _raw_output = _run_runtime_once(env)

    assert exit_code == 1
    assert _event_names(events) == ["startup", "cycle_started", "cycle_fatal", "shutdown"]

    cycle_fatal = events[2]
    assert cycle_fatal["phase"] == "fetch"
    assert cycle_fatal["failure_class"] == "fetch_malformed"
    assert cycle_fatal["attempted"] == 0
    assert cycle_fatal["posted"] == 0
    assert cycle_fatal["failed"] == 0
    assert cycle_fatal["acked"] == 0

    assert state.talk_requests == []
    assert _read_processed_count(db_path) == 0


def test_runtime_end_to_end_malformed_talk_ocs_response_stays_unacked(
    integration_tmp_path: Path,
) -> None:
    state = _FixtureServerState(
        json_payload=_load_fixture_bytes("issues.json"),
        html_payload=_load_fixture_bytes("aktuelle-meldungen.html"),
        talk_room_token="integration-room",
        talk_status_code=201,
        talk_payload=b'{"ocs": {}}',
    )

    db_path = integration_tmp_path / "malformed-talk.sqlite3"

    with _running_fixture_server(state) as base_url:
        env = _build_runtime_env(
            base_url=base_url,
            db_path=db_path,
            room_token=state.talk_room_token,
        )
        exit_code, events, raw_output = _run_runtime_once(env)

    assert exit_code == 1
    assert _event_names(events) == ["startup", "cycle_started", "cycle_fatal", "shutdown"]

    cycle_fatal = events[2]
    assert cycle_fatal["phase"] == "deliver"
    assert cycle_fatal["failure_class"] == "deliver_failure"
    assert cycle_fatal["attempted"] == 2
    assert cycle_fatal["posted"] == 0
    assert cycle_fatal["failed"] == 2
    assert cycle_fatal["acked"] == 0
    assert sorted(cycle_fatal["pending_source_ids"]) == ["84572", "84573"]

    assert len(state.talk_requests) == 2
    assert _read_processed_count(db_path) == 0

    assert "integration-app-password" not in raw_output
    assert "Authorization" not in raw_output


def test_runtime_end_to_end_ack_timeout_reports_pending_source_ids(
    integration_tmp_path: Path,
) -> None:
    state = _FixtureServerState(
        json_payload=_load_fixture_bytes("issues.json"),
        html_payload=_load_fixture_bytes("aktuelle-meldungen.html"),
        talk_room_token="integration-room",
    )

    db_path = integration_tmp_path / "ack-timeout.sqlite3"

    bootstrap_store = SqliteProcessedNoticeStore(db_path)
    bootstrap_store.close()

    sqlite_lock_connection = sqlite3.connect(db_path, timeout=0.1)
    sqlite_lock_connection.execute("BEGIN IMMEDIATE")

    try:
        with _running_fixture_server(state) as base_url:
            env = _build_runtime_env(
                base_url=base_url,
                db_path=db_path,
                room_token=state.talk_room_token,
                sqlite_timeout_seconds=0.1,
            )
            exit_code, events, _raw_output = _run_runtime_once(env)
    finally:
        sqlite_lock_connection.rollback()
        sqlite_lock_connection.close()

    assert exit_code == 1
    assert _event_names(events) == ["startup", "cycle_started", "cycle_fatal", "shutdown"]

    cycle_fatal = events[2]
    assert cycle_fatal["phase"] == "ack"
    assert cycle_fatal["failure_class"] == "ack_timeout"
    assert cycle_fatal["retryable"] is True
    assert cycle_fatal["attempted"] == 2
    assert cycle_fatal["posted"] == 2
    assert cycle_fatal["failed"] == 0
    assert cycle_fatal["acked"] == 0
    assert sorted(cycle_fatal["pending_source_ids"]) == ["84572", "84573"]

    assert len(state.talk_requests) == 2
    assert _read_processed_count(db_path) == 0
