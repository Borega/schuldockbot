from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from urllib.request import Request

import pytest

from schuldockbot.talk import (
    NextcloudTalkClient,
    TalkClientConfigError,
    TalkFailureClass,
    TalkPostError,
    build_talk_client_config,
)


@dataclass(slots=True)
class _FakeResponse:
    status: int
    payload: bytes
    closed: bool = False

    def read(self) -> bytes:
        return self.payload

    def close(self) -> None:
        self.closed = True


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


def _build_config():
    return build_talk_client_config(
        base_url="https://cloud.example/",
        room_token="abcDEF123",
        username="bot-user",
        app_password="super-secret-password",
        timeout_seconds=7,
    )


def test_post_message_builds_single_deterministic_request_and_maps_201_success() -> None:
    config = _build_config()
    captured: list[tuple[Request, float]] = []

    def sender(request: Request, timeout: float) -> _FakeResponse:
        captured.append((request, timeout))
        return _FakeResponse(status=201, payload=_ocs_payload())

    client = NextcloudTalkClient(config, sender=sender)
    result = client.post_message(source_id="notice-1", message="Message payload")

    assert len(captured) == 1
    request, timeout = captured[0]

    assert request.get_method() == "POST"
    assert timeout == 7.0
    assert request.full_url == "https://cloud.example/ocs/v2.php/apps/spreed/api/v1/chat/abcDEF123"

    header_map = {key.lower(): value for key, value in request.header_items()}
    assert header_map["ocs-apirequest"] == "true"
    assert header_map["accept"] == "application/json"
    assert header_map["content-type"].startswith("application/json")

    expected_auth = "Basic " + base64.b64encode(b"bot-user:super-secret-password").decode("ascii")
    assert header_map["authorization"] == expected_auth

    assert request.data == b'{"message":"Message payload"}'

    safe_result = result.to_safe_dict()
    assert safe_result == {
        "outcome": "posted",
        "source_id": "notice-1",
        "status_code": 201,
        "endpoint_path": "/ocs/v2.php/apps/spreed/api/v1/chat/abc***23",
        "ocs_status": "ok",
        "ocs_status_code": 100,
    }
    assert "super-secret-password" not in str(safe_result)


@pytest.mark.parametrize(
    ("status_code", "expected_failure", "expected_retryable"),
    [
        (403, TalkFailureClass.AUTH, False),
        (404, TalkFailureClass.NOT_FOUND, False),
        (412, TalkFailureClass.PRECONDITION, False),
        (413, TalkFailureClass.PAYLOAD_TOO_LARGE, False),
        (429, TalkFailureClass.RATE_LIMITED, True),
    ],
)
def test_post_message_maps_documented_failure_statuses(
    status_code: int,
    expected_failure: TalkFailureClass,
    expected_retryable: bool,
) -> None:
    config = _build_config()

    def sender(_: Request, __: float) -> _FakeResponse:
        return _FakeResponse(
            status=status_code,
            payload=_ocs_payload(status="failure", statuscode=status_code, message="failure"),
        )

    client = NextcloudTalkClient(config, sender=sender)

    with pytest.raises(TalkPostError) as exc_info:
        client.post_message(source_id="notice-42", message="Body")

    error = exc_info.value
    assert error.failure_class is expected_failure
    assert error.status_code == status_code
    assert error.retryable is expected_retryable
    assert error.source_id == "notice-42"

    safe = error.to_safe_dict()
    assert safe["failure_class"] == expected_failure.value
    assert safe["source_id"] == "notice-42"
    assert safe["status_code"] == status_code
    assert safe["endpoint_path"] == "/ocs/v2.php/apps/spreed/api/v1/chat/abc***23"
    assert "super-secret-password" not in str(safe)


def test_post_message_unknown_status_uses_fallback_classification() -> None:
    config = _build_config()

    def sender(_: Request, __: float) -> _FakeResponse:
        return _FakeResponse(
            status=500,
            payload=_ocs_payload(status="error", statuscode=500, message="server error"),
        )

    client = NextcloudTalkClient(config, sender=sender)

    with pytest.raises(TalkPostError) as exc_info:
        client.post_message(source_id="notice-9", message="Body")

    error = exc_info.value
    assert error.failure_class is TalkFailureClass.UNKNOWN_STATUS
    assert error.retryable is True
    assert error.status_code == 500


def test_post_message_timeout_raises_typed_retryable_error_with_source_context() -> None:
    config = _build_config()

    def sender(_: Request, __: float) -> _FakeResponse:
        raise TimeoutError("simulated timeout")

    client = NextcloudTalkClient(config, sender=sender)

    with pytest.raises(TalkPostError) as exc_info:
        client.post_message(source_id="notice-timeout", message="Body")

    error = exc_info.value
    assert error.failure_class is TalkFailureClass.TIMEOUT
    assert error.retryable is True
    assert error.source_id == "notice-timeout"
    assert error.status_code is None


@pytest.mark.parametrize(
    "payload",
    [
        b"not-json",
        b"[]",
        b"{}",
        json.dumps({"ocs": {}}).encode("utf-8"),
        json.dumps({"ocs": {"meta": {"status": "ok", "message": "x"}}}).encode("utf-8"),
        json.dumps({"ocs": {"meta": {"status": "", "statuscode": 100, "message": "x"}}}).encode(
            "utf-8"
        ),
    ],
)
def test_post_message_malformed_ocs_envelope_raises_protocol_error(payload: bytes) -> None:
    config = _build_config()

    def sender(_: Request, __: float) -> _FakeResponse:
        return _FakeResponse(status=201, payload=payload)

    client = NextcloudTalkClient(config, sender=sender)

    with pytest.raises(TalkPostError) as exc_info:
        client.post_message(source_id="notice-protocol", message="Body")

    error = exc_info.value
    assert error.failure_class is TalkFailureClass.PROTOCOL
    assert error.retryable is False
    assert error.source_id == "notice-protocol"


@pytest.mark.parametrize(
    ("kwargs", "message_fragment"),
    [
        ({"base_url": "   "}, "base_url"),
        ({"base_url": "ftp://cloud.example"}, "base_url"),
        ({"room_token": "   "}, "room_token"),
        ({"room_token": "room/token"}, "room_token"),
        ({"room_token": "../escape"}, "room_token"),
        ({"username": "   "}, "username"),
        ({"app_password": "\n"}, "app_password"),
    ],
)
def test_build_talk_client_config_rejects_blank_and_path_unsafe_values(
    kwargs: dict[str, object],
    message_fragment: str,
) -> None:
    base = {
        "base_url": "https://cloud.example",
        "room_token": "validToken123",
        "username": "bot-user",
        "app_password": "secret",
    }
    base.update(kwargs)

    with pytest.raises(TalkClientConfigError, match=message_fragment):
        build_talk_client_config(**base)


def test_safe_diagnostics_redact_room_token_and_never_include_password_or_authorization() -> None:
    config = _build_config()

    safe_config = config.to_safe_dict()
    assert safe_config["endpoint_path"] == "/ocs/v2.php/apps/spreed/api/v1/chat/abc***23"
    assert "super-secret-password" not in str(safe_config)
    assert "Authorization" not in str(safe_config)

    def sender(_: Request, __: float) -> _FakeResponse:
        return _FakeResponse(
            status=403,
            payload=_ocs_payload(status="failure", statuscode=403, message="forbidden"),
        )

    client = NextcloudTalkClient(config, sender=sender)

    with pytest.raises(TalkPostError) as exc_info:
        client.post_message(source_id="notice-safe", message="Private message body")

    safe_error = exc_info.value.to_safe_dict()
    serialized = str(safe_error)
    assert "super-secret-password" not in serialized
    assert "Authorization" not in serialized
    assert "Private message body" not in serialized
    assert safe_error["failure_class"] == "auth"
