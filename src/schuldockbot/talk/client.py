"""Stdlib Nextcloud Talk OCS client with typed safe diagnostics."""

from __future__ import annotations

import base64
import json
import socket
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .models import (
    TalkClientConfig,
    TalkFailureClass,
    TalkOcsMeta,
    TalkPostError,
    TalkPostResult,
)


class _ResponseLike(Protocol):
    """Small response protocol used to support dependency-injected senders."""

    status: int

    def read(self) -> bytes: ...

    def close(self) -> None: ...


class TalkRequestSender(Protocol):
    """Injected request sender contract used by ``NextcloudTalkClient``."""

    def __call__(self, request: Request, timeout: float) -> _ResponseLike: ...


class NextcloudTalkClient:
    """HTTP boundary for posting deterministic chat messages to one Talk room."""

    def __init__(
        self,
        config: TalkClientConfig,
        *,
        sender: TalkRequestSender | None = None,
    ) -> None:
        self._config = config
        self._sender = _default_sender if sender is None else sender

    def post_message(self, *, source_id: str, message: str) -> TalkPostResult:
        """Post one message to the configured Talk room and classify outcome."""

        normalized_source_id = _require_text("source_id", source_id)
        normalized_message = _require_text("message", message)

        request = self._build_request(normalized_message)

        try:
            status_code, response_payload = self._send_request(
                request,
                source_id=normalized_source_id,
            )
        except TalkPostError:
            raise

        ocs_meta = _parse_ocs_meta(
            response_payload,
            source_id=normalized_source_id,
            endpoint_path=self._config.safe_endpoint_path,
            status_code=status_code,
        )

        if status_code == 201:
            return TalkPostResult(
                source_id=normalized_source_id,
                status_code=status_code,
                endpoint_path=self._config.safe_endpoint_path,
                ocs_status=ocs_meta.status,
                ocs_status_code=ocs_meta.statuscode,
            )

        failure_class, retryable = _map_status_code(status_code)
        raise TalkPostError(
            failure_class=failure_class,
            source_id=normalized_source_id,
            endpoint_path=self._config.safe_endpoint_path,
            detail=f"Talk post failed with HTTP {status_code}",
            status_code=status_code,
            retryable=retryable,
            ocs_status=ocs_meta.status,
            ocs_status_code=ocs_meta.statuscode,
        )

    def _build_request(self, message: str) -> Request:
        payload = json.dumps({"message": message}, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        auth_header = _build_basic_auth_header(self._config.basic_auth_userpass)
        headers = {
            "OCS-APIRequest": "true",
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": auth_header,
        }
        return Request(
            url=self._config.endpoint_url,
            data=payload,
            headers=headers,
            method="POST",
        )

    def _send_request(self, request: Request, *, source_id: str) -> tuple[int, bytes]:
        try:
            response = self._sender(request, self._config.timeout_seconds)
            try:
                status_code = int(getattr(response, "status"))
                payload = response.read()
                return status_code, payload
            finally:
                response.close()
        except HTTPError as exc:
            try:
                payload = exc.read()
            finally:
                exc.close()
            return int(exc.code), payload
        except (socket.timeout, TimeoutError) as exc:
            raise TalkPostError(
                failure_class=TalkFailureClass.TIMEOUT,
                source_id=source_id,
                endpoint_path=self._config.safe_endpoint_path,
                detail="Talk post timed out",
                retryable=True,
            ) from exc
        except URLError as exc:
            if isinstance(exc.reason, (TimeoutError, socket.timeout)):
                raise TalkPostError(
                    failure_class=TalkFailureClass.TIMEOUT,
                    source_id=source_id,
                    endpoint_path=self._config.safe_endpoint_path,
                    detail="Talk post timed out",
                    retryable=True,
                ) from exc

            raise TalkPostError(
                failure_class=TalkFailureClass.TRANSPORT,
                source_id=source_id,
                endpoint_path=self._config.safe_endpoint_path,
                detail="Talk post transport failure",
                retryable=True,
            ) from exc


class _UrlopenResponseAdapter:
    """Adapter to make urllib responses satisfy the minimal protocol."""

    def __init__(self, response: _ResponseLike) -> None:
        self._response = response

    @property
    def status(self) -> int:
        return int(self._response.status)

    def read(self) -> bytes:
        return self._response.read()

    def close(self) -> None:
        self._response.close()


def _default_sender(request: Request, timeout: float) -> _ResponseLike:
    response = urlopen(request, timeout=timeout)
    return _UrlopenResponseAdapter(response)


def _build_basic_auth_header(userpass: str) -> str:
    encoded = base64.b64encode(userpass.encode("utf-8")).decode("ascii")
    return f"Basic {encoded}"


def _parse_ocs_meta(
    payload: bytes,
    *,
    source_id: str,
    endpoint_path: str,
    status_code: int,
) -> TalkOcsMeta:
    try:
        decoded = payload.decode("utf-8")
        parsed = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TalkPostError(
            failure_class=TalkFailureClass.PROTOCOL,
            source_id=source_id,
            endpoint_path=endpoint_path,
            detail="Talk response is not valid JSON OCS envelope",
            status_code=status_code,
            retryable=False,
        ) from exc

    if not isinstance(parsed, dict):
        raise TalkPostError(
            failure_class=TalkFailureClass.PROTOCOL,
            source_id=source_id,
            endpoint_path=endpoint_path,
            detail="Talk response root must be an object",
            status_code=status_code,
            retryable=False,
        )

    ocs = parsed.get("ocs")
    if not isinstance(ocs, dict):
        raise TalkPostError(
            failure_class=TalkFailureClass.PROTOCOL,
            source_id=source_id,
            endpoint_path=endpoint_path,
            detail="Talk response missing ocs object",
            status_code=status_code,
            retryable=False,
        )

    meta = ocs.get("meta")
    if not isinstance(meta, dict):
        raise TalkPostError(
            failure_class=TalkFailureClass.PROTOCOL,
            source_id=source_id,
            endpoint_path=endpoint_path,
            detail="Talk response missing ocs.meta object",
            status_code=status_code,
            retryable=False,
        )

    status = meta.get("status")
    if not isinstance(status, str) or not status.strip():
        raise TalkPostError(
            failure_class=TalkFailureClass.PROTOCOL,
            source_id=source_id,
            endpoint_path=endpoint_path,
            detail="Talk response meta.status must be a non-empty string",
            status_code=status_code,
            retryable=False,
        )

    statuscode = meta.get("statuscode")
    if not isinstance(statuscode, int):
        raise TalkPostError(
            failure_class=TalkFailureClass.PROTOCOL,
            source_id=source_id,
            endpoint_path=endpoint_path,
            detail="Talk response meta.statuscode must be an integer",
            status_code=status_code,
            retryable=False,
        )

    message = meta.get("message", "")
    if not isinstance(message, str):
        raise TalkPostError(
            failure_class=TalkFailureClass.PROTOCOL,
            source_id=source_id,
            endpoint_path=endpoint_path,
            detail="Talk response meta.message must be a string when present",
            status_code=status_code,
            retryable=False,
        )

    return TalkOcsMeta(status=status.strip(), statuscode=statuscode, message=message)


def _map_status_code(status_code: int) -> tuple[TalkFailureClass, bool]:
    if status_code == 403:
        return TalkFailureClass.AUTH, False
    if status_code == 404:
        return TalkFailureClass.NOT_FOUND, False
    if status_code == 412:
        return TalkFailureClass.PRECONDITION, False
    if status_code == 413:
        return TalkFailureClass.PAYLOAD_TOO_LARGE, False
    if status_code == 429:
        return TalkFailureClass.RATE_LIMITED, True
    return TalkFailureClass.UNKNOWN_STATUS, status_code >= 500


def _require_text(field_name: str, value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")

    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized
