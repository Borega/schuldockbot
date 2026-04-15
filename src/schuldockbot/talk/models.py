"""Typed models and diagnostics for Nextcloud Talk client interactions."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import re
from urllib.parse import quote, urlsplit, urlunsplit


_ROOM_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9._~-]+$")


class TalkClientConfigError(ValueError):
    """Raised when Talk client configuration values are missing or unsafe."""


class TalkFailureClass(StrEnum):
    """Stable failure classes for Talk post outcomes."""

    AUTH = "auth"
    NOT_FOUND = "not_found"
    PRECONDITION = "precondition"
    PAYLOAD_TOO_LARGE = "payload_too_large"
    RATE_LIMITED = "rate_limited"
    TIMEOUT = "timeout"
    PROTOCOL = "protocol"
    UNKNOWN_STATUS = "unknown_status"
    TRANSPORT = "transport"


@dataclass(frozen=True, slots=True)
class TalkOcsMeta:
    """Minimal validated OCS metadata parsed from a Talk response."""

    status: str
    statuscode: int
    message: str

    def to_safe_dict(self) -> dict[str, object]:
        """Return diagnostics-safe OCS metadata."""

        return {
            "status": self.status,
            "statuscode": self.statuscode,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class TalkClientConfig:
    """Validated Nextcloud Talk OCS chat configuration."""

    base_url: str
    room_token: str
    username: str
    app_password: str
    timeout_seconds: float = 10.0

    @property
    def endpoint_path(self) -> str:
        """Return the canonical chat endpoint path with encoded room token."""

        token = quote(self.room_token, safe="")
        return f"/ocs/v2.php/apps/spreed/api/v1/chat/{token}"

    @property
    def endpoint_url(self) -> str:
        """Return the full chat endpoint URL."""

        return f"{self.base_url}{self.endpoint_path}"

    @property
    def basic_auth_userpass(self) -> str:
        """Return the raw user:password pair for Basic auth generation."""

        return f"{self.username}:{self.app_password}"

    @property
    def safe_endpoint_path(self) -> str:
        """Return endpoint path with the room token redacted."""

        return (
            "/ocs/v2.php/apps/spreed/api/v1/chat/"
            f"{_redact_secret(self.room_token, prefix=3, suffix=2)}"
        )

    def to_safe_dict(self) -> dict[str, object]:
        """Return diagnostics-safe config information."""

        return {
            "base_url": self.base_url,
            "endpoint_path": self.safe_endpoint_path,
            "username": self.username,
            "timeout_seconds": self.timeout_seconds,
        }


@dataclass(frozen=True, slots=True)
class TalkPostResult:
    """Typed success result for one Talk message post."""

    source_id: str
    status_code: int
    endpoint_path: str
    ocs_status: str
    ocs_status_code: int

    def to_safe_dict(self) -> dict[str, object]:
        """Return diagnostics-safe post success metadata."""

        return {
            "outcome": "posted",
            "source_id": self.source_id,
            "status_code": self.status_code,
            "endpoint_path": self.endpoint_path,
            "ocs_status": self.ocs_status,
            "ocs_status_code": self.ocs_status_code,
        }


@dataclass(slots=True)
class TalkPostError(RuntimeError):
    """Typed post error with safe context for retries/alerts."""

    failure_class: TalkFailureClass
    source_id: str
    endpoint_path: str
    detail: str
    status_code: int | None = None
    retryable: bool = False
    ocs_status: str | None = None
    ocs_status_code: int | None = None

    def __post_init__(self) -> None:
        RuntimeError.__init__(self, self.detail)

    def to_safe_dict(self) -> dict[str, object]:
        """Return diagnostics-safe post failure metadata."""

        return {
            "failure_class": self.failure_class.value,
            "source_id": self.source_id,
            "status_code": self.status_code,
            "retryable": self.retryable,
            "endpoint_path": self.endpoint_path,
            "ocs_status": self.ocs_status,
            "ocs_status_code": self.ocs_status_code,
            "detail": self.detail,
        }


def build_talk_client_config(
    *,
    base_url: object,
    room_token: object,
    username: object,
    app_password: object,
    timeout_seconds: object = 10.0,
) -> TalkClientConfig:
    """Validate raw values and build a typed ``TalkClientConfig`` instance."""

    normalized_base_url = _normalize_base_url(base_url)
    normalized_room_token = _validate_room_token(room_token)
    normalized_username = _validate_required_text("username", username)
    normalized_app_password = _validate_required_text("app_password", app_password)
    normalized_timeout = _validate_timeout_seconds(timeout_seconds)

    return TalkClientConfig(
        base_url=normalized_base_url,
        room_token=normalized_room_token,
        username=normalized_username,
        app_password=normalized_app_password,
        timeout_seconds=normalized_timeout,
    )


def _normalize_base_url(value: object) -> str:
    if not isinstance(value, str):
        raise TalkClientConfigError("base_url must be a non-empty http(s) URL")

    candidate = value.strip()
    if not candidate:
        raise TalkClientConfigError("base_url must be a non-empty http(s) URL")

    split = urlsplit(candidate)
    if split.scheme.lower() not in {"http", "https"} or not split.netloc:
        raise TalkClientConfigError("base_url must be a non-empty http(s) URL")

    if split.query or split.fragment:
        raise TalkClientConfigError("base_url must not include query or fragment")

    normalized_path = split.path.rstrip("/")
    return urlunsplit((split.scheme.lower(), split.netloc, normalized_path, "", ""))


def _validate_room_token(value: object) -> str:
    token = _validate_required_text("room_token", value)
    if not _ROOM_TOKEN_PATTERN.fullmatch(token):
        raise TalkClientConfigError(
            "room_token must contain only URL-safe token characters and no path separators"
        )
    return token


def _validate_timeout_seconds(value: object) -> float:
    if isinstance(value, bool):
        raise TalkClientConfigError("timeout_seconds must be a positive number")

    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise TalkClientConfigError("timeout_seconds must be a positive number") from exc

    if parsed <= 0:
        raise TalkClientConfigError("timeout_seconds must be a positive number")
    return parsed


def _validate_required_text(field_name: str, value: object) -> str:
    if not isinstance(value, str):
        raise TalkClientConfigError(f"{field_name} must be a non-empty string")

    normalized = value.strip()
    if not normalized:
        raise TalkClientConfigError(f"{field_name} must be a non-empty string")
    return normalized


def _redact_secret(value: str, *, prefix: int, suffix: int) -> str:
    if len(value) <= prefix + suffix:
        return "***"
    return f"{value[:prefix]}***{value[-suffix:]}"
