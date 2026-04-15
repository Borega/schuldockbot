"""Runtime configuration parsing and startup-validation diagnostics."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit, urlunsplit

from schuldockbot.talk import TalkClientConfig, TalkClientConfigError, build_talk_client_config

ENV_SOURCE_JSON_URL = "SCHULDOCKBOT_SOURCE_JSON_URL"
ENV_SOURCE_HTML_URL = "SCHULDOCKBOT_SOURCE_HTML_URL"
ENV_POLL_INTERVAL_SECONDS = "SCHULDOCKBOT_POLL_INTERVAL_SECONDS"
ENV_RETRY_MAX_ATTEMPTS = "SCHULDOCKBOT_RETRY_MAX_ATTEMPTS"
ENV_RETRY_BACKOFF_SECONDS = "SCHULDOCKBOT_RETRY_BACKOFF_SECONDS"
ENV_STATE_DB_PATH = "SCHULDOCKBOT_STATE_DB_PATH"
ENV_SQLITE_TIMEOUT_SECONDS = "SCHULDOCKBOT_SQLITE_TIMEOUT_SECONDS"
ENV_TALK_BASE_URL = "SCHULDOCKBOT_TALK_BASE_URL"
ENV_TALK_ROOM_TOKEN = "SCHULDOCKBOT_TALK_ROOM_TOKEN"
ENV_TALK_USERNAME = "SCHULDOCKBOT_TALK_USERNAME"
ENV_TALK_APP_PASSWORD = "SCHULDOCKBOT_TALK_APP_PASSWORD"
ENV_TALK_TIMEOUT_SECONDS = "SCHULDOCKBOT_TALK_TIMEOUT_SECONDS"

DEFAULT_POLL_INTERVAL_SECONDS = 600
DEFAULT_RETRY_MAX_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 30.0
DEFAULT_SQLITE_TIMEOUT_SECONDS = 5.0
DEFAULT_TALK_TIMEOUT_SECONDS = 10.0

_MIN_POLL_INTERVAL_SECONDS = 10
_MAX_POLL_INTERVAL_SECONDS = 86_400
_MIN_RETRY_ATTEMPTS = 1
_MAX_RETRY_ATTEMPTS = 20
_MIN_BACKOFF_SECONDS = 1.0
_MAX_BACKOFF_SECONDS = 3_600.0
_MIN_TIMEOUT_SECONDS = 0.1
_MAX_TIMEOUT_SECONDS = 120.0

_TALK_FIELD_TO_ENV_KEY = {
    "base_url": ENV_TALK_BASE_URL,
    "room_token": ENV_TALK_ROOM_TOKEN,
    "username": ENV_TALK_USERNAME,
    "app_password": ENV_TALK_APP_PASSWORD,
    "timeout_seconds": ENV_TALK_TIMEOUT_SECONDS,
}


@dataclass(slots=True)
class RuntimeStartupConfigError(RuntimeError):
    """Raised when runtime startup config is missing or invalid."""

    key: str | None
    category: str
    detail: str
    phase: str = "config"

    def __post_init__(self) -> None:
        RuntimeError.__init__(self, self.detail)

    def to_safe_dict(self) -> dict[str, object]:
        """Return diagnostics-safe startup error metadata."""

        return {
            "phase": self.phase,
            "error_class": self.__class__.__name__,
            "category": self.category,
            "key": self.key,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class RuntimeSourceConfig:
    """Validated source endpoint settings for one runtime process."""

    json_url: str
    html_url: str

    def to_safe_dict(self) -> dict[str, str]:
        """Return diagnostics-safe source settings."""

        return {
            "json_url": self.json_url,
            "html_url": self.html_url,
        }


@dataclass(frozen=True, slots=True)
class RuntimeRetryPolicy:
    """Validated bounded retry settings for runtime cycle execution."""

    max_attempts: int
    backoff_seconds: float

    def to_safe_dict(self) -> dict[str, object]:
        """Return diagnostics-safe retry policy settings."""

        return {
            "max_attempts": self.max_attempts,
            "backoff_seconds": self.backoff_seconds,
        }


@dataclass(frozen=True, slots=True)
class RuntimeStateConfig:
    """Validated persistence settings for runtime state storage."""

    sqlite_db_path: str
    sqlite_timeout_seconds: float

    def to_safe_dict(self) -> dict[str, object]:
        """Return diagnostics-safe state storage settings."""

        return {
            "sqlite_db_path": self.sqlite_db_path,
            "sqlite_timeout_seconds": self.sqlite_timeout_seconds,
        }


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    """Validated runtime configuration used to bootstrap service wiring."""

    source: RuntimeSourceConfig
    poll_interval_seconds: int
    retry: RuntimeRetryPolicy
    state: RuntimeStateConfig
    talk: TalkClientConfig

    def to_safe_dict(self) -> dict[str, object]:
        """Return diagnostics-safe startup config snapshot."""

        return {
            "source": self.source.to_safe_dict(),
            "poll_interval_seconds": self.poll_interval_seconds,
            "retry": self.retry.to_safe_dict(),
            "state": self.state.to_safe_dict(),
            "talk": self.talk.to_safe_dict(),
        }


def load_runtime_config(env: Mapping[str, str] | None = None) -> RuntimeConfig:
    """Parse runtime settings from environment values and fail fast when invalid."""

    environ = os.environ if env is None else env

    source = RuntimeSourceConfig(
        json_url=_read_required_http_url(environ, ENV_SOURCE_JSON_URL),
        html_url=_read_required_http_url(environ, ENV_SOURCE_HTML_URL),
    )

    poll_interval_seconds = _read_positive_int(
        environ,
        ENV_POLL_INTERVAL_SECONDS,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        min_value=_MIN_POLL_INTERVAL_SECONDS,
        max_value=_MAX_POLL_INTERVAL_SECONDS,
    )
    retry = RuntimeRetryPolicy(
        max_attempts=_read_positive_int(
            environ,
            ENV_RETRY_MAX_ATTEMPTS,
            default=DEFAULT_RETRY_MAX_ATTEMPTS,
            min_value=_MIN_RETRY_ATTEMPTS,
            max_value=_MAX_RETRY_ATTEMPTS,
        ),
        backoff_seconds=_read_positive_float(
            environ,
            ENV_RETRY_BACKOFF_SECONDS,
            default=DEFAULT_RETRY_BACKOFF_SECONDS,
            min_value=_MIN_BACKOFF_SECONDS,
            max_value=_MAX_BACKOFF_SECONDS,
        ),
    )

    state = RuntimeStateConfig(
        sqlite_db_path=_normalize_sqlite_db_path(_read_required_text(environ, ENV_STATE_DB_PATH)),
        sqlite_timeout_seconds=_read_positive_float(
            environ,
            ENV_SQLITE_TIMEOUT_SECONDS,
            default=DEFAULT_SQLITE_TIMEOUT_SECONDS,
            min_value=_MIN_TIMEOUT_SECONDS,
            max_value=_MAX_TIMEOUT_SECONDS,
        ),
    )

    talk_timeout = _read_positive_float(
        environ,
        ENV_TALK_TIMEOUT_SECONDS,
        default=DEFAULT_TALK_TIMEOUT_SECONDS,
        min_value=_MIN_TIMEOUT_SECONDS,
        max_value=_MAX_TIMEOUT_SECONDS,
    )

    try:
        talk = build_talk_client_config(
            base_url=_read_required_text(environ, ENV_TALK_BASE_URL),
            room_token=_read_required_text(environ, ENV_TALK_ROOM_TOKEN),
            username=_read_required_text(environ, ENV_TALK_USERNAME),
            app_password=_read_required_text(environ, ENV_TALK_APP_PASSWORD),
            timeout_seconds=talk_timeout,
        )
    except TalkClientConfigError as exc:
        raise RuntimeStartupConfigError(
            key=_infer_talk_env_key(str(exc)),
            category="talk_config",
            detail=f"Talk configuration validation failed: {exc}",
        ) from exc

    return RuntimeConfig(
        source=source,
        poll_interval_seconds=poll_interval_seconds,
        retry=retry,
        state=state,
        talk=talk,
    )


def _read_required_text(env: Mapping[str, str], key: str) -> str:
    raw_value = env.get(key)
    if raw_value is None:
        raise RuntimeStartupConfigError(
            key=key,
            category="missing_env",
            detail=f"Missing required environment variable: {key}",
        )

    normalized = raw_value.strip()
    if not normalized:
        raise RuntimeStartupConfigError(
            key=key,
            category="invalid_env",
            detail=f"Environment variable {key} must be a non-empty string",
        )

    return normalized


def _read_required_http_url(env: Mapping[str, str], key: str) -> str:
    raw = _read_required_text(env, key)

    split = urlsplit(raw)
    if split.scheme.lower() not in {"http", "https"} or not split.netloc:
        raise RuntimeStartupConfigError(
            key=key,
            category="invalid_env",
            detail=f"Environment variable {key} must be a valid http(s) URL",
        )

    if split.query or split.fragment:
        raise RuntimeStartupConfigError(
            key=key,
            category="invalid_env",
            detail=(
                f"Environment variable {key} must not include query parameters or fragments"
            ),
        )

    if not split.path or split.path == "/":
        raise RuntimeStartupConfigError(
            key=key,
            category="invalid_env",
            detail=f"Environment variable {key} must include a non-root URL path",
        )

    return urlunsplit((split.scheme.lower(), split.netloc, split.path.rstrip("/"), "", ""))


def _read_positive_int(
    env: Mapping[str, str],
    key: str,
    *,
    default: int,
    min_value: int,
    max_value: int,
) -> int:
    raw_value = env.get(key)
    if raw_value is None:
        return default

    normalized = raw_value.strip()
    if not normalized:
        raise RuntimeStartupConfigError(
            key=key,
            category="invalid_env",
            detail=f"Environment variable {key} must be an integer",
        )

    try:
        parsed = int(normalized)
    except ValueError as exc:
        raise RuntimeStartupConfigError(
            key=key,
            category="invalid_env",
            detail=f"Environment variable {key} must be an integer",
        ) from exc

    if parsed < min_value or parsed > max_value:
        raise RuntimeStartupConfigError(
            key=key,
            category="invalid_env",
            detail=f"Environment variable {key} must be between {min_value} and {max_value}",
        )

    return parsed


def _read_positive_float(
    env: Mapping[str, str],
    key: str,
    *,
    default: float,
    min_value: float,
    max_value: float,
) -> float:
    raw_value = env.get(key)
    if raw_value is None:
        return default

    normalized = raw_value.strip()
    if not normalized:
        raise RuntimeStartupConfigError(
            key=key,
            category="invalid_env",
            detail=f"Environment variable {key} must be a number",
        )

    try:
        parsed = float(normalized)
    except ValueError as exc:
        raise RuntimeStartupConfigError(
            key=key,
            category="invalid_env",
            detail=f"Environment variable {key} must be a number",
        ) from exc

    if parsed < min_value or parsed > max_value:
        raise RuntimeStartupConfigError(
            key=key,
            category="invalid_env",
            detail=(
                f"Environment variable {key} must be between {min_value:g} and {max_value:g}"
            ),
        )

    return parsed


def _normalize_sqlite_db_path(raw_path: str) -> str:
    if raw_path == ":memory:":
        return raw_path

    candidate = Path(raw_path).expanduser()
    try:
        resolved = candidate.resolve(strict=False)
    except (OSError, RuntimeError, ValueError) as exc:
        raise RuntimeStartupConfigError(
            key=ENV_STATE_DB_PATH,
            category="invalid_env",
            detail=f"Environment variable {ENV_STATE_DB_PATH} could not be resolved",
        ) from exc

    if not resolved.name:
        raise RuntimeStartupConfigError(
            key=ENV_STATE_DB_PATH,
            category="invalid_env",
            detail=(
                f"Environment variable {ENV_STATE_DB_PATH} must point to a SQLite file path"
            ),
        )

    try:
        resolved.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise RuntimeStartupConfigError(
            key=ENV_STATE_DB_PATH,
            category="invalid_env",
            detail=(
                f"Environment variable {ENV_STATE_DB_PATH} parent path cannot be created"
            ),
        ) from exc

    return str(resolved)


def _infer_talk_env_key(message: str) -> str | None:
    for field_name, env_key in _TALK_FIELD_TO_ENV_KEY.items():
        if field_name in message:
            return env_key
    return None


__all__ = [
    "DEFAULT_POLL_INTERVAL_SECONDS",
    "DEFAULT_RETRY_BACKOFF_SECONDS",
    "DEFAULT_RETRY_MAX_ATTEMPTS",
    "DEFAULT_SQLITE_TIMEOUT_SECONDS",
    "DEFAULT_TALK_TIMEOUT_SECONDS",
    "ENV_POLL_INTERVAL_SECONDS",
    "ENV_RETRY_BACKOFF_SECONDS",
    "ENV_RETRY_MAX_ATTEMPTS",
    "ENV_SOURCE_HTML_URL",
    "ENV_SOURCE_JSON_URL",
    "ENV_SQLITE_TIMEOUT_SECONDS",
    "ENV_STATE_DB_PATH",
    "ENV_TALK_APP_PASSWORD",
    "ENV_TALK_BASE_URL",
    "ENV_TALK_ROOM_TOKEN",
    "ENV_TALK_TIMEOUT_SECONDS",
    "ENV_TALK_USERNAME",
    "RuntimeConfig",
    "RuntimeRetryPolicy",
    "RuntimeSourceConfig",
    "RuntimeStartupConfigError",
    "RuntimeStateConfig",
    "load_runtime_config",
]
