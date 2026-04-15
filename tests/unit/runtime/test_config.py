from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
import shutil
import tempfile

import pytest

from schuldockbot.runtime import (
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_RETRY_BACKOFF_SECONDS,
    DEFAULT_RETRY_MAX_ATTEMPTS,
    DEFAULT_SQLITE_TIMEOUT_SECONDS,
    DEFAULT_TALK_TIMEOUT_SECONDS,
    ENV_POLL_INTERVAL_SECONDS,
    ENV_RETRY_BACKOFF_SECONDS,
    ENV_RETRY_MAX_ATTEMPTS,
    ENV_SOURCE_HTML_URL,
    ENV_SOURCE_JSON_URL,
    ENV_SQLITE_TIMEOUT_SECONDS,
    ENV_STATE_DB_PATH,
    ENV_TALK_APP_PASSWORD,
    ENV_TALK_BASE_URL,
    ENV_TALK_ROOM_TOKEN,
    ENV_TALK_TIMEOUT_SECONDS,
    ENV_TALK_USERNAME,
    RuntimeStartupConfigError,
    load_runtime_config,
)


@pytest.fixture
def workspace_tmp_path() -> Iterator[Path]:
    root = Path(".pytest-tmp")
    root.mkdir(parents=True, exist_ok=True)

    path = Path(tempfile.mkdtemp(prefix="runtime-config-", dir=root))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _base_env(tmp_path: Path) -> dict[str, str]:
    return {
        ENV_SOURCE_JSON_URL: "https://schuldock.hamburg/schuldock/v1/issues",
        ENV_SOURCE_HTML_URL: "https://schuldock.hamburg/aktuelle-meldungen/",
        ENV_STATE_DB_PATH: str(tmp_path / "state" / "processed.db"),
        ENV_TALK_BASE_URL: "https://cloud.example/",
        ENV_TALK_ROOM_TOKEN: "roomToken123",
        ENV_TALK_USERNAME: "bot-user",
        ENV_TALK_APP_PASSWORD: "secret-app-password",
    }


def test_load_runtime_config_parses_defaults_and_normalizes_paths(
    workspace_tmp_path: Path,
) -> None:
    env = _base_env(workspace_tmp_path)

    config = load_runtime_config(env)

    assert config.source.json_url == "https://schuldock.hamburg/schuldock/v1/issues"
    assert config.source.html_url == "https://schuldock.hamburg/aktuelle-meldungen"
    assert config.poll_interval_seconds == DEFAULT_POLL_INTERVAL_SECONDS
    assert config.retry.max_attempts == DEFAULT_RETRY_MAX_ATTEMPTS
    assert config.retry.backoff_seconds == DEFAULT_RETRY_BACKOFF_SECONDS
    assert config.state.sqlite_timeout_seconds == DEFAULT_SQLITE_TIMEOUT_SECONDS
    assert config.talk.timeout_seconds == DEFAULT_TALK_TIMEOUT_SECONDS
    assert Path(config.state.sqlite_db_path).is_absolute()
    assert (workspace_tmp_path / "state").exists()


def test_load_runtime_config_preserves_explicit_numeric_overrides(
    workspace_tmp_path: Path,
) -> None:
    env = _base_env(workspace_tmp_path)
    env[ENV_POLL_INTERVAL_SECONDS] = "900"
    env[ENV_RETRY_MAX_ATTEMPTS] = "5"
    env[ENV_RETRY_BACKOFF_SECONDS] = "45.5"
    env[ENV_SQLITE_TIMEOUT_SECONDS] = "9"
    env[ENV_TALK_TIMEOUT_SECONDS] = "12.5"

    config = load_runtime_config(env)

    assert config.poll_interval_seconds == 900
    assert config.retry.max_attempts == 5
    assert config.retry.backoff_seconds == 45.5
    assert config.state.sqlite_timeout_seconds == 9.0
    assert config.talk.timeout_seconds == 12.5


@pytest.mark.parametrize(
    "missing_key",
    [
        ENV_SOURCE_JSON_URL,
        ENV_SOURCE_HTML_URL,
        ENV_STATE_DB_PATH,
        ENV_TALK_BASE_URL,
        ENV_TALK_ROOM_TOKEN,
        ENV_TALK_USERNAME,
        ENV_TALK_APP_PASSWORD,
    ],
)
def test_load_runtime_config_missing_required_keys_fail_fast_with_actionable_key(
    workspace_tmp_path: Path,
    missing_key: str,
) -> None:
    env = _base_env(workspace_tmp_path)
    env.pop(missing_key)

    with pytest.raises(RuntimeStartupConfigError) as exc_info:
        load_runtime_config(env)

    error = exc_info.value
    assert error.phase == "config"
    assert error.category == "missing_env"
    assert error.key == missing_key

    safe = error.to_safe_dict()
    assert safe["phase"] == "config"
    assert safe["category"] == "missing_env"
    assert safe["key"] == missing_key


@pytest.mark.parametrize(
    ("key", "value", "expected_detail"),
    [
        (ENV_POLL_INTERVAL_SECONDS, "abc", "must be an integer"),
        (ENV_POLL_INTERVAL_SECONDS, "0", "must be between"),
        (ENV_RETRY_MAX_ATTEMPTS, "zero", "must be an integer"),
        (ENV_RETRY_MAX_ATTEMPTS, "0", "must be between"),
        (ENV_RETRY_BACKOFF_SECONDS, "none", "must be a number"),
        (ENV_RETRY_BACKOFF_SECONDS, "0", "must be between"),
        (ENV_SQLITE_TIMEOUT_SECONDS, "invalid", "must be a number"),
        (ENV_SQLITE_TIMEOUT_SECONDS, "0", "must be between"),
        (ENV_TALK_TIMEOUT_SECONDS, "oops", "must be a number"),
        (ENV_TALK_TIMEOUT_SECONDS, "-1", "must be between"),
    ],
)
def test_load_runtime_config_rejects_invalid_numeric_values(
    workspace_tmp_path: Path,
    key: str,
    value: str,
    expected_detail: str,
) -> None:
    env = _base_env(workspace_tmp_path)
    env[key] = value

    with pytest.raises(RuntimeStartupConfigError) as exc_info:
        load_runtime_config(env)

    error = exc_info.value
    assert error.phase == "config"
    assert error.category == "invalid_env"
    assert error.key == key
    assert expected_detail in error.detail


def test_load_runtime_config_wraps_talk_config_failures_with_safe_keyed_diagnostics(
    workspace_tmp_path: Path,
) -> None:
    env = _base_env(workspace_tmp_path)
    env[ENV_TALK_ROOM_TOKEN] = "room/token"

    with pytest.raises(RuntimeStartupConfigError) as exc_info:
        load_runtime_config(env)

    error = exc_info.value
    assert error.phase == "config"
    assert error.category == "talk_config"
    assert error.key == ENV_TALK_ROOM_TOKEN

    safe = error.to_safe_dict()
    serialized = str(safe)
    assert "room/token" not in serialized
    assert env[ENV_TALK_APP_PASSWORD] not in serialized
    assert "Authorization" not in serialized


def test_load_runtime_config_rejects_invalid_source_urls_and_blank_db_path(
    workspace_tmp_path: Path,
) -> None:
    env = _base_env(workspace_tmp_path)
    env[ENV_SOURCE_JSON_URL] = "schuldock/v1/issues"

    with pytest.raises(RuntimeStartupConfigError, match=ENV_SOURCE_JSON_URL):
        load_runtime_config(env)

    env = _base_env(workspace_tmp_path)
    env[ENV_SOURCE_HTML_URL] = "https://schuldock.hamburg/"

    with pytest.raises(RuntimeStartupConfigError, match=ENV_SOURCE_HTML_URL):
        load_runtime_config(env)

    env = _base_env(workspace_tmp_path)
    env[ENV_STATE_DB_PATH] = "   "

    with pytest.raises(RuntimeStartupConfigError, match=ENV_STATE_DB_PATH):
        load_runtime_config(env)


def test_load_runtime_config_surfaces_db_path_parent_creation_failure(
    workspace_tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = _base_env(workspace_tmp_path)

    def _raise_os_error(self: Path, parents: bool = False, exist_ok: bool = False) -> None:
        raise OSError("permission denied")

    monkeypatch.setattr(Path, "mkdir", _raise_os_error)

    with pytest.raises(RuntimeStartupConfigError) as exc_info:
        load_runtime_config(env)

    error = exc_info.value
    assert error.phase == "config"
    assert error.category == "invalid_env"
    assert error.key == ENV_STATE_DB_PATH
    assert "parent path cannot be created" in error.detail
