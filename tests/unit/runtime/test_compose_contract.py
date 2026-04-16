from __future__ import annotations

from pathlib import Path
import re

from schuldockbot.runtime.config import (
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
)

COMPOSE_PATH = Path("compose.yaml")
ENV_EXAMPLE_PATH = Path(".env.example")


def _read_required(path: Path) -> str:
    assert path.exists(), f"{path} must exist"
    return path.read_text(encoding="utf-8")


def _parse_env_file(path: Path) -> dict[str, str]:
    entries: dict[str, str] = {}

    for raw_line in _read_required(path).splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        key, separator, value = line.partition("=")
        assert separator, f"Invalid env entry (missing '='): {raw_line!r}"

        normalized_key = key.strip()
        normalized_value = value.strip()
        assert normalized_key, f"Invalid env entry (missing key): {raw_line!r}"
        assert normalized_value, f"Invalid env entry (missing value): {raw_line!r}"

        entries[normalized_key] = normalized_value

    return entries


def test_compose_runs_long_running_runtime_by_default_without_smoke_flags() -> None:
    compose = _read_required(COMPOSE_PATH)

    assert re.search(
        r'entrypoint:\s*\[\s*"python"\s*,\s*"-m"\s*,\s*"schuldockbot\.runtime\.main"\s*\]',
        compose,
    )

    assert "--once" not in compose
    assert "--dry-run" not in compose


def test_compose_declares_env_file_and_named_state_volume_wiring() -> None:
    compose = _read_required(COMPOSE_PATH)

    assert re.search(r"env_file:\s*\n\s*-\s*\.env\.example", compose)
    assert "schuldockbot_state:/var/lib/schuldockbot" in compose
    assert re.search(r"(?m)^volumes:\s*$", compose)
    assert re.search(r"(?m)^\s{2}schuldockbot_state:\s*$", compose)


def test_compose_healthcheck_is_python_native_and_bounded() -> None:
    compose = _read_required(COMPOSE_PATH)

    assert "healthcheck:" in compose
    assert re.search(r"healthcheck:[\s\S]*\bpython\b", compose)
    assert re.search(r"healthcheck:[\s\S]*\binterval:\s*\d+s", compose)
    assert re.search(r"healthcheck:[\s\S]*\btimeout:\s*\d+s", compose)
    assert re.search(r"healthcheck:[\s\S]*\bretries:\s*\d+", compose)

    assert not re.search(r"healthcheck:[\s\S]*\bcurl\b", compose)
    assert not re.search(r"healthcheck:[\s\S]*\bwget\b", compose)


def test_env_example_covers_required_and_optional_runtime_keys() -> None:
    values = _parse_env_file(ENV_EXAMPLE_PATH)

    required_keys = {
        ENV_SOURCE_JSON_URL,
        ENV_SOURCE_HTML_URL,
        ENV_STATE_DB_PATH,
        ENV_TALK_BASE_URL,
        ENV_TALK_ROOM_TOKEN,
        ENV_TALK_USERNAME,
        ENV_TALK_APP_PASSWORD,
    }
    optional_default_keys = {
        ENV_POLL_INTERVAL_SECONDS,
        ENV_RETRY_MAX_ATTEMPTS,
        ENV_RETRY_BACKOFF_SECONDS,
        ENV_SQLITE_TIMEOUT_SECONDS,
        ENV_TALK_TIMEOUT_SECONDS,
    }

    missing_required = sorted(required_keys - set(values))
    missing_optional_defaults = sorted(optional_default_keys - set(values))

    assert not missing_required, (
        "Missing required runtime env keys in .env.example: "
        + ", ".join(missing_required)
    )
    assert not missing_optional_defaults, (
        "Missing optional default env keys in .env.example: "
        + ", ".join(missing_optional_defaults)
    )


def test_env_example_uses_safe_token_placeholder_and_state_path_for_volume_mount() -> None:
    values = _parse_env_file(ENV_EXAMPLE_PATH)

    room_token = values[ENV_TALK_ROOM_TOKEN]
    assert re.fullmatch(r"[A-Za-z0-9._~-]+", room_token), (
        "Room token placeholder must be URL-safe and contain no path separators"
    )

    state_path = values[ENV_STATE_DB_PATH]
    assert state_path.startswith("/var/lib/schuldockbot/"), (
        "State DB placeholder must point into the compose-mounted persistent path"
    )
