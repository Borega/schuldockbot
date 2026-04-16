from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile

SCRIPT_PATH = Path("scripts/verify_published_image.py")


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "verify_published_image",
        SCRIPT_PATH,
    )
    assert spec is not None and spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_verify_published_image_script_exists() -> None:
    assert SCRIPT_PATH.exists(), (
        "scripts/verify_published_image.py must exist for publish proof verification"
    )


def test_verify_published_image_help_exposes_required_cli_options() -> None:
    completed = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert "--image" in completed.stdout
    assert "--env-file" in completed.stdout
    assert "--skip-pull" in completed.stdout
    assert "ghcr.io/borega/schuldockbot:latest" in completed.stdout


def test_validate_image_reference_rejects_missing_tag_and_whitespace() -> None:
    module = _load_module()

    assert module._validate_image_reference("ghcr.io/borega/schuldockbot") is not None
    assert module._validate_image_reference("ghcr.io/borega bad:latest") is not None
    assert module._validate_image_reference("ghcr.io/borega/schuldockbot:latest") is None


def test_env_parser_reports_invalid_entries_and_blank_required_keys() -> None:
    module = _load_module()

    with tempfile.TemporaryDirectory(dir=".") as temp_dir:
        env_path = Path(temp_dir) / "contract.env"
        env_path.write_text(
            "\n".join(
                [
                    "BROKEN_LINE_WITHOUT_EQUALS",
                    "SCHULDOCKBOT_SOURCE_JSON_URL=https://schuldock.hamburg/wp-json/schuldock/v1/issues",
                    "SCHULDOCKBOT_SOURCE_HTML_URL=https://schuldock.hamburg/aktuelle-meldungen",
                    "SCHULDOCKBOT_STATE_DB_PATH=/var/lib/schuldockbot/state.db",
                    "SCHULDOCKBOT_TALK_BASE_URL=https://nextcloud.example",
                    "SCHULDOCKBOT_TALK_ROOM_TOKEN=room-token",
                    "SCHULDOCKBOT_TALK_USERNAME=bot-user",
                    "SCHULDOCKBOT_TALK_APP_PASSWORD=",
                ]
            ),
            encoding="utf-8",
        )

        entries, issues = module._collect_env_keys_and_issues(env_path)

    assert issues == ["line_1:missing_equals"]
    assert entries["SCHULDOCKBOT_TALK_APP_PASSWORD"] == ""


def test_extract_lifecycle_events_finds_startup_and_shutdown_json_lines() -> None:
    module = _load_module()

    stdout = "\n".join(
        [
            "{\"event\": \"startup\", \"mode\": \"once\"}",
            "{\"event\": \"cycle_started\"}",
            "{\"event\": \"shutdown\", \"exit_code\": 0}",
        ]
    )

    events = module._extract_lifecycle_events(stdout_text=stdout, stderr_text="")

    assert "startup" in events
    assert "shutdown" in events
