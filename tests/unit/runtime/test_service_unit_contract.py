from __future__ import annotations

from configparser import ConfigParser
from pathlib import Path

UNIT_PATH = Path("deploy/systemd/schuldockbot.service")


def _load_unit() -> ConfigParser:
    parser = ConfigParser(interpolation=None, strict=False)
    parser.optionxform = str
    parser.read_string(UNIT_PATH.read_text(encoding="utf-8"))
    return parser


def test_service_unit_file_exists_with_expected_name() -> None:
    assert UNIT_PATH.exists(), "deploy/systemd/schuldockbot.service must exist"
    assert UNIT_PATH.name == "schuldockbot.service"


def test_service_unit_contains_required_sections() -> None:
    unit = _load_unit()
    assert unit.has_section("Unit")
    assert unit.has_section("Service")
    assert unit.has_section("Install")


def test_service_unit_core_directives_are_present() -> None:
    unit = _load_unit()

    assert "schuldock" in unit.get("Unit", "Description").lower()
    assert unit.get("Service", "WorkingDirectory") == "/opt/schuldockbot"
    assert unit.get("Service", "ExecStart") == "/usr/bin/env python3 -m schuldockbot.runtime.main"
    assert unit.get("Service", "Restart") == "on-failure"
    assert unit.get("Service", "RestartSec") == "15s"
    assert unit.get("Service", "EnvironmentFile") == "-/etc/schuldockbot/schuldockbot.env"


def test_service_unit_logs_to_journald_and_installs_for_multi_user() -> None:
    unit = _load_unit()

    assert unit.get("Service", "StandardOutput") == "journal"
    assert unit.get("Service", "StandardError") == "journal"
    assert unit.get("Install", "WantedBy") == "multi-user.target"


def test_service_unit_includes_minimum_hardening_directives() -> None:
    unit = _load_unit()

    assert unit.get("Service", "NoNewPrivileges") == "true"
    assert unit.get("Service", "PrivateTmp") == "true"
    assert unit.get("Service", "ProtectSystem") == "full"
    assert unit.get("Service", "ProtectHome") == "true"
    assert unit.get("Service", "UMask") == "0077"
