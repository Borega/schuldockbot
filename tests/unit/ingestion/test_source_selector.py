from __future__ import annotations

import json
from pathlib import Path

import pytest

from schuldockbot.ingestion import SourceMode
from schuldockbot.ingestion.json_source import JsonFailureClass
from schuldockbot.ingestion.source_selector import SourceSelectionError, fetch_notices

TESTS_DIR = Path(__file__).resolve().parents[2]
FIXTURE_DIR = TESTS_DIR / "fixtures" / "schuldock"


def _load_json_fixture_text() -> str:
    return (FIXTURE_DIR / "issues.json").read_text(encoding="utf-8")


def _load_html_fixture_text() -> str:
    return (FIXTURE_DIR / "aktuelle-meldungen.html").read_text(encoding="utf-8")


def test_fetch_notices_prefers_json_source_when_primary_path_succeeds() -> None:
    def _html_fetcher_not_expected() -> str:
        raise AssertionError("HTML fallback should not be called when JSON succeeds")

    result = fetch_notices(lambda: _load_json_fixture_text(), _html_fetcher_not_expected)

    assert result.source_mode is SourceMode.JSON
    assert result.fallback_reason is None
    assert result.record_count == 2

    diagnostics = result.to_safe_dict()
    assert diagnostics["source_mode"] == "json"
    assert diagnostics["fallback_reason"] is None
    assert diagnostics["record_count"] == 2
    assert diagnostics["json_failure_class"] is None


def test_fetch_notices_fallback_on_json_decode_failure_returns_html_records() -> None:
    secret_marker = "SECRET_TOKEN_VALUE"
    invalid_json = "{" + secret_marker

    result = fetch_notices(lambda: invalid_json, lambda: _load_html_fixture_text())

    assert result.source_mode is SourceMode.HTML_FALLBACK
    assert result.fallback_reason == JsonFailureClass.DECODE.value
    assert result.record_count == 2
    assert all(record.source_mode is SourceMode.HTML_FALLBACK for record in result.records)

    diagnostics = result.to_safe_dict()
    assert diagnostics["source_mode"] == "html_fallback"
    assert diagnostics["fallback_reason"] == "decode"
    assert diagnostics["json_failure_class"] == "decode"
    assert diagnostics["record_count"] == 2
    assert diagnostics["json_failure"]["failure_class"] == "decode"
    assert secret_marker not in json.dumps(diagnostics)


def test_fetch_notices_raises_dual_source_error_when_json_and_fallback_fail() -> None:
    with pytest.raises(SourceSelectionError) as exc_info:
        fetch_notices(
            lambda: "{",
            lambda: "<html><body><main><p>broken</p></main></body></html>",
        )

    error = exc_info.value
    diagnostics = error.to_safe_dict()
    assert diagnostics["source_mode"] == "html_fallback"
    assert diagnostics["fallback_reason"] == "decode"
    assert diagnostics["json_failure"]["failure_class"] == "decode"
    assert diagnostics["html_failure"]["selector"] == "section.section-issues"
