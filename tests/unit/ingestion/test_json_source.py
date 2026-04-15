from __future__ import annotations

import json
from pathlib import Path

import pytest

from schuldockbot.ingestion import SourceMode
from schuldockbot.ingestion.json_source import (
    JsonDecodeError,
    JsonFailureClass,
    JsonFetchError,
    JsonSchemaError,
    decode_issues_payload,
    fetch_and_parse_issues,
    parse_issues_payload,
)

TESTS_DIR = Path(__file__).resolve().parents[2]
FIXTURE_DIR = TESTS_DIR / "fixtures" / "schuldock"


def _load_fixture_text() -> str:
    return (FIXTURE_DIR / "issues.json").read_text(encoding="utf-8")


def _load_fixture_payload() -> list[dict[str, object]]:
    return json.loads(_load_fixture_text())


def test_decode_issues_payload_maps_fixture_to_normalized_records() -> None:
    records = decode_issues_payload(_load_fixture_text())

    assert len(records) == 2
    first = records[0]

    assert first.source_mode is SourceMode.JSON
    assert first.source_id == "84572"
    assert first.type == "Schulbetrieb"
    assert first.title == "Warnstreik am Freitag – Unterricht teilweise ausgesetzt"
    assert (
        first.content
        == "Aufgrund eines Warnstreiks kann es am Freitag zu Unterrichtsausfällen kommen. "
        "Bitte prüfen Sie regelmäßig den Vertretungsplan."
    )
    assert first.source_link == "https://schuldock.hamburg/issues/84572/"
    assert first.published_at.isoformat() == "2026-04-15T05:30:00+00:00"
    assert first.modified_at.isoformat() == "2026-04-15T08:05:00+00:00"
    assert len(first.revision_token) == 64


def test_decode_issues_payload_classifies_invalid_json_as_decode_error() -> None:
    with pytest.raises(JsonDecodeError) as exc_info:
        decode_issues_payload("{" )

    error = exc_info.value
    assert error.failure_class is JsonFailureClass.DECODE
    assert error.to_safe_dict()["failure_class"] == "decode"


def test_parse_issues_payload_classifies_missing_required_key_as_schema_error() -> None:
    payload = _load_fixture_payload()
    payload[0]["details"] = {}

    with pytest.raises(JsonSchemaError) as exc_info:
        parse_issues_payload(payload)

    error = exc_info.value
    assert error.failure_class is JsonFailureClass.SCHEMA
    assert "issue.details.issue_type" in str(error)
    assert error.to_safe_dict()["path"] == "issue.details.issue_type"


def test_fetch_and_parse_issues_classifies_fetch_failures() -> None:
    def _failing_fetcher() -> str:
        raise TimeoutError("socket timeout")

    with pytest.raises(JsonFetchError) as exc_info:
        fetch_and_parse_issues(_failing_fetcher)

    error = exc_info.value
    assert error.failure_class is JsonFailureClass.FETCH
    assert error.to_safe_dict()["failure_class"] == "fetch"
