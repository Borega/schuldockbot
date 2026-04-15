from __future__ import annotations

from dataclasses import fields
from pathlib import Path

from schuldockbot.ingestion import SourceMode
from schuldockbot.ingestion.models import NoticeRecord
from schuldockbot.ingestion.source_selector import fetch_notices

TESTS_DIR = Path(__file__).resolve().parents[2]
FIXTURE_DIR = TESTS_DIR / "fixtures" / "schuldock"

EXPECTED_NOTICE_FIELDS = {
    "source_mode",
    "source_id",
    "type",
    "title",
    "content",
    "source_link",
    "published_at",
    "modified_at",
    "revision_token",
}


def _load_json_fixture_text() -> str:
    return (FIXTURE_DIR / "issues.json").read_text(encoding="utf-8")


def _load_html_fixture_text() -> str:
    return (FIXTURE_DIR / "aktuelle-meldungen.html").read_text(encoding="utf-8")


def _json_mode_result():
    def _html_fetcher_not_expected() -> str:
        raise AssertionError("HTML fallback should not be used when JSON succeeds")

    return fetch_notices(lambda: _load_json_fixture_text(), _html_fetcher_not_expected)


def _html_fallback_result():
    return fetch_notices(lambda: "{", lambda: _load_html_fixture_text())


def test_notice_record_contract_field_set_is_stable_across_source_modes() -> None:
    model_fields = {field.name for field in fields(NoticeRecord)}
    assert model_fields == EXPECTED_NOTICE_FIELDS

    json_result = _json_mode_result()
    html_result = _html_fallback_result()

    assert json_result.source_mode is SourceMode.JSON
    assert html_result.source_mode is SourceMode.HTML_FALLBACK

    for record in [*json_result.records, *html_result.records]:
        assert set(record.__dataclass_fields__.keys()) == EXPECTED_NOTICE_FIELDS
        assert len(record.revision_token) == 64


def test_revision_tokens_and_source_ids_are_deterministic_per_source_mode() -> None:
    json_first = _json_mode_result().records
    json_second = _json_mode_result().records

    assert [record.source_id for record in json_first] == [record.source_id for record in json_second]
    assert [record.revision_token for record in json_first] == [
        record.revision_token for record in json_second
    ]

    html_first = _html_fallback_result().records
    html_second = _html_fallback_result().records

    assert [record.source_id for record in html_first] == [record.source_id for record in html_second]
    assert [record.revision_token for record in html_first] == [
        record.revision_token for record in html_second
    ]
