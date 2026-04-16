from __future__ import annotations

from pathlib import Path

import pytest

from schuldockbot.ingestion import SourceMode
from schuldockbot.ingestion.html_source import (
    HTML_FALLBACK_URL,
    HtmlFieldError,
    HtmlFetchError,
    HtmlSelectorError,
    fetch_and_parse_html,
    parse_html_payload,
)

TESTS_DIR = Path(__file__).resolve().parents[2]
FIXTURE_DIR = TESTS_DIR / "fixtures" / "schuldock"


def _load_fixture_text() -> str:
    return (FIXTURE_DIR / "aktuelle-meldungen.html").read_text(encoding="utf-8")


def test_parse_html_payload_maps_fixture_to_normalized_records() -> None:
    records = parse_html_payload(_load_fixture_text())

    assert len(records) == 2

    first = records[0]
    assert first.source_mode is SourceMode.HTML_FALLBACK
    assert first.source_id.startswith("html_fallback:")
    assert first.type == "Schulbetrieb"
    assert first.title == "Warnstreik am Freitag – Unterricht teilweise ausgesetzt"
    assert (
        first.content
        == "Aufgrund eines Warnstreiks kann es zu Unterrichtsausfällen kommen. "
        "Bitte prüfen Sie regelmäßig den Vertretungsplan."
    )
    assert first.source_link == HTML_FALLBACK_URL
    assert first.published_at.isoformat() == "2026-04-14T22:00:00+00:00"
    assert first.modified_at == first.published_at
    assert len(first.revision_token) == 64

    second = records[1]
    assert second.type == "Gebäude"
    assert second.title == "Sperrung der Sporthalle bis Montag"
    assert second.content == "Die Sporthalle bleibt bis Montag aufgrund technischer Prüfungen geschlossen."


def test_parse_html_payload_accepts_current_live_markup_classes() -> None:
    payload = """
    <div class="section section-issues">
      <ul class="pp-listing">
        <li class="pp-item">
          <article>
            <div class="pp-entry-terms"><span class="pp-term"><strong>Fehler</strong></span></div>
            <span class="issue-date">15. April 2026 9:27 – anhaltend</span>
            <h3 class="pp-entry-title">Einzelne E-Mail-Postfächer in IServ Hamburg nicht erreichbar!</h3>
            <div class="pp-entry-content"><p>IServ arbeitet an einer Lösung des Problems.</p></div>
          </article>
        </li>
      </ul>
    </div>
    """

    records = parse_html_payload(payload)

    assert len(records) == 1
    assert records[0].type == "Fehler"
    assert records[0].title == "Einzelne E-Mail-Postfächer in IServ Hamburg nicht erreichbar!"
    assert records[0].content == "IServ arbeitet an einer Lösung des Problems."


def test_parse_html_payload_is_deterministic_without_explicit_source_ids() -> None:
    first_run = parse_html_payload(_load_fixture_text())
    second_run = parse_html_payload(_load_fixture_text())

    assert [record.source_id for record in first_run] == [record.source_id for record in second_run]
    assert [record.revision_token for record in first_run] == [
        record.revision_token for record in second_run
    ]


def test_parse_html_payload_reports_missing_section_selector() -> None:
    drifted_markup = _load_fixture_text().replace("section-issues", "section-issues-renamed", 1)

    with pytest.raises(HtmlSelectorError) as exc_info:
        parse_html_payload(drifted_markup)

    error = exc_info.value
    assert "section-issues" in str(error)
    assert error.to_safe_dict()["selector"] == ".section-issues"


def test_parse_html_payload_reports_missing_item_title_field() -> None:
    broken_markup = _load_fixture_text().replace("pp-title", "pp-title-renamed", 1)

    with pytest.raises(HtmlFieldError) as exc_info:
        parse_html_payload(broken_markup)

    error = exc_info.value
    safe = error.to_safe_dict()
    assert safe["field"] == "title"
    assert safe["selector"] == ".pp-title/.pp-entry-title"
    assert safe["item_index"] == "0"


def test_parse_html_payload_reports_malformed_date_field() -> None:
    broken_markup = _load_fixture_text().replace("15.04.2026 – anhaltend", "unklar", 1)

    with pytest.raises(HtmlFieldError) as exc_info:
        parse_html_payload(broken_markup)

    error = exc_info.value
    safe = error.to_safe_dict()
    assert safe["field"] == "date"
    assert safe["selector"] == ".pp-date/.issue-date"
    assert safe["item_index"] == "0"


def test_fetch_and_parse_html_classifies_fetch_failures() -> None:
    def _failing_fetcher() -> str:
        raise TimeoutError("socket timeout")

    with pytest.raises(HtmlFetchError) as exc_info:
        fetch_and_parse_html(_failing_fetcher)

    error = exc_info.value
    assert error.to_safe_dict()["path"] == "aktuelle-meldungen/"
