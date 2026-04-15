from __future__ import annotations

import json
from pathlib import Path

from schuldockbot.ingestion import SourceMode, normalize_notice
from schuldockbot.ingestion.normalize import parse_german_date

TESTS_DIR = Path(__file__).resolve().parents[2]
FIXTURE_DIR = TESTS_DIR / "fixtures" / "schuldock"


def _load_issue_fixture() -> dict:
    issues = json.loads((FIXTURE_DIR / "issues.json").read_text(encoding="utf-8"))
    return issues[0]


def test_normalize_notice_preserves_explicit_json_source_id_and_normalizes_content() -> None:
    issue = _load_issue_fixture()

    record = normalize_notice(
        source_mode=SourceMode.JSON,
        source_id=issue["id"],
        notice_type=issue["details"]["issue_type"],
        title=issue["title"]["rendered"],
        content=issue["content"]["rendered"],
        source_link=issue["link"],
        published_at=issue["date"],
        modified_at=issue["modified"],
        content_is_html=True,
    )

    assert record.source_mode is SourceMode.JSON
    assert record.source_id == str(issue["id"])
    assert "<" not in record.content
    assert record.revision_token
    assert len(record.revision_token) == 64


def test_normalize_notice_fallback_id_and_revision_token_are_deterministic() -> None:
    issue = _load_issue_fixture()

    kwargs = {
        "source_mode": SourceMode.HTML_FALLBACK,
        "source_id": None,
        "notice_type": issue["details"]["issue_type"],
        "title": issue["title"]["rendered"],
        "content": issue["content"]["rendered"],
        "source_link": "https://schuldock.hamburg/aktuelle-meldungen/",
        "published_at": issue["date"],
        "modified_at": None,
        "content_is_html": True,
    }

    first = normalize_notice(**kwargs)
    second = normalize_notice(**kwargs)

    assert first.source_id == second.source_id
    assert first.revision_token == second.revision_token
    assert first.source_id.startswith("html_fallback:")


def test_normalize_notice_uses_published_timestamp_when_modified_missing() -> None:
    issue = _load_issue_fixture()

    record = normalize_notice(
        source_mode=SourceMode.HTML_FALLBACK,
        source_id=None,
        notice_type=issue["details"]["issue_type"],
        title=issue["title"]["rendered"],
        content=issue["content"]["rendered"],
        source_link="https://schuldock.hamburg/aktuelle-meldungen/",
        published_at=issue["date"],
        modified_at=None,
        content_is_html=True,
    )

    assert record.modified_at == record.published_at


def test_parse_german_date_accepts_suffixes_from_html_listing_fixture() -> None:
    html_fixture = (FIXTURE_DIR / "aktuelle-meldungen.html").read_text(encoding="utf-8")
    assert "15.04.2026 – anhaltend" in html_fixture

    parsed = parse_german_date("15.04.2026 – anhaltend")

    assert parsed.isoformat() == "2026-04-14T22:00:00+00:00"
