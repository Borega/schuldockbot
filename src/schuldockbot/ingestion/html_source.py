"""HTML fallback parsing for Schuldock issue ingestion."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from html.parser import HTMLParser
from urllib.parse import urljoin

from .models import NoticeRecord, SourceMode
from .normalize import canonical_text, normalize_notice, parse_german_date

HTML_FALLBACK_PATH = "aktuelle-meldungen/"
HTML_FALLBACK_URL = "https://schuldock.hamburg/aktuelle-meldungen/"
_SECTION_SELECTOR = "section.section-issues"
_ITEM_SELECTOR = "li.pp-item"


class HtmlSourceError(RuntimeError):
    """Base HTML ingestion exception with safe diagnostic metadata."""

    def __init__(self, message: str, *, context: dict[str, str] | None = None) -> None:
        super().__init__(message)
        self.context = dict(context or {})

    def to_safe_dict(self) -> dict[str, str]:
        """Return log-safe diagnostics without embedding raw notice HTML."""

        diagnostic = {"message": str(self)}
        diagnostic.update(self.context)
        return diagnostic


class HtmlFetchError(HtmlSourceError):
    """Raised when obtaining the HTML payload fails."""


class HtmlSelectorError(HtmlSourceError):
    """Raised when required selectors are missing from the listing."""


class HtmlFieldError(HtmlSourceError):
    """Raised when an item is missing required fields or has malformed values."""


@dataclass(slots=True)
class _ParsedIssueItem:
    notice_type_parts: list[str] = field(default_factory=list)
    date_parts: list[str] = field(default_factory=list)
    title_parts: list[str] = field(default_factory=list)
    content_parts: list[str] = field(default_factory=list)
    source_link: str | None = None


class _IssuesListingParser(HTMLParser):
    """Small purpose-built parser for Schuldock listing markup."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._depth = 0
        self._section_depth: int | None = None
        self._item_depth: int | None = None
        self._current_item: _ParsedIssueItem | None = None
        self._context_stack: list[str | None] = []
        self._items: list[_ParsedIssueItem] = []
        self.section_found = False

    @property
    def items(self) -> list[_ParsedIssueItem]:
        return self._items

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._depth += 1
        attrs_map = {key: value for key, value in attrs if value is not None}
        class_tokens = _class_tokens(attrs_map.get("class"))

        if self._section_depth is None and tag == "section" and "section-issues" in class_tokens:
            self._section_depth = self._depth
            self.section_found = True

        if self._inside_section() and self._current_item is None and tag == "li" and "pp-item" in class_tokens:
            self._current_item = _ParsedIssueItem()
            self._item_depth = self._depth
            self._context_stack = []
            self._items.append(self._current_item)

        if self._current_item is None:
            return

        context = _resolve_context(class_tokens=class_tokens, current_context=self._current_context())
        self._context_stack.append(context)

        if tag == "a" and self._current_item.source_link is None:
            href = attrs_map.get("href")
            if href and canonical_text(href):
                self._current_item.source_link = href

    def handle_endtag(self, tag: str) -> None:
        if self._current_item is not None and self._context_stack:
            self._context_stack.pop()

        if self._section_depth is not None and tag == "section" and self._depth == self._section_depth:
            self._section_depth = None

        if self._item_depth is not None and tag == "li" and self._depth == self._item_depth:
            self._item_depth = None
            self._current_item = None
            self._context_stack = []

        self._depth -= 1

    def handle_data(self, data: str) -> None:
        if self._current_item is None:
            return

        context = self._current_context()
        if context == "type":
            self._current_item.notice_type_parts.append(data)
        elif context == "date":
            self._current_item.date_parts.append(data)
        elif context == "title":
            self._current_item.title_parts.append(data)
        elif context == "content":
            self._current_item.content_parts.append(data)

    def _inside_section(self) -> bool:
        return self._section_depth is not None

    def _current_context(self) -> str | None:
        if not self._context_stack:
            return None
        return self._context_stack[-1]


def _class_tokens(raw_class: str | None) -> set[str]:
    if raw_class is None:
        return set()
    return {token for token in canonical_text(raw_class).split(" ") if token}


def _resolve_context(*, class_tokens: set[str], current_context: str | None) -> str | None:
    if "pp-type" in class_tokens:
        return "type"
    if "pp-date" in class_tokens:
        return "date"
    if "pp-title" in class_tokens:
        return "title"
    if "pp-content" in class_tokens:
        return "content"
    return current_context


def _coerce_text_payload(payload: str | bytes | bytearray) -> str:
    if isinstance(payload, str):
        return payload
    if isinstance(payload, (bytes, bytearray)):
        return payload.decode("utf-8")
    raise TypeError(f"Unsupported HTML payload type: {type(payload)!r}")


def _require_text_field(
    value: str,
    *,
    field: str,
    selector: str,
    item_index: int,
) -> str:
    cleaned = canonical_text(value)
    if cleaned:
        return cleaned

    raise HtmlFieldError(
        f"Missing required field '{field}' from selector '{selector}'",
        context={
            "field": field,
            "selector": selector,
            "item_index": str(item_index),
        },
    )


def _resolve_source_link(*, item_link: str | None, base_url: str) -> str:
    cleaned_base = canonical_text(base_url)
    if not cleaned_base:
        raise ValueError("base_url is required")

    if item_link and canonical_text(item_link):
        return urljoin(cleaned_base, item_link)
    return cleaned_base


def _parse_listing_items(payload: str) -> list[_ParsedIssueItem]:
    if not canonical_text(payload):
        raise HtmlSelectorError(
            "HTML payload is empty",
            context={"selector": _SECTION_SELECTOR},
        )

    parser = _IssuesListingParser()
    parser.feed(payload)
    parser.close()

    if not parser.section_found:
        raise HtmlSelectorError(
            f"Missing required selector '{_SECTION_SELECTOR}'",
            context={"selector": _SECTION_SELECTOR},
        )

    if not parser.items:
        raise HtmlSelectorError(
            f"Missing required selector '{_ITEM_SELECTOR}' within '{_SECTION_SELECTOR}'",
            context={"selector": _ITEM_SELECTOR, "within": _SECTION_SELECTOR},
        )

    return parser.items


def parse_html_payload(
    payload: str | bytes | bytearray,
    *,
    base_url: str = HTML_FALLBACK_URL,
) -> list[NoticeRecord]:
    """Parse Schuldock fallback HTML into normalized notice records."""

    html = _coerce_text_payload(payload)
    items = _parse_listing_items(html)

    records: list[NoticeRecord] = []
    for item_index, item in enumerate(items):
        notice_type = _require_text_field(
            " ".join(item.notice_type_parts),
            field="type",
            selector=".pp-type",
            item_index=item_index,
        )
        date_text = _require_text_field(
            " ".join(item.date_parts),
            field="date",
            selector=".pp-date",
            item_index=item_index,
        )
        title = _require_text_field(
            " ".join(item.title_parts),
            field="title",
            selector=".pp-title",
            item_index=item_index,
        )
        content = _require_text_field(
            " ".join(item.content_parts),
            field="content",
            selector=".pp-content",
            item_index=item_index,
        )

        try:
            published_at = parse_german_date(date_text)
        except ValueError as exc:
            raise HtmlFieldError(
                "Failed to parse required field 'date' from selector '.pp-date'",
                context={
                    "field": "date",
                    "selector": ".pp-date",
                    "item_index": str(item_index),
                },
            ) from exc

        records.append(
            normalize_notice(
                source_mode=SourceMode.HTML_FALLBACK,
                source_id=None,
                notice_type=notice_type,
                title=title,
                content=content,
                source_link=_resolve_source_link(item_link=item.source_link, base_url=base_url),
                published_at=published_at,
                modified_at=None,
                content_is_html=False,
            )
        )

    return records


def fetch_and_parse_html(
    fetcher: Callable[[], str | bytes | bytearray],
    *,
    base_url: str = HTML_FALLBACK_URL,
) -> list[NoticeRecord]:
    """Fetch fallback HTML via dependency injection and parse normalized records."""

    try:
        payload = fetcher()
    except HtmlSourceError:
        raise
    except Exception as exc:  # pragma: no cover - guarded by tests via synthetic fetcher.
        raise HtmlFetchError(
            "Failed to fetch schuldock fallback HTML payload",
            context={"path": HTML_FALLBACK_PATH},
        ) from exc

    try:
        return parse_html_payload(payload, base_url=base_url)
    except HtmlSourceError:
        raise
    except Exception as exc:
        raise HtmlSourceError(
            "Failed to parse schuldock fallback HTML payload",
            context={"path": HTML_FALLBACK_PATH},
        ) from exc


__all__ = [
    "HTML_FALLBACK_PATH",
    "HTML_FALLBACK_URL",
    "HtmlSourceError",
    "HtmlFetchError",
    "HtmlSelectorError",
    "HtmlFieldError",
    "parse_html_payload",
    "fetch_and_parse_html",
]
