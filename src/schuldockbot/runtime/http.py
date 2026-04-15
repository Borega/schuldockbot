"""HTTP fetch helpers for runtime polling with injectable transport senders."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Protocol
from urllib.request import Request, urlopen

DEFAULT_JSON_ACCEPT = "application/json"
DEFAULT_HTML_ACCEPT = "text/html,application/xhtml+xml"


class _ResponseLike(Protocol):
    """Minimal response contract used by stdlib and injected senders."""

    def read(self) -> bytes: ...

    def close(self) -> None: ...


class HttpRequestSender(Protocol):
    """Dependency-injected request sender for deterministic unit tests."""

    def __call__(self, request: Request, timeout: float) -> _ResponseLike: ...


def make_http_fetcher(
    url: str,
    *,
    timeout_seconds: float,
    accept: str,
    sender: HttpRequestSender | None = None,
    headers: Mapping[str, str] | None = None,
) -> Callable[[], bytes]:
    """Build a zero-arg fetcher closure compatible with ingestion selectors."""

    normalized_headers = {"Accept": accept}
    if headers is not None:
        normalized_headers.update(dict(headers))

    def _fetch() -> bytes:
        return fetch_http_bytes(
            url,
            timeout_seconds=timeout_seconds,
            sender=sender,
            headers=normalized_headers,
        )

    return _fetch


def build_schuldock_fetchers(
    *,
    json_url: str,
    html_url: str,
    timeout_seconds: float,
    sender: HttpRequestSender | None = None,
) -> tuple[Callable[[], bytes], Callable[[], bytes]]:
    """Build JSON-primary + HTML-fallback fetchers for one runtime cycle."""

    return (
        make_http_fetcher(
            json_url,
            timeout_seconds=timeout_seconds,
            accept=DEFAULT_JSON_ACCEPT,
            sender=sender,
        ),
        make_http_fetcher(
            html_url,
            timeout_seconds=timeout_seconds,
            accept=DEFAULT_HTML_ACCEPT,
            sender=sender,
        ),
    )


def fetch_http_bytes(
    url: str,
    *,
    timeout_seconds: float,
    sender: HttpRequestSender | None = None,
    headers: Mapping[str, str] | None = None,
) -> bytes:
    """Issue a stdlib GET request and return raw response bytes."""

    request_headers = dict(headers or {})
    request = Request(url=url, headers=request_headers, method="GET")

    response_sender = _default_sender if sender is None else sender
    response = response_sender(request, timeout_seconds)
    try:
        return response.read()
    finally:
        response.close()


def _default_sender(request: Request, timeout: float) -> _ResponseLike:
    return urlopen(request, timeout=timeout)


__all__ = [
    "DEFAULT_HTML_ACCEPT",
    "DEFAULT_JSON_ACCEPT",
    "HttpRequestSender",
    "build_schuldock_fetchers",
    "fetch_http_bytes",
    "make_http_fetcher",
]
