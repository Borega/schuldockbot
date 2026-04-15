"""Ingestion package for Schuldock source data."""

from .models import NoticeRecord, SourceMode
from .normalize import normalize_notice
from .source_selector import SourceSelectionError, SourceSelectionResult, fetch_notices

__all__ = [
    "NoticeRecord",
    "SourceMode",
    "SourceSelectionError",
    "SourceSelectionResult",
    "fetch_notices",
    "normalize_notice",
]
