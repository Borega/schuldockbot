"""Ingestion package for Schuldock source data."""

from .models import NoticeRecord, SourceMode
from .normalize import normalize_notice

__all__ = [
    "NoticeRecord",
    "SourceMode",
    "normalize_notice",
]
