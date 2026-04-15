"""State persistence and change-detection boundary for Schuldockbot."""

from .change_detector import detect_notice_changes
from .models import (
    ChangeKind,
    InvalidProcessedNoticeError,
    NoticeChange,
    ProcessedNoticeState,
    StateStoreError,
    StateStoreInitializationError,
    StateStoreLockTimeoutError,
    StateStorePersistenceError,
    StateStoreSchemaError,
    build_processed_notice_state,
)
from .sqlite_store import SqliteProcessedNoticeStore

__all__ = [
    "ChangeKind",
    "detect_notice_changes",
    "InvalidProcessedNoticeError",
    "NoticeChange",
    "ProcessedNoticeState",
    "SqliteProcessedNoticeStore",
    "StateStoreError",
    "StateStoreInitializationError",
    "StateStoreLockTimeoutError",
    "StateStorePersistenceError",
    "StateStoreSchemaError",
    "build_processed_notice_state",
]
