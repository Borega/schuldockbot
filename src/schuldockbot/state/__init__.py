"""State persistence and change-detection boundary for Schuldockbot."""

from .models import (
    InvalidProcessedNoticeError,
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
    "InvalidProcessedNoticeError",
    "ProcessedNoticeState",
    "SqliteProcessedNoticeStore",
    "StateStoreError",
    "StateStoreInitializationError",
    "StateStoreLockTimeoutError",
    "StateStorePersistenceError",
    "StateStoreSchemaError",
    "build_processed_notice_state",
]
