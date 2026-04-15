"""Talk delivery/rendering boundary for Schuldockbot."""

from .client import NextcloudTalkClient
from .delivery import (
    ProcessedNoticeStoreLike,
    TalkClientLike,
    TalkDeliveryAckError,
    TalkDeliveryFailure,
    TalkDeliveryInputError,
    TalkDeliveryInvariantError,
    TalkDeliverySummary,
    deliver_notice_changes,
)
from .formatter import (
    DEFAULT_MAX_MESSAGE_LENGTH,
    TalkFormatterInputError,
    render_notice_change,
)
from .models import (
    TalkClientConfig,
    TalkClientConfigError,
    TalkFailureClass,
    TalkOcsMeta,
    TalkPostError,
    TalkPostResult,
    build_talk_client_config,
)

__all__ = [
    "DEFAULT_MAX_MESSAGE_LENGTH",
    "NextcloudTalkClient",
    "ProcessedNoticeStoreLike",
    "TalkClientConfig",
    "TalkClientConfigError",
    "TalkClientLike",
    "TalkDeliveryAckError",
    "TalkDeliveryFailure",
    "TalkDeliveryInputError",
    "TalkDeliveryInvariantError",
    "TalkDeliverySummary",
    "TalkFailureClass",
    "TalkFormatterInputError",
    "TalkOcsMeta",
    "TalkPostError",
    "TalkPostResult",
    "build_talk_client_config",
    "deliver_notice_changes",
    "render_notice_change",
]
