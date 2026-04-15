"""Talk delivery/rendering boundary for Schuldockbot."""

from .client import NextcloudTalkClient
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
    "TalkClientConfig",
    "TalkClientConfigError",
    "TalkFailureClass",
    "TalkFormatterInputError",
    "TalkOcsMeta",
    "TalkPostError",
    "TalkPostResult",
    "build_talk_client_config",
    "render_notice_change",
]
