"""
Channel Config — Presentation-only channel hint (BQ-VZ-CHANNEL)
===============================================================

Parses VECTORAIZ_CHANNEL env var once at startup.
Channel is PRESENTATION ONLY — it affects sidebar ordering,
allAI greeting, and default landing page. It NEVER gates features,
auth, billing, or access control (Condition C2).
"""

from enum import Enum
import os
import logging

logger = logging.getLogger(__name__)


class ChannelType(str, Enum):
    direct = "direct"
    marketplace = "marketplace"
    aim_data = "aim-data"


def parse_channel() -> ChannelType:
    """
    Parse VECTORAIZ_CHANNEL env var exactly once at startup.

    Rules (Condition C1):
    - "marketplace" → ChannelType.marketplace
    - "aim-data" → ChannelType.aim_data
    - "direct" → ChannelType.direct
    - Unset → ChannelType.direct (default)
    - Any other value → ChannelType.direct (log warning)
    """
    raw = os.environ.get("VECTORAIZ_CHANNEL", "").strip().lower()

    if raw == "marketplace":
        return ChannelType.marketplace
    elif raw == "aim-data":
        return ChannelType.aim_data
    elif raw == "direct" or raw == "":
        return ChannelType.direct
    else:
        logger.warning(
            "Invalid VECTORAIZ_CHANNEL='%s', defaulting to 'direct'. "
            "Valid values: 'marketplace', 'aim-data', 'direct'",
            raw,
        )
        return ChannelType.direct


# Parsed once at module import time
CHANNEL: ChannelType = parse_channel()
