"""
Tests for channel config parsing (BQ-VZ-CHANNEL, Condition C1/C5).

8 test cases: marketplace, aim-data, direct, unset, invalid, case-insensitive, whitespace, default fallback.
"""

import logging
import os
from unittest.mock import patch


from app.core.channel_config import ChannelType, parse_channel


def test_channel_marketplace():
    """VECTORAIZ_CHANNEL=marketplace → ChannelType.marketplace"""
    with patch.dict(os.environ, {"VECTORAIZ_CHANNEL": "marketplace"}):
        assert parse_channel() == ChannelType.marketplace


def test_channel_direct():
    """VECTORAIZ_CHANNEL=direct → ChannelType.direct"""
    with patch.dict(os.environ, {"VECTORAIZ_CHANNEL": "direct"}):
        assert parse_channel() == ChannelType.direct


def test_channel_aim_data():
    """VECTORAIZ_CHANNEL=aim-data → ChannelType.aim_data"""
    with patch.dict(os.environ, {"VECTORAIZ_CHANNEL": "aim-data"}):
        assert parse_channel() == ChannelType.aim_data


def test_channel_unset():
    """VECTORAIZ_CHANNEL unset → ChannelType.direct (default)"""
    with patch.dict(os.environ, {}, clear=True):
        # Ensure VECTORAIZ_CHANNEL is not set
        os.environ.pop("VECTORAIZ_CHANNEL", None)
        assert parse_channel() == ChannelType.direct


def test_channel_invalid(caplog):
    """VECTORAIZ_CHANNEL=foobar → ChannelType.direct + warning logged"""
    with patch.dict(os.environ, {"VECTORAIZ_CHANNEL": "foobar"}):
        with caplog.at_level(logging.WARNING):
            result = parse_channel()
        assert result == ChannelType.direct
        assert "Invalid VECTORAIZ_CHANNEL" in caplog.text


def test_channel_case_insensitive():
    """VECTORAIZ_CHANNEL=MARKETPLACE → ChannelType.marketplace (CH-C1)"""
    with patch.dict(os.environ, {"VECTORAIZ_CHANNEL": "MARKETPLACE"}):
        assert parse_channel() == ChannelType.marketplace


def test_channel_whitespace():
    """VECTORAIZ_CHANNEL=' marketplace ' → trimmed → marketplace"""
    with patch.dict(os.environ, {"VECTORAIZ_CHANNEL": " marketplace "}):
        assert parse_channel() == ChannelType.marketplace


def test_channel_aim_data_invalid_fallback(caplog):
    """Invalid aim-data-like value falls back to direct and logs valid values."""
    with patch.dict(os.environ, {"VECTORAIZ_CHANNEL": "aim_data"}):
        with caplog.at_level(logging.WARNING):
            result = parse_channel()
        assert result == ChannelType.direct
        assert "Valid values: 'marketplace', 'aim-data', 'direct'" in caplog.text
