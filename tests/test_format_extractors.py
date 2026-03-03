"""
Tests for format_extractors.py — the bridge between NativeFormatProcessor
and the streaming TextBlock format.
"""

import tempfile
from pathlib import Path

import pytest

from app.services.format_extractors import can_extract, extract_text_blocks, EXTRACTABLE_TYPES
from app.services.streaming_processor import TextBlock


# ---------------------------------------------------------------------------
# can_extract()
# ---------------------------------------------------------------------------

def test_can_extract_supported_types():
    """All types in EXTRACTABLE_TYPES should return True."""
    for ft in EXTRACTABLE_TYPES:
        assert can_extract(ft), f"Expected can_extract('{ft}') to be True"


def test_can_extract_unsupported_types():
    """Types NOT in EXTRACTABLE_TYPES should return False."""
    for ft in ("pdf", "docx", "csv", "pages", "wps", "unknown"):
        assert not can_extract(ft), f"Expected can_extract('{ft}') to be False"


# ---------------------------------------------------------------------------
# extract_text_blocks() — RTF
# ---------------------------------------------------------------------------

def test_extract_rtf_returns_textblocks(tmp_path):
    """RTF extraction should return a list of TextBlock objects."""
    p = tmp_path / "hello.rtf"
    p.write_text(r"{\rtf1\ansi Hello from RTF}")
    blocks = extract_text_blocks(p, "rtf")

    assert isinstance(blocks, list)
    assert len(blocks) >= 1
    assert all(isinstance(b, TextBlock) for b in blocks)
    assert any("Hello from RTF" in b.text for b in blocks)


# ---------------------------------------------------------------------------
# extract_text_blocks() — EML
# ---------------------------------------------------------------------------

def test_extract_eml_returns_textblocks(tmp_path):
    """EML extraction should return TextBlocks with subject and body."""
    p = tmp_path / "test.eml"
    p.write_text(
        "From: alice@example.com\r\n"
        "To: bob@example.com\r\n"
        "Subject: Test Email\r\n"
        "Content-Type: text/plain\r\n"
        "\r\n"
        "Hello, this is the body of the email."
    )
    blocks = extract_text_blocks(p, "eml")

    assert isinstance(blocks, list)
    assert len(blocks) >= 1
    assert all(isinstance(b, TextBlock) for b in blocks)
    # Should contain subject and/or body text
    full_text = " ".join(b.text for b in blocks)
    assert "Test Email" in full_text or "body of the email" in full_text


# ---------------------------------------------------------------------------
# extract_text_blocks() — ICS
# ---------------------------------------------------------------------------

def test_extract_ics_returns_textblocks(tmp_path):
    """ICS extraction should return TextBlocks with event data."""
    p = tmp_path / "events.ics"
    p.write_text(
        "BEGIN:VCALENDAR\r\n"
        "VERSION:2.0\r\n"
        "BEGIN:VEVENT\r\n"
        "SUMMARY:Team Meeting\r\n"
        "DESCRIPTION:Weekly sync\r\n"
        "END:VEVENT\r\n"
        "END:VCALENDAR\r\n"
    )
    blocks = extract_text_blocks(p, "ics")

    assert isinstance(blocks, list)
    assert len(blocks) >= 1
    assert all(isinstance(b, TextBlock) for b in blocks)
    full_text = " ".join(b.text for b in blocks)
    assert "Team Meeting" in full_text


# ---------------------------------------------------------------------------
# extract_text_blocks() — empty file yields at least one block
# ---------------------------------------------------------------------------

def test_extract_empty_xml_returns_block(tmp_path):
    """Even empty/minimal XML should return at least one TextBlock."""
    p = tmp_path / "empty.xml"
    p.write_text("<root></root>")
    blocks = extract_text_blocks(p, "xml")

    assert isinstance(blocks, list)
    assert len(blocks) >= 1
    assert all(isinstance(b, TextBlock) for b in blocks)


# ---------------------------------------------------------------------------
# extract_text_blocks() — VCF
# ---------------------------------------------------------------------------

def test_extract_vcf_returns_textblocks(tmp_path):
    """VCF extraction should return TextBlocks with contact data."""
    p = tmp_path / "contact.vcf"
    p.write_text(
        "BEGIN:VCARD\r\n"
        "VERSION:3.0\r\n"
        "FN:John Doe\r\n"
        "ORG:Acme Corp\r\n"
        "END:VCARD\r\n"
    )
    blocks = extract_text_blocks(p, "vcf")

    assert isinstance(blocks, list)
    assert len(blocks) >= 1
    full_text = " ".join(b.text for b in blocks)
    assert "John Doe" in full_text


# ---------------------------------------------------------------------------
# PROCESSABLE_TYPES consistency
# ---------------------------------------------------------------------------

def test_processable_types_includes_extractable():
    """All EXTRACTABLE_TYPES should be in PROCESSABLE_TYPES."""
    from app.services.processing_service import PROCESSABLE_TYPES
    for ft in EXTRACTABLE_TYPES:
        assert ft in PROCESSABLE_TYPES, (
            f"'{ft}' is in EXTRACTABLE_TYPES but not in PROCESSABLE_TYPES"
        )


def test_processable_types_excludes_unsupported():
    """Removed types (pages, numbers, key, wps, wpd) should NOT be processable."""
    from app.services.processing_service import PROCESSABLE_TYPES
    for ft in ("pages", "numbers", "key", "wps", "wpd"):
        assert ft not in PROCESSABLE_TYPES, (
            f"'{ft}' should not be in PROCESSABLE_TYPES (no handler)"
        )
