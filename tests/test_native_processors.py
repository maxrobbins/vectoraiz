"""
Tests for BQ-VZ-PERF Phase 3: NativeFormatProcessor.

Each test creates a minimal synthetic file and verifies the processor returns
the standard document result dict with correct structure.
"""

import textwrap
from pathlib import Path
from zipfile import ZipFile

import pytest

from app.services.native_document_processors import NativeFormatProcessor


@pytest.fixture
def processor():
    return NativeFormatProcessor()


def _assert_result_shape(result: dict, filepath: Path):
    """Verify the standard output schema."""
    assert "text_content" in result
    assert "tables" in result
    assert "metadata" in result

    meta = result["metadata"]
    assert meta["filename"] == filepath.name
    assert meta["file_type"] == filepath.suffix.lstrip(".")
    assert meta["processor"] == "native"
    assert meta["element_count"] == len(result["text_content"])
    assert meta["text_blocks"] == len(result["text_content"])
    assert meta["table_count"] == 0
    assert result["tables"] == []

    for i, block in enumerate(result["text_content"]):
        assert block["type"] == "NarrativeText"
        assert isinstance(block["text"], str)
        assert block["metadata"]["page_number"] == 0
        assert block["metadata"]["block_index"] == i


# ---------------------------------------------------------------------------
# RTF
# ---------------------------------------------------------------------------

def test_rtf(processor, tmp_path):
    p = tmp_path / "sample.rtf"
    p.write_text(r"{\rtf1\ansi Hello RTF world}")
    result = processor.process(p)
    _assert_result_shape(result, p)
    assert any("Hello RTF world" in b["text"] for b in result["text_content"])


# ---------------------------------------------------------------------------
# XML / RSS
# ---------------------------------------------------------------------------

def test_xml(processor, tmp_path):
    p = tmp_path / "sample.xml"
    p.write_text('<?xml version="1.0"?><root><item>Alpha</item><item>Beta</item></root>')
    result = processor.process(p)
    _assert_result_shape(result, p)
    texts = [b["text"] for b in result["text_content"]]
    assert "Alpha" in texts
    assert "Beta" in texts


def test_rss(processor, tmp_path):
    p = tmp_path / "feed.rss"
    p.write_text(
        '<?xml version="1.0"?>'
        "<rss><channel><item><title>News</title></item></channel></rss>"
    )
    result = processor.process(p)
    _assert_result_shape(result, p)
    assert any("News" in b["text"] for b in result["text_content"])


# ---------------------------------------------------------------------------
# EML
# ---------------------------------------------------------------------------

def test_eml(processor, tmp_path):
    p = tmp_path / "msg.eml"
    p.write_text(
        "Subject: Test Email\n"
        "From: alice@example.com\n"
        "To: bob@example.com\n"
        "Content-Type: text/plain\n\n"
        "Hello from EML body."
    )
    result = processor.process(p)
    _assert_result_shape(result, p)
    texts = " ".join(b["text"] for b in result["text_content"])
    assert "Test Email" in texts
    assert "Hello from EML body" in texts


# ---------------------------------------------------------------------------
# MBOX
# ---------------------------------------------------------------------------

def test_mbox(processor, tmp_path):
    p = tmp_path / "mail.mbox"
    p.write_text(
        "From alice@example.com Mon Jan  1 00:00:00 2024\n"
        "Subject: Mbox Test\n"
        "Content-Type: text/plain\n\n"
        "Body of mbox message.\n\n"
    )
    result = processor.process(p)
    _assert_result_shape(result, p)
    texts = " ".join(b["text"] for b in result["text_content"])
    assert "Mbox Test" in texts
    assert "Body of mbox message" in texts


# ---------------------------------------------------------------------------
# ICS
# ---------------------------------------------------------------------------

def test_ics(processor, tmp_path):
    p = tmp_path / "event.ics"
    p.write_text(textwrap.dedent("""\
        BEGIN:VCALENDAR
        BEGIN:VEVENT
        SUMMARY:Team Standup
        DESCRIPTION:Daily sync meeting
        END:VEVENT
        END:VCALENDAR
    """))
    result = processor.process(p)
    _assert_result_shape(result, p)
    texts = [b["text"] for b in result["text_content"]]
    assert "Team Standup" in texts
    assert "Daily sync meeting" in texts


# ---------------------------------------------------------------------------
# VCF
# ---------------------------------------------------------------------------

def test_vcf(processor, tmp_path):
    p = tmp_path / "contact.vcf"
    p.write_text(textwrap.dedent("""\
        BEGIN:VCARD
        VERSION:3.0
        FN:Jane Doe
        ORG:Acme Corp
        NOTE:VIP customer
        END:VCARD
    """))
    result = processor.process(p)
    _assert_result_shape(result, p)
    texts = [b["text"] for b in result["text_content"]]
    assert "Jane Doe" in texts
    assert "Acme Corp" in texts
    assert "VIP customer" in texts


# ---------------------------------------------------------------------------
# EPUB (minimal ZIP-based epub)
# ---------------------------------------------------------------------------

def test_epub(processor, tmp_path):
    epub_path = tmp_path / "book.epub"

    container_xml = textwrap.dedent("""\
        <?xml version="1.0"?>
        <container xmlns="urn:oasis:names:tc:opendocument:xmlns:container" version="1.0">
          <rootfiles>
            <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>
          </rootfiles>
        </container>
    """)

    content_opf = textwrap.dedent("""\
        <?xml version="1.0"?>
        <package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="uid">
          <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
            <dc:title>Test Book</dc:title>
            <dc:identifier id="uid">test-epub-001</dc:identifier>
            <dc:language>en</dc:language>
          </metadata>
          <manifest>
            <item id="ch1" href="ch1.xhtml" media-type="application/xhtml+xml"/>
          </manifest>
          <spine>
            <itemref idref="ch1"/>
          </spine>
        </package>
    """)

    ch1_xhtml = textwrap.dedent("""\
        <?xml version="1.0" encoding="utf-8"?>
        <html xmlns="http://www.w3.org/1999/xhtml">
        <head><title>Chapter 1</title></head>
        <body><p>Once upon a time in EPUB land.</p></body>
        </html>
    """)

    with ZipFile(epub_path, "w") as zf:
        zf.writestr("mimetype", "application/epub+zip")
        zf.writestr("META-INF/container.xml", container_xml)
        zf.writestr("OEBPS/content.opf", content_opf)
        zf.writestr("OEBPS/ch1.xhtml", ch1_xhtml)

    result = processor.process(epub_path)
    _assert_result_shape(result, epub_path)
    texts = " ".join(b["text"] for b in result["text_content"])
    assert "EPUB land" in texts


# ---------------------------------------------------------------------------
# ODT (minimal ODF text document)
# ---------------------------------------------------------------------------

def test_odt(processor, tmp_path):
    odt_path = tmp_path / "doc.odt"

    manifest = textwrap.dedent("""\
        <?xml version="1.0" encoding="UTF-8"?>
        <manifest:manifest xmlns:manifest="urn:oasis:names:tc:opendocument:xmlns:manifest:1.0">
          <manifest:file-entry manifest:full-path="/" manifest:media-type="application/vnd.oasis.opendocument.text"/>
          <manifest:file-entry manifest:full-path="content.xml" manifest:media-type="text/xml"/>
        </manifest:manifest>
    """)

    content = textwrap.dedent("""\
        <?xml version="1.0" encoding="UTF-8"?>
        <office:document-content
            xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
            xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0">
          <office:body>
            <office:text>
              <text:p>Hello from ODT document.</text:p>
            </office:text>
          </office:body>
        </office:document-content>
    """)

    with ZipFile(odt_path, "w") as zf:
        zf.writestr("mimetype", "application/vnd.oasis.opendocument.text")
        zf.writestr("META-INF/manifest.xml", manifest)
        zf.writestr("content.xml", content)

    result = processor.process(odt_path)
    _assert_result_shape(result, odt_path)
    texts = " ".join(b["text"] for b in result["text_content"])
    assert "Hello from ODT" in texts


# ---------------------------------------------------------------------------
# ODS (minimal ODF spreadsheet)
# ---------------------------------------------------------------------------

def test_ods(processor, tmp_path):
    ods_path = tmp_path / "sheet.ods"

    manifest = textwrap.dedent("""\
        <?xml version="1.0" encoding="UTF-8"?>
        <manifest:manifest xmlns:manifest="urn:oasis:names:tc:opendocument:xmlns:manifest:1.0">
          <manifest:file-entry manifest:full-path="/" manifest:media-type="application/vnd.oasis.opendocument.spreadsheet"/>
          <manifest:file-entry manifest:full-path="content.xml" manifest:media-type="text/xml"/>
        </manifest:manifest>
    """)

    content = textwrap.dedent("""\
        <?xml version="1.0" encoding="UTF-8"?>
        <office:document-content
            xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
            xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0"
            xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0">
          <office:body>
            <office:spreadsheet>
              <table:table table:name="Sheet1">
                <table:table-row>
                  <table:table-cell><text:p>Name</text:p></table:table-cell>
                  <table:table-cell><text:p>Age</text:p></table:table-cell>
                </table:table-row>
                <table:table-row>
                  <table:table-cell><text:p>Alice</text:p></table:table-cell>
                  <table:table-cell><text:p>30</text:p></table:table-cell>
                </table:table-row>
              </table:table>
            </office:spreadsheet>
          </office:body>
        </office:document-content>
    """)

    with ZipFile(ods_path, "w") as zf:
        zf.writestr("mimetype", "application/vnd.oasis.opendocument.spreadsheet")
        zf.writestr("META-INF/manifest.xml", manifest)
        zf.writestr("content.xml", content)

    result = processor.process(ods_path)
    _assert_result_shape(result, ods_path)
    texts = " ".join(b["text"] for b in result["text_content"])
    assert "Alice" in texts


# ---------------------------------------------------------------------------
# ODP (minimal ODF presentation)
# ---------------------------------------------------------------------------

def test_odp(processor, tmp_path):
    odp_path = tmp_path / "pres.odp"

    manifest = textwrap.dedent("""\
        <?xml version="1.0" encoding="UTF-8"?>
        <manifest:manifest xmlns:manifest="urn:oasis:names:tc:opendocument:xmlns:manifest:1.0">
          <manifest:file-entry manifest:full-path="/" manifest:media-type="application/vnd.oasis.opendocument.presentation"/>
          <manifest:file-entry manifest:full-path="content.xml" manifest:media-type="text/xml"/>
        </manifest:manifest>
    """)

    content = textwrap.dedent("""\
        <?xml version="1.0" encoding="UTF-8"?>
        <office:document-content
            xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
            xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"
            xmlns:draw="urn:oasis:names:tc:opendocument:xmlns:drawing:1.0"
            xmlns:presentation="urn:oasis:names:tc:opendocument:xmlns:presentation:1.0">
          <office:body>
            <office:presentation>
              <draw:page>
                <draw:frame><draw:text-box><text:p>Slide One Title</text:p></draw:text-box></draw:frame>
              </draw:page>
            </office:presentation>
          </office:body>
        </office:document-content>
    """)

    with ZipFile(odp_path, "w") as zf:
        zf.writestr("mimetype", "application/vnd.oasis.opendocument.presentation")
        zf.writestr("META-INF/manifest.xml", manifest)
        zf.writestr("content.xml", content)

    result = processor.process(odp_path)
    _assert_result_shape(result, odp_path)
    texts = " ".join(b["text"] for b in result["text_content"])
    assert "Slide One Title" in texts


# ---------------------------------------------------------------------------
# MSG (extract-msg) — skipped if library not installed
# ---------------------------------------------------------------------------

def test_msg_not_found(processor, tmp_path):
    """MSG handler raises FileNotFoundError for missing file."""
    p = tmp_path / "missing.msg"
    with pytest.raises(FileNotFoundError):
        processor.process(p)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_unsupported_extension(processor, tmp_path):
    p = tmp_path / "file.xyz"
    p.write_text("nope")
    with pytest.raises(ValueError, match="No native handler"):
        processor.process(p)


def test_file_not_found(processor, tmp_path):
    p = tmp_path / "ghost.rtf"
    with pytest.raises(FileNotFoundError):
        processor.process(p)


def test_supported_types(processor):
    types = processor.supported_types()
    for ext in ["rtf", "xml", "rss", "eml", "mbox", "ics", "vcf", "epub", "odt", "ods", "odp", "msg"]:
        assert ext in types, f"{ext} missing from supported_types()"
