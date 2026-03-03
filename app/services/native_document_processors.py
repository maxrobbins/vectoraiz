"""
Native document processors — lightweight Python replacements for Apache Tika.

BQ-VZ-PERF Phase 3: Each handler extracts text using a small, focused library
and returns the SAME dict format as TikaDocumentProcessor / LocalDocumentProcessor:

    {
        "text_content": [{"type": "NarrativeText", "text": "...", "metadata": {"page_number": 0, "block_index": i}}],
        "tables": [],
        "metadata": {"filename": "...", "file_type": "...", "element_count": N, "text_blocks": N, "table_count": 0, "processor": "native"},
    }
"""

import email
import logging
import mailbox
import tempfile
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def _build_result(paragraphs: List[str], filepath: Path) -> Dict[str, Any]:
    """Convert a list of paragraph strings into the standard document result dict."""
    text_content = [
        {
            "type": "NarrativeText",
            "text": p,
            "metadata": {"page_number": 0, "block_index": i},
        }
        for i, p in enumerate(paragraphs)
    ]
    return {
        "text_content": text_content,
        "tables": [],
        "metadata": {
            "filename": filepath.name,
            "file_type": filepath.suffix.lstrip("."),
            "element_count": len(text_content),
            "text_blocks": len(text_content),
            "table_count": 0,
            "processor": "native",
        },
    }


def _split_paragraphs(text: str) -> List[str]:
    """Split text on double-newlines, strip blanks."""
    return [p.strip() for p in text.split("\n\n") if p.strip()]


# ---------------------------------------------------------------------------
# Individual format handlers
# ---------------------------------------------------------------------------

def _process_rtf(filepath: Path) -> Dict[str, Any]:
    from striprtf.striprtf import rtf_to_text

    raw = filepath.read_text(errors="replace")
    text = rtf_to_text(raw)
    return _build_result(_split_paragraphs(text), filepath)


def _process_xml(filepath: Path) -> Dict[str, Any]:
    from lxml import etree

    tree = etree.parse(str(filepath))
    texts = [t.strip() for t in tree.xpath("//text()") if t.strip()]
    return _build_result(texts or [""], filepath)


def _process_rss(filepath: Path) -> Dict[str, Any]:
    """Parse RSS/Atom feeds using feedparser for better semantic extraction."""
    try:
        import feedparser
    except ImportError:
        # Fall back to generic XML extraction if feedparser unavailable
        return _process_xml(filepath)

    feed = feedparser.parse(str(filepath))
    parts: List[str] = []

    if feed.feed.get("title"):
        parts.append(f"Feed: {feed.feed.title}")

    for entry in feed.entries:
        if entry.get("title"):
            parts.append(entry.title)
        if entry.get("summary"):
            parts.append(entry.summary)
        elif entry.get("description"):
            parts.append(entry.description)

    return _build_result(parts or [""], filepath)


def _process_eml(filepath: Path) -> Dict[str, Any]:
    raw = filepath.read_bytes()
    msg = email.message_from_bytes(raw)

    parts: List[str] = []
    subject = msg.get("Subject", "")
    if subject:
        parts.append(f"Subject: {subject}")

    for part in msg.walk():
        if part.get_content_type() == "text/plain":
            payload = part.get_payload(decode=True)
            if payload:
                parts.append(payload.decode(errors="replace").strip())

    return _build_result(parts or [""], filepath)


def _process_mbox(filepath: Path) -> Dict[str, Any]:
    mbox = mailbox.mbox(str(filepath))
    parts: List[str] = []

    for msg in mbox:
        subject = msg.get("Subject", "")
        if subject:
            parts.append(f"Subject: {subject}")
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    parts.append(payload.decode(errors="replace").strip())

    return _build_result(parts or [""], filepath)


def _process_ics(filepath: Path) -> Dict[str, Any]:
    from icalendar import Calendar

    raw = filepath.read_bytes()
    cal = Calendar.from_ical(raw)
    parts: List[str] = []

    for component in cal.walk():
        if component.name == "VEVENT":
            summary = str(component.get("SUMMARY", ""))
            description = str(component.get("DESCRIPTION", ""))
            if summary:
                parts.append(summary)
            if description:
                parts.append(description)

    return _build_result(parts or [""], filepath)


def _process_vcf(filepath: Path) -> Dict[str, Any]:
    import vobject

    raw = filepath.read_text(errors="replace")
    parts: List[str] = []

    for vcard in vobject.readComponents(raw):
        fn = getattr(vcard, "fn", None)
        if fn:
            parts.append(fn.value)
        org = getattr(vcard, "org", None)
        if org:
            parts.append(";".join(org.value))
        note = getattr(vcard, "note", None)
        if note:
            parts.append(note.value)

    return _build_result(parts or [""], filepath)


def _process_epub(filepath: Path) -> Dict[str, Any]:
    import ebooklib
    from ebooklib import epub
    from lxml import etree

    book = epub.read_epub(str(filepath), options={"ignore_ncx": True})
    parts: List[str] = []

    for item in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
        html = item.get_body_content()
        tree = etree.fromstring(html, parser=etree.HTMLParser())
        texts = tree.xpath("//text()")
        text = " ".join(t.strip() for t in texts if t.strip())
        if text:
            parts.append(text)

    return _build_result(parts or [""], filepath)


def _process_odt(filepath: Path) -> Dict[str, Any]:
    from odf.opendocument import load as odf_load
    from odf.text import P

    doc = odf_load(str(filepath))
    parts: List[str] = []

    for p in doc.getElementsByType(P):
        text = ""
        for node in p.childNodes:
            if hasattr(node, "data"):
                text += node.data
            elif hasattr(node, "__str__"):
                text += str(node)
        text = text.strip()
        if text:
            parts.append(text)

    return _build_result(parts or [""], filepath)


def _process_ods(filepath: Path) -> Dict[str, Any]:
    from odf.opendocument import load as odf_load
    from odf.table import Table, TableRow, TableCell
    from odf.text import P

    doc = odf_load(str(filepath))
    parts: List[str] = []

    for table in doc.getElementsByType(Table):
        for row in table.getElementsByType(TableRow):
            cells: List[str] = []
            for cell in row.getElementsByType(TableCell):
                cell_text = ""
                for p in cell.getElementsByType(P):
                    for node in p.childNodes:
                        if hasattr(node, "data"):
                            cell_text += node.data
                cells.append(cell_text.strip())
            line = "\t".join(cells).strip()
            if line:
                parts.append(line)

    return _build_result(parts or [""], filepath)


def _process_odp(filepath: Path) -> Dict[str, Any]:
    from odf.opendocument import load as odf_load
    from odf.text import P

    doc = odf_load(str(filepath))
    parts: List[str] = []

    for p in doc.getElementsByType(P):
        text = ""
        for node in p.childNodes:
            if hasattr(node, "data"):
                text += node.data
            elif hasattr(node, "__str__"):
                text += str(node)
        text = text.strip()
        if text:
            parts.append(text)

    return _build_result(parts or [""], filepath)


def _process_msg(filepath: Path) -> Dict[str, Any]:
    import extract_msg

    msg = extract_msg.Message(str(filepath))
    parts: List[str] = []

    if msg.subject:
        parts.append(f"Subject: {msg.subject}")
    if msg.body:
        parts.extend(_split_paragraphs(msg.body))

    msg.close()
    return _build_result(parts or [""], filepath)


# ---------------------------------------------------------------------------
# Handler registry
# ---------------------------------------------------------------------------

_HANDLERS = {
    "rtf": _process_rtf,
    "xml": _process_xml,
    "rss": _process_rss,  # RSS/Atom via feedparser
    "eml": _process_eml,
    "mbox": _process_mbox,
    "ics": _process_ics,
    "vcf": _process_vcf,
    "epub": _process_epub,
    "odt": _process_odt,
    "ods": _process_ods,
    "odp": _process_odp,
    "msg": _process_msg,
}


class NativeFormatProcessor:
    """Drop-in replacement for TikaDocumentProcessor using pure-Python libs."""

    NATIVE_TYPES = set(_HANDLERS.keys())

    def supported_types(self) -> List[str]:
        return list(self.NATIVE_TYPES)

    def process(self, filepath: Path) -> Dict[str, Any]:
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        ext = filepath.suffix.lstrip(".").lower()
        handler = _HANDLERS.get(ext)
        if handler is None:
            raise ValueError(f"No native handler for .{ext}")

        return handler(filepath)
