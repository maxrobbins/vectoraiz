"""
Tests for aim-data sidebar ordering in Sidebar.tsx.
"""

import ast
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SIDEBAR_PATH = REPO_ROOT / "frontend" / "src" / "components" / "layout" / "Sidebar.tsx"
EXPECTED_ORDER = [
    "/",
    "/datasets",
    "/ai-market",
    "/data-requests",
    "/search",
    "/sql",
    "/artifacts",
    "/databases",
    "/earnings",
    "/billing",
    "/data-types",
    "/settings",
]


def _read_sidebar() -> str:
    assert SIDEBAR_PATH.exists(), f"Sidebar.tsx not found at {SIDEBAR_PATH}"
    return SIDEBAR_PATH.read_text()


def _parse_array(source: str, const_name: str) -> list[str]:
    match = re.search(rf"const\s+{const_name}\s*=\s*\[(.*?)\];", source, re.DOTALL)
    assert match, f"{const_name} array not found"
    return ast.literal_eval("[" + match.group(1) + "]")


def _parse_number(source: str, const_name: str) -> int:
    match = re.search(rf"const\s+{const_name}\s*=\s*(\d+);", source)
    assert match, f"{const_name} constant not found"
    return int(match.group(1))


def test_nav_order_aim_data_contains_expected_paths_in_order():
    content = _read_sidebar()
    assert _parse_array(content, "NAV_ORDER_AIM_DATA") == EXPECTED_ORDER


def test_separator_index_aim_data_equals_four():
    content = _read_sidebar()
    assert _parse_number(content, "SEPARATOR_INDEX_AIM_DATA") == 4


def test_get_ordered_items_aim_data_uses_expected_top_bottom_split():
    content = _read_sidebar()
    order = _parse_array(content, "NAV_ORDER_AIM_DATA")
    separator_index = _parse_number(content, "SEPARATOR_INDEX_AIM_DATA")

    assert 'channel === "aim-data"' in content
    assert "? NAV_ORDER_MARKETPLACE" in content
    assert "? NAV_ORDER_AIM_DATA" in content
    assert "? SEPARATOR_INDEX_MARKETPLACE" in content
    assert "? SEPARATOR_INDEX_AIM_DATA" in content
    assert "top: ordered.slice(0, sepIdx)" in content
    assert "bottom: ordered.slice(sepIdx)" in content

    assert order[:separator_index] == [
        "/",
        "/datasets",
        "/ai-market",
        "/data-requests",
    ]
    assert order[separator_index:] == [
        "/search",
        "/sql",
        "/artifacts",
        "/databases",
        "/earnings",
        "/billing",
        "/data-types",
        "/settings",
    ]
