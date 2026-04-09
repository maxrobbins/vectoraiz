"""Tests for filename and path sanitization utilities (BQ-115)."""


from app.utils.sanitization import sanitize_filename, sql_quote_literal


# ── sanitize_filename ─────────────────────────────────────────────────


class TestSanitizeFilename:
    def test_simple_filename_unchanged(self):
        assert sanitize_filename("report.csv") == "report.csv"

    def test_single_quotes_replaced(self):
        result = sanitize_filename("O'Brien.csv")
        assert "'" not in result
        assert result.endswith(".csv")

    def test_double_quotes_replaced(self):
        result = sanitize_filename('file"name.csv')
        assert '"' not in result

    def test_path_traversal_stripped(self):
        result = sanitize_filename("../../etc/passwd")
        assert result == "passwd"
        assert "/" not in result
        assert ".." not in result

    def test_path_traversal_windows(self):
        result = sanitize_filename("..\\..\\windows\\system32\\config")
        assert "\\" not in result
        assert ".." not in result
        assert result != "_unnamed"  # should still produce a usable name

    def test_backslashes_replaced(self):
        result = sanitize_filename("dir\\file.csv")
        assert "\\" not in result

    def test_semicolons_replaced(self):
        result = sanitize_filename("file;drop table.csv")
        assert ";" not in result

    def test_null_bytes_replaced(self):
        result = sanitize_filename("file\x00.csv")
        assert "\x00" not in result

    def test_extension_lowercased(self):
        assert sanitize_filename("data.CSV") == "data.csv"
        assert sanitize_filename("data.Parquet") == "data.parquet"

    def test_unicode_filename_preserved(self):
        result = sanitize_filename("données_2024.csv")
        assert "données_2024" in result
        assert result.endswith(".csv")

    def test_empty_string_returns_unnamed(self):
        assert sanitize_filename("") == "_unnamed"

    def test_whitespace_only_returns_unnamed(self):
        assert sanitize_filename("   ") == "_unnamed"

    def test_none_returns_unnamed(self):
        assert sanitize_filename(None) == "_unnamed"

    def test_slash_only_returns_unnamed(self):
        assert sanitize_filename("/") == "_unnamed"

    def test_long_filename_truncated(self):
        long_name = "a" * 300 + ".csv"
        result = sanitize_filename(long_name)
        assert len(result) <= 200
        assert result.endswith(".csv")

    def test_long_filename_preserves_extension(self):
        long_name = "x" * 250 + ".parquet"
        result = sanitize_filename(long_name)
        assert result.endswith(".parquet")
        assert len(result) <= 200

    def test_directory_components_stripped(self):
        assert sanitize_filename("/uploads/data/report.csv") == "report.csv"

    def test_colons_replaced(self):
        result = sanitize_filename("file:name.csv")
        assert ":" not in result


# ── sql_quote_literal ─────────────────────────────────────────────────


class TestSqlQuoteLiteral:
    def test_no_quotes_unchanged(self):
        assert sql_quote_literal("/data/file.parquet") == "/data/file.parquet"

    def test_single_quote_escaped(self):
        assert sql_quote_literal("O'Brien.csv") == "O''Brien.csv"

    def test_multiple_quotes_escaped(self):
        assert sql_quote_literal("it's a 'test'") == "it''s a ''test''"

    def test_empty_string(self):
        assert sql_quote_literal("") == ""

    def test_path_with_apostrophe(self):
        path = "/data/user's files/report.csv"
        result = sql_quote_literal(path)
        assert result == "/data/user''s files/report.csv"
