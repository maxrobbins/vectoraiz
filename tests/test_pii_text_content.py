"""Tests for PII scanning of text-based document content (PDF, TXT, DOCX, etc.)."""

import pytest

from app.services.pii_service import PIIService


@pytest.fixture
def pii_service():
    """Create PII service instance."""
    return PIIService()


class TestScanTextContent:
    """Tests for scan_text_content() method."""

    def test_detects_email_in_text_blocks(self, pii_service):
        """Text blocks containing email addresses should be flagged."""
        blocks = [
            "Please contact John Smith at john.smith@example.com for details.",
            "The project deadline is next Friday.",
            "Send invoices to billing@company.org",
        ]
        result = pii_service.scan_text_content(blocks)

        assert result["columns_with_pii"] > 0
        assert result["overall_risk"] != "none"
        assert result["column_results"][0]["column"] == "document_content"
        assert "EMAIL_ADDRESS" in result["column_results"][0]["pii_types"]

    def test_detects_phone_in_text_blocks(self, pii_service):
        """Text blocks containing phone numbers should be flagged."""
        blocks = [
            "For support, call 212-555-1234 during business hours.",
            "Our office is located in downtown.",
        ]
        result = pii_service.scan_text_content(blocks, score_threshold=0.3)

        assert result["columns_with_pii"] > 0
        assert "PHONE_NUMBER" in result["column_results"][0]["pii_types"]

    def test_clean_text_blocks(self, pii_service):
        """Text blocks without PII should return no or low-risk findings."""
        blocks = [
            "The quarterly revenue increased by 15 percent year over year.",
            "Product launches are scheduled for next quarter.",
            "Market analysis suggests strong growth in the sector.",
        ]
        result = pii_service.scan_text_content(blocks, score_threshold=0.7)

        # Clean text should have minimal or no PII (same tolerance as test_scan_clean_dataset)
        assert result["overall_risk"] in ("none", "low")

    def test_empty_text_blocks(self, pii_service):
        """Empty block list should return clean results."""
        result = pii_service.scan_text_content([])

        assert result["overall_risk"] == "none"
        assert result["columns_with_pii"] == 0
        assert result["total_blocks"] == 0
        assert result["blocks_sampled"] == 0

    def test_result_format_matches_scan_dataset(self, pii_service):
        """scan_text_content() should return same top-level keys as scan_dataset()."""
        blocks = ["Contact me at test@example.com"]
        result = pii_service.scan_text_content(blocks)

        # All keys that scan_dataset() returns must be present
        expected_keys = {
            "scanned_at", "total_rows", "rows_sampled",
            "total_columns", "columns_with_pii", "columns_clean",
            "overall_risk", "privacy_score", "column_results",
            "clean_columns", "duration_seconds", "entities_checked",
        }
        assert expected_keys.issubset(set(result.keys()))

        # Text-specific extras
        assert "scan_type" in result
        assert result["scan_type"] == "text_content"
        assert "total_blocks" in result
        assert "blocks_sampled" in result

    def test_column_result_format(self, pii_service):
        """Each column_result entry should have the standard fields."""
        blocks = ["My SSN is 111-22-3344"]
        result = pii_service.scan_text_content(blocks, score_threshold=0.3)

        assert len(result["column_results"]) == 1
        entry = result["column_results"][0]
        assert "column" in entry
        assert "pii_detected" in entry
        assert "pii_types" in entry
        assert "risk_level" in entry
        assert "sample_matches" in entry

    def test_sampling_limits_blocks(self, pii_service):
        """When blocks exceed sample_size, only sample_size are scanned."""
        blocks = [f"Block {i} has email user{i}@test.com" for i in range(200)]
        result = pii_service.scan_text_content(blocks, sample_size=10)

        assert result["total_blocks"] == 200
        assert result["blocks_sampled"] == 10

    def test_privacy_score_calculated(self, pii_service):
        """Privacy score should be calculated and in valid range."""
        blocks = [
            "John Smith's SSN is 111-22-3344",
            "His email is john@test.com",
        ]
        result = pii_service.scan_text_content(blocks, score_threshold=0.3)

        assert "privacy_score" in result
        assert 0.0 <= result["privacy_score"] <= 10.0
        # With PII detected, score should be less than perfect
        assert result["privacy_score"] < 10.0

    def test_handles_none_and_empty_blocks(self, pii_service):
        """None values and empty strings in blocks should be skipped gracefully."""
        blocks = [None, "", "   ", "Contact: admin@test.com", None, ""]
        result = pii_service.scan_text_content(blocks)

        # Should still detect the email without crashing
        assert result["columns_with_pii"] > 0

    def test_truncates_very_long_blocks(self, pii_service):
        """Very long text blocks should be truncated, not cause timeouts."""
        long_block = "A" * 50000 + " email: test@example.com"
        blocks = [long_block]
        result = pii_service.scan_text_content(blocks)

        # Should complete without error (block truncated to 10000 chars)
        assert "duration_seconds" in result
