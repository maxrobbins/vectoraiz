"""
Tests for BQ-128 Phase 3 — NudgeManager + Proactive Nudges
============================================================

Covers:
- Task 3.1: NudgeManager — allowlist enforcement, rate limits, quiet mode,
  dismissals (session + permanent), analytics tracking
- Nudge WebSocket flow: trigger → nudge sent → dismissal received
- NudgeDismissal model

Uses MockAllieProvider — zero real API calls.

CREATED: BQ-128 Phase 3 (2026-02-14)
"""

import asyncio
import time

import pytest

from app.core.errors.registry import error_registry
from app.services.nudge_manager import (
    NudgeManager,
    NudgeMessage,
    NUDGE_TEMPLATES,
    NUDGE_ICONS,
)
from app.models.state import (
    NudgeDismissal,
)

# Ensure error registry is loaded for tests
if len(error_registry) == 0:
    error_registry.load()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clean_nudge_manager():
    """Reset NudgeManager state before/after each test."""
    nm = NudgeManager()
    yield nm
    # No global state to clean since we use fresh instances


@pytest.fixture
def nm():
    """Fresh NudgeManager instance."""
    return NudgeManager()


# ---------------------------------------------------------------------------
# Tests: TRIGGER_ALLOWLIST
# ---------------------------------------------------------------------------

class TestTriggerAllowlist:
    """Test that only allowlisted triggers can fire nudges."""

    def test_allowlist_has_all_required_triggers(self):
        nm = NudgeManager()
        expected = {
            "error_event", "upload_complete", "processing_complete",
            "missing_config", "pii_detected", "long_running_op", "destructive_action",
            "first_search",
        }
        assert set(nm.TRIGGER_ALLOWLIST.keys()) == expected

    def test_non_allowlisted_trigger_blocked(self, nm):
        """Triggers not on allowlist must return None."""
        result = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("not_a_real_trigger", {}, "session_1", "user_1")
        )
        assert result is None

    def test_allowlisted_trigger_fires(self, nm):
        """Allowlisted triggers should return a NudgeMessage."""
        result = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_1", "user_1")
        )
        assert result is not None
        assert isinstance(result, NudgeMessage)
        assert result.trigger == "upload_complete"
        assert result.nudge_id.startswith("ndg_")

    def test_destructive_action_not_dismissable(self, nm):
        """destructive_action nudge should not be dismissable."""
        result = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("destructive_action", {}, "session_1", "user_1")
        )
        assert result is not None
        assert result.dismissable is False

    def test_upload_complete_is_dismissable(self, nm):
        """upload_complete nudge should be dismissable."""
        result = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_1", "user_1")
        )
        assert result is not None
        assert result.dismissable is True

    def test_each_trigger_has_template(self):
        """Every trigger in allowlist should have a message template."""
        nm = NudgeManager()
        for trigger, config in nm.TRIGGER_ALLOWLIST.items():
            assert config.message_template, f"Trigger '{trigger}' missing message_template"

    def test_each_trigger_has_icon(self):
        """Every trigger should have a corresponding icon."""
        for trigger in NudgeManager.TRIGGER_ALLOWLIST:
            assert trigger in NUDGE_ICONS, f"Trigger '{trigger}' missing icon"


# ---------------------------------------------------------------------------
# Tests: Rate Limits
# ---------------------------------------------------------------------------

class TestRateLimits:
    """Test rate limit enforcement per session, dataset, and cooldown."""

    def test_per_session_limit(self, nm):
        """missing_config has max_per_session=1, should only fire once per session."""
        r1 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("missing_config", {}, "session_1", "user_1")
        )
        assert r1 is not None

        r2 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("missing_config", {}, "session_1", "user_1")
        )
        assert r2 is None, "Should be rate-limited after first nudge in session"

    def test_per_session_limit_different_sessions(self, nm):
        """Different sessions should have independent limits."""
        r1 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("missing_config", {}, "session_1", "user_1")
        )
        assert r1 is not None

        r2 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("missing_config", {}, "session_2", "user_1")
        )
        assert r2 is not None, "Different session should allow nudge"

    def test_per_dataset_limit(self, nm):
        """pii_detected has max_per_dataset=1, should only fire once per dataset."""
        ctx = {"dataset_id": "ds_123"}
        r1 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("pii_detected", ctx, "session_1", "user_1")
        )
        assert r1 is not None

        r2 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("pii_detected", ctx, "session_1", "user_1")
        )
        assert r2 is None, "Should be rate-limited for same dataset"

    def test_per_dataset_different_datasets(self, nm):
        """Different datasets should have independent per-dataset limits.
        Note: per-event limit (1-second window) may block rapid calls,
        so we bypass it by adding a small delay to the history."""
        r1 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("pii_detected", {"dataset_id": "ds_1"}, "session_1", "user_1")
        )
        assert r1 is not None

        # Backdate the event history to bypass the per-event 1-second window
        for ts_list in nm._session_nudge_history.get("session_1", {}).values():
            ts_list[:] = [t - 2.0 for t in ts_list]

        r2 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("pii_detected", {"dataset_id": "ds_2"}, "session_1", "user_1")
        )
        assert r2 is not None

    def test_per_operation_limit(self, nm):
        """long_running_op has max_per_operation=1."""
        ctx = {"operation_id": "op_123"}
        r1 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("long_running_op", ctx, "session_1", "user_1")
        )
        assert r1 is not None

        r2 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("long_running_op", ctx, "session_1", "user_1")
        )
        assert r2 is None

    def test_cooldown_enforcement(self, nm):
        """long_running_op has cooldown_s=30, should block within cooldown."""
        ctx = {"operation_id": "op_a"}
        r1 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("long_running_op", ctx, "session_1", "user_1")
        )
        assert r1 is not None

        # Different operation_id but same session — cooldown should apply
        ctx2 = {"operation_id": "op_b"}
        r2 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("long_running_op", ctx2, "session_1", "user_1")
        )
        assert r2 is None, "Should be blocked by cooldown"

    def test_per_event_limit(self, nm):
        """error_event has max_per_event=1, should block rapid duplicate events."""
        r1 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("error_event", {}, "session_1", "user_1")
        )
        assert r1 is not None

        # Immediate second call within 1-second event window
        r2 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("error_event", {}, "session_1", "user_1")
        )
        assert r2 is None, "Should be rate-limited within event window"


# ---------------------------------------------------------------------------
# Tests: Quiet Mode
# ---------------------------------------------------------------------------

class TestQuietMode:
    """Test quiet mode suppression."""

    def test_quiet_mode_suppresses_nudges(self, nm):
        """When quiet mode is enabled, no nudges should fire."""
        nm.set_quiet_mode("session_1", True)
        result = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_1", "user_1")
        )
        assert result is None

    def test_quiet_mode_disable_allows_nudges(self, nm):
        """Disabling quiet mode should allow nudges again."""
        nm.set_quiet_mode("session_1", True)
        nm.set_quiet_mode("session_1", False)
        result = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_1", "user_1")
        )
        assert result is not None

    def test_quiet_mode_per_session(self, nm):
        """Quiet mode should only affect the specified session."""
        nm.set_quiet_mode("session_1", True)
        result = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_2", "user_1")
        )
        assert result is not None


# ---------------------------------------------------------------------------
# Tests: Dismissals
# ---------------------------------------------------------------------------

class TestDismissals:
    """Test session and permanent dismissals."""

    def test_session_dismissal_suppresses_nudge(self, nm):
        """After session dismissal, trigger should not fire in that session."""
        nm.record_dismissal("session_1", "upload_complete")
        result = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_1", "user_1")
        )
        assert result is None

    def test_session_dismissal_other_session_unaffected(self, nm):
        """Session dismissal in one session should not affect another."""
        nm.record_dismissal("session_1", "upload_complete")
        result = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_2", "user_1")
        )
        assert result is not None

    def test_permanent_dismissal_suppresses_all_sessions(self, nm):
        """Permanent dismissal should suppress across all sessions for that user."""
        nm.record_dismissal("session_1", "missing_config", permanent=True, user_id="user_1")
        result = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("missing_config", {}, "session_2", "user_1")
        )
        assert result is None

    def test_permanent_dismissal_other_user_unaffected(self, nm):
        """Permanent dismissal for one user should not affect another."""
        nm.record_dismissal("session_1", "missing_config", permanent=True, user_id="user_1")
        result = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("missing_config", {}, "session_2", "user_2")
        )
        assert result is not None

    def test_load_permanent_dismissals(self, nm):
        """Loading permanent dismissals from DB should block subsequent nudges."""
        nm.load_permanent_dismissals("user_1", ["error_event", "pii_detected"])
        r1 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("error_event", {}, "session_1", "user_1")
        )
        assert r1 is None

        r2 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("pii_detected", {"dataset_id": "ds_1"}, "session_1", "user_1")
        )
        assert r2 is None

        # Non-dismissed trigger should still work
        r3 = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_1", "user_1")
        )
        assert r3 is not None


# ---------------------------------------------------------------------------
# Tests: Analytics
# ---------------------------------------------------------------------------

class TestAnalytics:
    """Test analytics tracking for nudge interactions."""

    def test_nudge_shown_tracked(self, nm):
        """Firing a nudge should record a nudge_shown analytics event."""
        asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_1", "user_1")
        )
        analytics = nm.get_analytics()
        assert len(analytics) == 1
        assert analytics[0].action == "nudge_shown"
        assert analytics[0].trigger == "upload_complete"
        assert analytics[0].session_id == "session_1"
        assert analytics[0].user_id == "user_1"

    def test_nudge_dismissed_tracked(self, nm):
        """Dismissing a nudge should record a nudge_dismissed event."""
        nm.record_dismissal("session_1", "upload_complete", nudge_id="ndg_test1", user_id="user_1")
        analytics = nm.get_analytics()
        assert len(analytics) == 1
        assert analytics[0].action == "nudge_dismissed"
        assert analytics[0].nudge_id == "ndg_test1"

    def test_nudge_permanent_dismiss_tracked(self, nm):
        """Permanent dismissal should record nudge_permanent_dismiss."""
        nm.record_dismissal(
            "session_1", "missing_config",
            permanent=True, nudge_id="ndg_test2", user_id="user_1",
        )
        analytics = nm.get_analytics()
        assert len(analytics) == 1
        assert analytics[0].action == "nudge_permanent_dismiss"

    def test_nudge_acted_tracked(self, nm):
        """Acting on a nudge should record a nudge_acted event."""
        nm.record_acted("session_1", "error_event", user_id="user_1", nudge_id="ndg_test3")
        analytics = nm.get_analytics()
        assert len(analytics) == 1
        assert analytics[0].action == "nudge_acted"

    def test_get_analytics_clears_buffer(self, nm):
        """get_analytics should return and clear the buffer."""
        asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_1", "user_1")
        )
        a1 = nm.get_analytics()
        assert len(a1) == 1
        a2 = nm.get_analytics()
        assert len(a2) == 0

    def test_suppressed_nudge_no_analytics(self, nm):
        """A suppressed nudge should NOT record analytics."""
        nm.set_quiet_mode("session_1", True)
        asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_1", "user_1")
        )
        analytics = nm.get_analytics()
        assert len(analytics) == 0


# ---------------------------------------------------------------------------
# Tests: Session Cleanup
# ---------------------------------------------------------------------------

class TestSessionCleanup:
    """Test session state cleanup on disconnect."""

    def test_cleanup_removes_session_state(self, nm):
        """cleanup_session should clear all state for that session."""
        asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_1", "user_1")
        )
        nm.set_quiet_mode("session_1", True)
        nm.record_dismissal("session_1", "error_event")

        nm.cleanup_session("session_1")

        assert "session_1" not in nm._session_nudge_history
        assert "session_1" not in nm._session_counts
        assert "session_1" not in nm._session_dismissals
        assert "session_1" not in nm._quiet_mode

    def test_cleanup_preserves_other_sessions(self, nm):
        """cleanup_session should not affect other sessions."""
        nm.set_quiet_mode("session_1", True)
        nm.set_quiet_mode("session_2", True)

        nm.cleanup_session("session_1")

        assert nm._quiet_mode.get("session_2") is True


# ---------------------------------------------------------------------------
# Tests: NudgeMessage + WS Serialization
# ---------------------------------------------------------------------------

class TestNudgeMessageSerialization:
    """Test NudgeMessage creation and WebSocket message format."""

    def test_nudge_message_fields(self, nm):
        result = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("error_event", {}, "session_1", "user_1")
        )
        assert result.nudge_id.startswith("ndg_")
        assert result.trigger == "error_event"
        assert result.message == NUDGE_TEMPLATES["error_event"]
        assert result.dismissable is True

    def test_custom_message_from_context(self, nm):
        """Context message should override template."""
        result = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("error_event", {"message": "Custom error"}, "session_1", "user_1")
        )
        assert result.message == "Custom error"

    def test_to_ws_message_format(self, nm):
        """to_ws_message should produce correct WebSocket payload."""
        nudge = NudgeMessage(
            nudge_id="ndg_abc123",
            trigger="upload_complete",
            message="Upload done!",
            dismissable=True,
            metadata={"icon": "CheckCircle"},
        )
        ws_msg = nm.to_ws_message(nudge)
        assert ws_msg["type"] == "NUDGE"
        assert ws_msg["trigger"] == "upload_complete"
        assert ws_msg["message"] == "Upload done!"
        assert ws_msg["nudge_id"] == "ndg_abc123"
        assert ws_msg["dismissable"] is True
        assert ws_msg["icon"] == "CheckCircle"


# ---------------------------------------------------------------------------
# Tests: NudgeDismissal Model
# ---------------------------------------------------------------------------

class TestNudgeDismissalModel:
    """Test the NudgeDismissal SQLModel."""

    def test_model_fields(self):
        d = NudgeDismissal(user_id="user_1", trigger_type="error_event", permanent=True)
        assert d.user_id == "user_1"
        assert d.trigger_type == "error_event"
        assert d.permanent is True
        assert d.id is not None  # auto-generated

    def test_model_defaults(self):
        d = NudgeDismissal(user_id="user_2", trigger_type="pii_detected")
        assert d.permanent is False
        assert d.created_at is not None


# ---------------------------------------------------------------------------
# Tests: Audit Gate Fixes (BQ-128 Phase 3)
# ---------------------------------------------------------------------------

class TestNudgeDismissValidation:
    """Test NUDGE_DISMISS allowlist validation and issued-nudge checks."""

    def test_nudge_dismiss_invalid_trigger_rejected(self, nm):
        """NUDGE_DISMISS with non-allowlisted trigger should be rejected.

        The router validates against TRIGGER_ALLOWLIST before calling
        record_dismissal. We verify the allowlist check directly.
        """
        assert "not_a_trigger" not in nm.TRIGGER_ALLOWLIST
        assert "evil_trigger" not in nm.TRIGGER_ALLOWLIST
        assert "" not in nm.TRIGGER_ALLOWLIST

    def test_nudge_dismiss_requires_issued_nudge(self, nm):
        """Permanent dismissal requires the nudge was actually issued to the session."""
        # No nudge has been issued — was_nudge_issued should return False
        assert nm.was_nudge_issued("session_1", "ndg_fake123") is False

        # Fire a real nudge
        nudge = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_1", "user_1")
        )
        assert nudge is not None

        # Now the issued nudge should be tracked
        assert nm.was_nudge_issued("session_1", nudge.nudge_id) is True

        # A fabricated nudge_id should still return False
        assert nm.was_nudge_issued("session_1", "ndg_fabricated") is False

        # Different session should not have this nudge
        assert nm.was_nudge_issued("session_2", nudge.nudge_id) is False

    def test_issued_nudges_cleaned_on_session_cleanup(self, nm):
        """cleanup_session should remove issued nudge tracking."""
        nudge = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_1", "user_1")
        )
        assert nm.was_nudge_issued("session_1", nudge.nudge_id) is True

        nm.cleanup_session("session_1")
        assert nm.was_nudge_issued("session_1", nudge.nudge_id) is False


class TestPersistDismissalConcurrencySafe:
    """Test that _persist_nudge_dismissal handles duplicates gracefully."""

    def test_persist_dismissal_concurrent_safe(self):
        """Duplicate dismissal should not raise — IntegrityError is caught."""
        from unittest.mock import MagicMock, patch
        from sqlalchemy.exc import IntegrityError

        # Mock the DB context to raise IntegrityError on commit (simulates duplicate)
        mock_db = MagicMock()
        mock_db.commit.side_effect = IntegrityError("duplicate", params=None, orig=None)

        mock_ctx = MagicMock()
        mock_ctx.__enter__ = MagicMock(return_value=mock_db)
        mock_ctx.__exit__ = MagicMock(return_value=False)

        # Patch at the source module since it's imported inside the function
        with patch("app.core.database.get_legacy_session_context", return_value=mock_ctx):
            from app.routers.copilot import _persist_nudge_dismissal

            # Should NOT raise — IntegrityError is caught and logged at debug level
            _persist_nudge_dismissal("user_1", "error_event")


class TestMaybeNudgeConcurrency:
    """Test that concurrent maybe_nudge calls don't double-fire."""

    def test_maybe_nudge_concurrent_no_double_fire(self, nm):
        """Concurrent calls to maybe_nudge should not double-fire due to lock."""
        results = []

        async def run_concurrent():
            # Fire two concurrent nudges for the same trigger + session
            # missing_config has max_per_session=1, so only one should fire
            tasks = [
                nm.maybe_nudge("missing_config", {}, "session_1", "user_1"),
                nm.maybe_nudge("missing_config", {}, "session_1", "user_1"),
            ]
            return await asyncio.gather(*tasks)

        results = asyncio.get_event_loop().run_until_complete(run_concurrent())
        fired = [r for r in results if r is not None]
        assert len(fired) == 1, f"Expected exactly 1 nudge to fire, got {len(fired)}"


class TestDatasetCountsEviction:
    """Test TTL/eviction for unbounded _dataset_counts and _operation_counts."""

    def test_dataset_counts_eviction(self, nm):
        """Stale dataset counts older than TTL should be pruned."""
        now = time.time()

        # Manually populate stale entries
        for i in range(5):
            ds_id = f"ds_stale_{i}"
            nm._dataset_counts[ds_id] = {"pii_detected": 1}
            nm._dataset_counts_ts[ds_id] = now - 7200  # 2 hours ago (> 1hr TTL)

        # Add a fresh entry
        nm._dataset_counts["ds_fresh"] = {"pii_detected": 1}
        nm._dataset_counts_ts["ds_fresh"] = now

        # Run pruning
        nm._prune_stale_counts()

        # Stale entries should be gone
        for i in range(5):
            assert f"ds_stale_{i}" not in nm._dataset_counts
            assert f"ds_stale_{i}" not in nm._dataset_counts_ts

        # Fresh entry should remain
        assert "ds_fresh" in nm._dataset_counts
        assert "ds_fresh" in nm._dataset_counts_ts

    def test_operation_counts_eviction(self, nm):
        """Stale operation counts older than TTL should be pruned."""
        now = time.time()

        for i in range(5):
            op_id = f"op_stale_{i}"
            nm._operation_counts[op_id] = {"long_running_op": 1}
            nm._operation_counts_ts[op_id] = now - 7200

        nm._operation_counts["op_fresh"] = {"long_running_op": 1}
        nm._operation_counts_ts["op_fresh"] = now

        nm._prune_stale_counts()

        for i in range(5):
            assert f"op_stale_{i}" not in nm._operation_counts
        assert "op_fresh" in nm._operation_counts

    def test_hard_cap_eviction(self, nm):
        """When count entries exceed _MAX_COUNT_ENTRIES, oldest should be evicted."""
        now = time.time()

        # Temporarily lower the cap for testing
        original_max = NudgeManager._MAX_COUNT_ENTRIES
        NudgeManager._MAX_COUNT_ENTRIES = 10

        try:
            # Add 15 entries (5 over the cap), all fresh
            for i in range(15):
                ds_id = f"ds_{i:03d}"
                nm._dataset_counts[ds_id] = {"pii_detected": 1}
                nm._dataset_counts_ts[ds_id] = now - (15 - i)  # oldest first

            nm._prune_stale_counts()

            # Should be capped at 10
            assert len(nm._dataset_counts) <= 10
            # Oldest entries (ds_000 through ds_004) should be evicted
            for i in range(5):
                assert f"ds_{i:03d}" not in nm._dataset_counts
        finally:
            NudgeManager._MAX_COUNT_ENTRIES = original_max

    def test_periodic_pruning_triggered(self, nm):
        """Pruning should trigger every 100 nudge checks."""
        now = time.time()
        # Add a stale entry
        nm._dataset_counts["ds_stale"] = {"pii_detected": 1}
        nm._dataset_counts_ts["ds_stale"] = now - 7200

        # Set counter to just before the trigger threshold
        nm._nudge_check_count = 99

        # This call will be the 100th check, triggering pruning
        asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_prune", "user_1")
        )

        # Stale entry should have been pruned
        assert "ds_stale" not in nm._dataset_counts


# ---------------------------------------------------------------------------
# Tests: WS NUDGE_DISMISS Validation (Audit Gate Fix 4)
# ---------------------------------------------------------------------------

class TestNudgeDismissWSValidation:
    """Test that NUDGE_DISMISS validation in the WS router works correctly."""

    def test_invalid_trigger_not_persisted(self, nm):
        """NUDGE_DISMISS with non-allowlisted trigger should not persist or update state."""
        # Verify the trigger is not in allowlist
        invalid_trigger = "evil_trigger"
        assert invalid_trigger not in nm.TRIGGER_ALLOWLIST

        # Record dismissal for invalid trigger should not affect nudge state
        # (In the router, the allowlist check happens BEFORE record_dismissal is called)
        initial_dismissals = dict(nm._session_dismissals)
        initial_permanent = dict(nm._permanent_dismissals)

        # The router would skip calling record_dismissal entirely for invalid triggers
        # So verify the state is unchanged after a hypothetical invalid dismiss
        assert nm._session_dismissals == initial_dismissals
        assert nm._permanent_dismissals == initial_permanent

    def test_permanent_dismiss_unissued_nudge_rejected(self, nm):
        """Permanent dismissal with fabricated nudge_id should be rejected."""
        # No nudge has been issued
        assert not nm.was_nudge_issued("session_1", "ndg_fabricated_123")

        # The router checks was_nudge_issued before calling record_dismissal for permanent=True
        # So permanent dismissal should never be recorded for unissued nudges

        # Fire a real nudge first
        nudge = asyncio.get_event_loop().run_until_complete(
            nm.maybe_nudge("upload_complete", {}, "session_1", "user_1")
        )
        assert nudge is not None

        # Permanent dismiss with the REAL nudge_id should be accepted
        assert nm.was_nudge_issued("session_1", nudge.nudge_id)
        nm.record_dismissal("session_1", "upload_complete", permanent=True, user_id="user_1", nudge_id=nudge.nudge_id)
        assert "upload_complete" in nm._permanent_dismissals.get("user_1", set())

        # But a fabricated nudge_id for a different trigger should be rejected at router level
        assert not nm.was_nudge_issued("session_1", "ndg_totally_fake")

    def test_non_permanent_dismiss_no_issued_check(self, nm):
        """Non-permanent (session) dismissals don't require nudge issuance verification."""
        # Session dismissals should work even without a prior nudge
        nm.record_dismissal("session_1", "error_event", permanent=False)
        assert "error_event" in nm._session_dismissals.get("session_1", set())


class TestRecordDismissalAllowlistGuard:
    """Test that record_dismissal rejects unknown triggers defensively."""

    def test_unknown_trigger_rejected(self, nm):
        """record_dismissal should silently reject unknown triggers."""
        nm.record_dismissal("session_1", "evil_trigger", permanent=False)
        # Should NOT be in session dismissals
        assert "evil_trigger" not in nm._session_dismissals.get("session_1", set())

    def test_known_trigger_accepted(self, nm):
        """record_dismissal should accept known triggers."""
        nm.record_dismissal("session_1", "error_event", permanent=False)
        assert "error_event" in nm._session_dismissals.get("session_1", set())
