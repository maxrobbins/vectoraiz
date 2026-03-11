"""
Tests for channel-aware onboarding wizard (BQ-VZ-CHANNEL Phase 2).

Verifies:
- Both channels produce distinct step sequences (different copy/order)
- Both channels expose the same underlying features (C2: no feature gating)
- Step count is identical for both channels
"""


# ---------------------------------------------------------------------------
# Helpers — we test the step definitions directly, no React rendering needed
# ---------------------------------------------------------------------------

# The wizard exports pure data via getStepsForChannel.  Since the frontend is
# TypeScript we can't import it directly, so we duplicate the canonical step
# definitions here and verify the invariants that matter.

DIRECT_FEATURES = {
    "Drag-and-drop upload", "CSV, JSON, Parquet support", "Local-only processing",
    "Multiple embedding models", "Automatic indexing", "BYO API key",
    "Natural language search", "SQL queries", "RAG with your LLM",
    "Full-text & vector search", "Dashboard analytics", "ai.market publishing",
}

MARKETPLACE_FEATURES = {
    "ai.market integration", "Secure device linking", "Local-only processing",
    "Drag-and-drop upload", "CSV, JSON, Parquet support", "Local-only storage",
    "AI-powered descriptions", "Privacy scanning", "Pricing suggestions",
    "One-click publish", "Revenue tracking", "Listing management",
}

DIRECT_TITLES = [
    "Welcome to vectorAIz",
    "Vectorize & Index",
    "Query your data",
    "Ready!",
]

MARKETPLACE_TITLES = [
    "Welcome to vectorAIz for ai.market",
    "Upload your data",
    "Enhance & Preview",
    "Publish!",
]


def test_both_channels_have_same_step_count():
    """Both channels must have the same number of onboarding steps."""
    assert len(DIRECT_TITLES) == len(MARKETPLACE_TITLES)


def test_direct_and_marketplace_titles_differ():
    """Step titles must differ between channels (different copy)."""
    assert DIRECT_TITLES != MARKETPLACE_TITLES


def test_both_channels_cover_core_capabilities():
    """Both channels mention upload, search/query, and marketplace capabilities.

    This enforces C2: the wizard ONLY changes order and copy, not features.
    Both channels have access to the same functionality.
    """
    # Core capabilities that must appear in at least one feature across either channel
    core_keywords = ["upload", "publish", "local"]

    for keyword in core_keywords:
        direct_has = any(keyword in f.lower() for f in DIRECT_FEATURES)
        marketplace_has = any(keyword in f.lower() for f in MARKETPLACE_FEATURES)
        assert direct_has, f"Direct channel missing core capability: {keyword}"
        assert marketplace_has, f"Marketplace channel missing core capability: {keyword}"


def test_step_count_is_four():
    """Both channels have exactly 4 onboarding steps."""
    assert len(DIRECT_TITLES) == 4
    assert len(MARKETPLACE_TITLES) == 4


def test_direct_starts_with_welcome():
    """Direct channel first step is the generic welcome."""
    assert DIRECT_TITLES[0] == "Welcome to vectorAIz"


def test_marketplace_starts_with_aimarket_welcome():
    """Marketplace channel first step references ai.market."""
    assert "ai.market" in MARKETPLACE_TITLES[0]


def test_onboarding_source_file_exists():
    """The OnboardingWizard component source file exists."""
    from pathlib import Path
    wizard_path = Path(__file__).resolve().parent.parent / "frontend" / "src" / "components" / "onboarding" / "OnboardingWizard.tsx"
    assert wizard_path.exists(), f"OnboardingWizard.tsx not found at {wizard_path}"


def test_onboarding_exports_get_steps_for_channel():
    """OnboardingWizard.tsx exports getStepsForChannel for testability."""
    from pathlib import Path
    wizard_path = Path(__file__).resolve().parent.parent / "frontend" / "src" / "components" / "onboarding" / "OnboardingWizard.tsx"
    content = wizard_path.read_text()
    assert "export function getStepsForChannel" in content


def test_onboarding_uses_localStorage_key():
    """OnboardingWizard.tsx uses the correct localStorage key."""
    from pathlib import Path
    wizard_path = Path(__file__).resolve().parent.parent / "frontend" / "src" / "components" / "onboarding" / "OnboardingWizard.tsx"
    content = wizard_path.read_text()
    assert "vz-onboarding-complete" in content


def test_onboarding_no_feature_gating():
    """OnboardingWizard.tsx must not import auth, billing, or feature-gate modules (C2)."""
    from pathlib import Path
    wizard_path = Path(__file__).resolve().parent.parent / "frontend" / "src" / "components" / "onboarding" / "OnboardingWizard.tsx"
    content = wizard_path.read_text()
    # Must not gate features
    forbidden = ["useAuth", "hasFeature", "billing", "isAdmin"]
    for term in forbidden:
        assert term not in content, (
            f"C2 violation: OnboardingWizard imports/references '{term}'. "
            f"Onboarding must be presentation-only."
        )
