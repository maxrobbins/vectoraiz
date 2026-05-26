import importlib

import pytest


@pytest.fixture
def reload_config():
    import app.config

    yield lambda: importlib.reload(app.config)


@pytest.mark.parametrize(
    "env_name,field_name,test_value",
    [
        ("VECTORAIZ_KEYSTORE_PASSPHRASE", "keystore_passphrase", "test-vec-keystore"),
        ("AIM_DATA_KEYSTORE_PASSPHRASE", "keystore_passphrase", "test-aim-keystore"),
        ("VECTORAIZ_AI_MARKET_URL", "ai_market_url", "https://vec.example/api"),
        ("AIM_DATA_AI_MARKET_URL", "ai_market_url", "https://aim.example/api"),
        ("VECTORAIZ_INTERNAL_API_KEY", "internal_api_key", "k-vec-1"),
        ("AIM_DATA_INTERNAL_API_KEY", "internal_api_key", "k-aim-1"),
        ("VECTORAIZ_SERIAL", "serial", "VZ-001"),
        ("AIM_DATA_SERIAL", "serial", "AD-001"),
        ("VECTORAIZ_APIKEY_HMAC_SECRET", "apikey_hmac_secret", "hmac-vec"),
        ("AIM_DATA_APIKEY_HMAC_SECRET", "apikey_hmac_secret", "hmac-aim"),
    ],
)
def test_field_reads_from_both_prefixes(monkeypatch, reload_config, env_name, field_name, test_value):
    for pref in ("VECTORAIZ_", "AIM_DATA_"):
        monkeypatch.delenv(pref + field_name.upper(), raising=False)
    monkeypatch.setenv(env_name, test_value)
    mod = reload_config()
    assert getattr(mod.settings, field_name) == test_value


def test_vectoraiz_prefix_wins_when_both_set(monkeypatch, reload_config):
    monkeypatch.setenv("VECTORAIZ_KEYSTORE_PASSPHRASE", "vec-wins")
    monkeypatch.setenv("AIM_DATA_KEYSTORE_PASSPHRASE", "aim-loses")
    mod = reload_config()
    assert mod.settings.keystore_passphrase == "vec-wins"


def test_boolean_fields_accept_either_prefix(monkeypatch, reload_config):
    monkeypatch.setenv("AIM_DATA_CONNECTIVITY_ENABLED", "true")
    monkeypatch.setenv("AIM_DATA_AUTH_ENABLED", "false")
    monkeypatch.setenv("AIM_DATA_ALLAI_ENABLED", "true")
    mod = reload_config()
    assert mod.settings.connectivity_enabled is True
    assert mod.settings.auth_enabled is False
    assert mod.settings.allai_enabled is True
