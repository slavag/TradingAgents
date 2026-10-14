"""Current curated model choices and three-role compatibility contracts."""

from tradingagents.llm_clients import model_catalog
from tradingagents.llm_clients.api_key_env import PROVIDER_API_KEY_ENV
from tradingagents.llm_clients.capabilities import get_capabilities
from tradingagents.llm_clients.openai_client import _supports_reasoning_effort


def _ids(role: str, provider: str) -> list[str]:
    return [value for _, value in model_catalog.get_role_model_options()[role][provider]]


def test_all_backend_providers_have_all_three_role_options():
    expected = set(PROVIDER_API_KEY_ENV)
    assert set(model_catalog.get_web_model_options()) == expected
    roles = model_catalog.get_role_model_options()
    assert set(roles) == {"quick", "deep", "verifier"}
    assert all(set(providers) == expected for providers in roles.values())


def test_current_role_suggestions_are_exact_documented_ids():
    assert _ids("quick", "openai")[0] == "gpt-6-luna"
    assert _ids("deep", "openai")[0] == "gpt-6.1-sol"
    assert _ids("verifier", "anthropic")[0] == "claude-sonnet-5-5"
    assert "gemini-3.8-flash" in _ids("quick", "google")
    assert "grok-4.7" in _ids("deep", "xai")
    assert "deepseek-flash" in _ids("quick", "deepseek")
    assert "qwen3.8-max" in _ids("deep", "qwen")
    assert _ids("deep", "glm")[0] == "glm-5.3"
    assert _ids("deep", "glm-cn")[0] == "glm-5.2"
    assert "muse-spark-1.3" in _ids("deep", "meta")
    assert not _ids("deep", "meta")[0].endswith("-contributor")


def test_curated_models_have_source_and_lifecycle_metadata():
    getter = getattr(model_catalog, "get_catalog_metadata", None)
    assert callable(getter), "shared catalog must expose source-backed metadata"
    metadata = getter()
    assert set(metadata) == set(PROVIDER_API_KEY_ENV)
    for provider, options in model_catalog.get_web_model_options().items():
        for _, model_id in options:
            if model_id == "custom":
                continue
            item = metadata[provider][model_id]
            assert item["source_url"].startswith("https://")
            assert item["checked_at"] in {"2026-10-13", "2026-10-14"}
            assert item["rationale"]
            assert item["lifecycle"] in {"current", "preview", "legacy"}
    assert metadata["meta"]["muse-wonder-1.0"]["lifecycle"] == "preview"


def test_preview_models_are_never_first_choice_for_any_role():
    metadata = model_catalog.get_catalog_metadata()
    for role, providers in model_catalog.get_role_model_options().items():
        for provider, options in providers.items():
            first_id = options[0][1]
            if first_id != "custom":
                assert metadata[provider][first_id]["lifecycle"] == "current", (role, provider)


def test_meta_wonder_is_selectable_in_every_role_but_never_the_default():
    roles = model_catalog.get_role_model_options()
    for role in ("quick", "deep", "verifier"):
        values = [model_id for _, model_id in roles[role]["meta"]]
        assert values[0] == "muse-spark-1.3"
        assert "muse-wonder-1.0" in values
        assert values[-1] == "custom"
    assert model_catalog.get_catalog_metadata()["meta"]["muse-wonder-1.0"]["lifecycle"] == "preview"
    assert model_catalog.get_catalog_metadata()["meta"]["muse-wonder-1.0"]["checked_at"] == "2026-10-14"


def test_current_specialist_provider_ids_are_selectable_with_role_first_choices():
    roles = model_catalog.get_role_model_options()
    expected = {
        "kimi": ("kimi-k2.6", "kimi-k3", {"kimi-k2.7-code", "kimi-k2.7-code-highspeed"}),
        "nvidia": (
            "nvidia/nemotron-3.5-lightning-30b-a3b",
            "nvidia/nemotron-3-super-120b-a12b",
            set(),
        ),
        "mistral": ("mistral-small-2603", "mistral-medium-3-5", {"mistral-large-4"}),
    }
    for provider, (quick_first, deep_first, extra) in expected.items():
        assert roles["quick"][provider][0][1] == quick_first
        assert roles["deep"][provider][0][1] == deep_first
        for role in ("quick", "deep", "verifier"):
            values = [model_id for _, model_id in roles[role][provider]]
            assert {quick_first, deep_first, *extra} <= set(values)
            assert values[-1] == "custom"
            assert len(values) == len(set(values))


def test_current_google_flash_lite_remains_selectable_without_becoming_first_choice():
    roles = model_catalog.get_role_model_options()
    assert roles["quick"]["google"][0][1] == "gemini-3.8-flash"
    for role in roles:
        assert "gemini-3.1-flash-lite" in [model_id for _, model_id in roles[role]["google"]]


def test_gpt6_and_deepseek_flash_use_supported_parameter_paths():
    ui = model_catalog.get_model_capabilities("openai", "gpt-6.1-sol")
    assert ui["reasoning_effort"] is True
    assert ui["temperature"] is False
    assert _supports_reasoning_effort("gpt-6.1-sol") is True
    assert get_capabilities("deepseek-flash").supports_tool_choice is False


def test_every_provider_has_custom_id_escape_hatch():
    for provider, role_options in model_catalog.get_role_model_options()["quick"].items():
        assert role_options[-1][1] == "custom", provider
