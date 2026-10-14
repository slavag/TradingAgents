"""Shared model catalog for CLI selections and validation."""

from __future__ import annotations

import re
from typing import TypedDict

ModelOption = tuple[str, str]
ProviderModeOptions = dict[str, dict[str, list[ModelOption]]]
WebProviderOptions = dict[str, list[ModelOption]]
RoleWebOptions = dict[str, WebProviderOptions]


class ModelUICapabilities(TypedDict):
    temperature: bool
    reasoning_effort: bool
    thinking_level: bool
    anthropic_effort: bool

OPENAI_MODEL_ALIASES = {
    "gpt-5-mini": "gpt-5.4-mini",
    "gpt-5-nano": "gpt-5.4-nano",
}

# Providers that serve many / frequently-changing models: offer only "Custom
# model ID" rather than a list that goes stale.
_CUSTOM_ONLY: dict[str, list[ModelOption]] = {
    "quick": [("Custom model ID", "custom")],
    "deep": [("Custom model ID", "custom")],
}

_VERIFIER_PREFERRED = {
    "openai": ("gpt-6.1-sol", "gpt-6-luna", "gpt-6-astra"),
    "anthropic": ("claude-sonnet-5-5", "claude-haiku-5-5", "claude-opus-5-5"),
    "google": ("gemini-3.8-flash", "gemini-3.6-flash", "gemini-3.1-pro-preview"),
    "xai": ("grok-4.7", "grok-4.3"),
    "meta": ("muse-spark-1.3", "muse-spark-1.2"),
}


# Older IDs we continue to accept for backwards compatibility, even when they
# are no longer shown as current choices in the CLI/web catalog.
LEGACY_MODEL_IDS = {
    "openai": [
        "gpt-5.4-mini", "gpt-5.4-nano", "gpt-5.4", "gpt-5.4-pro", "gpt-5.2",
        "gpt-5.6-luna", "gpt-5.6-terra", "gpt-5.6-sol", "gpt-5.5",
        "gpt-5.5-pro", "gpt-4.1",
    ],
    "anthropic": [
        "claude-fable-5",
        "claude-opus-5",
        "claude-sonnet-5",
        "claude-haiku-4-5",
        "claude-opus-4-8",
        "claude-opus-4-7",
        "claude-opus-4-6",
        "claude-sonnet-4-6",
        "claude-sonnet-4-5",
    ],
    "google": [
        "gemini-3.5-flash",
        "gemini-3.5-flash-lite",
        "gemini-3.1-flash-lite", "gemini-2.5-flash-lite", "gemini-2.5-flash",
        "gemini-2.5-pro", "gemini-3-flash-preview",
        "gemini-3.1-flash-lite-preview",
        "gemini-2.5-flash-lite-preview",
    ],
    "xai": [
        "grok-4.5",
        "grok-4.3",
        "grok-build-0.1", "grok-4.20-0309-non-reasoning",
        "grok-4.20-0309-reasoning", "grok-4.20-multi-agent-0309",
        "grok-4-1-fast-reasoning",
        "grok-4-1-fast-non-reasoning",
        "grok-4-0709",
        "grok-4-fast-reasoning",
        "grok-4-fast-non-reasoning",
    ],
    "meta": ["muse-spark-1.2", "muse-spark-1.2-contributor", "muse-spark-1.1"],
    "deepseek": ["deepseek-v4-flash"],
}


# Shared model list for GLM via Z.AI (international) and BigModel (China).
# Source: docs.z.ai (GLM Coding Plan supported models + LLM guides).
# All GLM 4.7+ entries support thinking mode via thinking={"type":"enabled"}.
_GLM_MODELS: dict[str, list[ModelOption]] = {
    "quick": [
        ("GLM-5-Turbo - Fast, switchable thinking modes", "glm-5-turbo"),
        ("GLM-4.7 - Previous-gen flagship", "glm-4.7"),
        ("GLM-4.5-Air - Lightweight, cost-efficient", "glm-4.5-air"),
        ("Custom model ID", "custom"),
    ],
    "deep": [
        ("GLM-5.2 - Latest flagship, 1M ctx", "glm-5.2"),
        ("GLM-5.1 - 745B, 200K ctx", "glm-5.1"),
        ("GLM-5 - Flagship, 204K ctx", "glm-5"),
        ("GLM-4.7 - Previous-gen flagship", "glm-4.7"),
        ("Custom model ID", "custom"),
    ],
}


# Shared model list for Qwen's global (dashscope-intl) and CN (dashscope)
# endpoints. Only versioned IDs are exposed in the dropdown; version-less
# aliases can still be entered through "Custom model ID" where supported.
_QWEN_MODELS: dict[str, list[ModelOption]] = {
    "quick": [
        ("Qwen 3.8 Flash - Lower-latency tool-enabled analysis", "qwen3.8-flash"),
        ("Qwen 3.7 Plus - Balanced alternative", "qwen3.7-plus"),
        ("Custom model ID", "custom"),
    ],
    "deep": [
        ("Qwen 3.8 Max - Complex reasoning and tool use", "qwen3.8-max"),
        ("Qwen 3.7 Max - Earlier high-capability option", "qwen3.7-max"),
        ("Qwen 3.8 Flash - Faster alternative", "qwen3.8-flash"),
        ("Custom model ID", "custom"),
    ],
}


# Shared model list for MiniMax's global and CN endpoints (same IDs).
_MINIMAX_MODELS: dict[str, list[ModelOption]] = {
    "quick": [
        ("MiniMax-M3 - Latest, 1M ctx, native multimodal", "MiniMax-M3"),
        ("MiniMax-M2.7-highspeed - Fast M2.7, 204K ctx, ~100 TPS", "MiniMax-M2.7-highspeed"),
        ("MiniMax-M2.5-highspeed - Previous-gen highspeed, 204K ctx", "MiniMax-M2.5-highspeed"),
        ("Custom model ID", "custom"),
    ],
    "deep": [
        ("MiniMax-M3 - Latest flagship, 1M ctx, multimodal coding/agent", "MiniMax-M3"),
        ("MiniMax-M2.7 - Previous flagship, 204K ctx", "MiniMax-M2.7"),
        ("MiniMax-M2.7-highspeed - Same quality as M2.7, ~100 TPS", "MiniMax-M2.7-highspeed"),
        ("MiniMax-M2.5 - Earlier flagship, 204K ctx", "MiniMax-M2.5"),
        ("Custom model ID", "custom"),
    ],
}


MODEL_OPTIONS: ProviderModeOptions = {
    "openai": {
        "quick": [
            ("GPT-6 Luna - Quick analysis and lower cost", "gpt-6-luna"),
            ("GPT-6.1 Sol - Higher-capability alternative", "gpt-6.1-sol"),
        ],
        "deep": [
            ("GPT-6.1 Sol - Deep reasoning and tool use", "gpt-6.1-sol"),
            ("GPT-6 Astra - Highest-capability alternative", "gpt-6-astra"),
            ("GPT-6 Luna - Lower-cost alternative", "gpt-6-luna"),
        ],
    },
    "anthropic": {
        "quick": [
            ("Claude Haiku 5.5 - Fast tool-enabled analysis", "claude-haiku-5-5"),
            ("Claude Sonnet 5.5 - Higher-capability alternative", "claude-sonnet-5-5"),
        ],
        "deep": [
            ("Claude Opus 5.5 - Deep reasoning", "claude-opus-5-5"),
            ("Claude Fable 5.1 - Long-context reasoning", "claude-fable-5-1"),
            ("Claude Sonnet 5.5 - Balanced alternative", "claude-sonnet-5-5"),
        ],
    },
    "google": {
        "quick": [
            ("Gemini 3.8 Flash - Quick reasoning and tools", "gemini-3.8-flash"),
            ("Gemini 3.6 Flash - Balanced alternative", "gemini-3.6-flash"),
            ("Gemini 3.5 Flash-Lite - Lower-cost alternative", "gemini-3.5-flash-lite"),
        ],
        "deep": [
            ("Gemini 3.8 Flash - Deep reasoning and tools", "gemini-3.8-flash"),
            ("Gemini 3.1 Pro Preview - High-capability preview", "gemini-3.1-pro-preview"),
            ("Gemini 3.6 Flash - Established alternative", "gemini-3.6-flash"),
        ],
    },
    "xai": {
        "quick": [
            ("Grok 4.7 - Current general analysis", "grok-4.7"),
            ("Grok 4.3 - Earlier lower-cost option", "grok-4.3"),
        ],
        "deep": [
            ("Grok 4.7 - Current deep reasoning", "grok-4.7"),
            ("Grok 4.5 - Earlier alternative", "grok-4.5"),
        ],
    },
    "meta": {
        "quick": [
            ("Muse Spark 1.3 - Standard tier, quick analysis", "muse-spark-1.3"),
        ],
        "deep": [
            ("Muse Spark 1.3 - Standard tier, deep reasoning", "muse-spark-1.3"),
            ("Muse Wonder 1.0 - Preview; verify account access", "muse-wonder-1.0"),
        ],
    },
    "deepseek": {
        "quick": [
            ("DeepSeek Flash - Current quick analysis", "deepseek-flash"),
            ("Custom model ID", "custom"),
        ],
        "deep": [
            ("DeepSeek V4 Pro - Deep reasoning", "deepseek-v4-pro"),
            ("DeepSeek Flash - Faster alternative", "deepseek-flash"),
            ("Custom model ID", "custom"),
        ],
    },
    "qwen": _QWEN_MODELS,
    "qwen-cn": _QWEN_MODELS,
    "glm": {
        "quick": list(_GLM_MODELS["quick"]),
        "deep": [("GLM-5.3 - Text reasoning and tool use", "glm-5.3"), *_GLM_MODELS["deep"]],
    },
    "glm-cn": _GLM_MODELS,
    "minimax": _MINIMAX_MODELS,
    "minimax-cn": _MINIMAX_MODELS,
    "openrouter": {
        "quick": [
            ("NVIDIA Nemotron 3 Nano 30B (free)", "nvidia/nemotron-3-nano-30b-a3b:free"),
            ("Z.AI GLM 4.5 Air (free)", "z-ai/glm-4.5-air:free"),
        ],
        "deep": [
            ("Z.AI GLM 4.5 Air (free)", "z-ai/glm-4.5-air:free"),
            ("NVIDIA Nemotron 3 Nano 30B (free)", "nvidia/nemotron-3-nano-30b-a3b:free"),
        ],
    },
    "ollama": {
        "quick": [
            ("Qwen3:latest (8B)", "qwen3:latest"),
            ("GPT-OSS:latest (20B)", "gpt-oss:latest"),
            ("GLM-4.7-Flash:latest (30B)", "glm-4.7-flash:latest"),
            ("Custom model ID", "custom"),
        ],
        "deep": [
            ("GLM-4.7-Flash:latest (30B)", "glm-4.7-flash:latest"),
            ("GPT-OSS:latest (20B)", "gpt-oss:latest"),
            ("Qwen3:latest (8B)", "qwen3:latest"),
            ("Custom model ID", "custom"),
        ],
    },
    "openai_compatible": _CUSTOM_ONLY,
    "mistral": {
        "quick": [("Mistral Small 4 - Tool-enabled analysis", "mistral-small-2603")],
        "deep": [
            ("Mistral Small 4 - Reasoning and tools", "mistral-small-2603"),
            ("Mistral Large 4 - Preview", "mistral-large-4"),
        ],
    },
    "kimi": _CUSTOM_ONLY,
    "groq": {
        "quick": [("GPT-OSS 20B - Fast hosted reasoning", "openai/gpt-oss-20b")],
        "deep": [("GPT-OSS 120B - Hosted reasoning and tools", "openai/gpt-oss-120b")],
    },
    "nvidia": _CUSTOM_ONLY,
    "bedrock": _CUSTOM_ONLY,
    "azure": _CUSTOM_ONLY,
}


# The tuples remain the compatibility surface for CLI, web, and validators.
# Every provider also offers an exact custom/deployment ID escape hatch.
for _modes in MODEL_OPTIONS.values():
    for _mode in ("quick", "deep"):
        if not any(value == "custom" for _, value in _modes[_mode]):
            _modes[_mode].append(("Custom model ID", "custom"))

_OFFICIAL_CATALOG_SOURCES = {
    "openai": "https://developers.openai.com/api/docs/models",
    "anthropic": "https://platform.claude.com/docs/en/models/overview",
    "google": "https://ai.google.dev/gemini-api/docs/models",
    "xai": "https://docs.x.ai/developers/models",
    "meta": "https://dev.meta.ai/docs/models",
    "deepseek": "https://api-docs.deepseek.com/quick_start/pricing/",
    "qwen": "https://help.aliyun.com/zh/model-studio/model-list-text-generation/",
    "qwen-cn": "https://help.aliyun.com/zh/model-studio/model-list-text-generation/",
    "glm": "https://docs.z.ai/guides/llm/glm-5.3",
    "glm-cn": "https://docs.z.ai/guides/llm/glm-5",
    "minimax": "https://platform.minimax.io/docs/guides/models-intro",
    "minimax-cn": "https://platform.minimax.io/docs/guides/models-intro",
    "openrouter": "https://openrouter.ai/docs/api/api-reference/models/get-models",
    "ollama": "https://docs.ollama.com/api/tags",
    "mistral": "https://docs.mistral.ai/inference",
    "groq": "https://console.groq.com/docs/models",
}

_PREVIEW_MODEL_IDS = {"muse-wonder-1.0", "gemini-3.1-pro-preview", "mistral-large-4"}


def get_catalog_metadata() -> dict[str, dict[str, dict[str, str]]]:
    """Give every curated choice its lifecycle, official source, and role rationale."""
    return {
        provider: {
            value: {
                "lifecycle": "preview" if value in _PREVIEW_MODEL_IDS else "current",
                "source_url": _OFFICIAL_CATALOG_SOURCES[provider],
                "checked_at": "2026-10-13",
                "rationale": label.split(" - ", 1)[-1],
            }
            for label, value in get_web_model_options()[provider]
            if value != "custom"
        }
        for provider in MODEL_OPTIONS
    }


def get_model_options(provider: str, mode: str) -> list[ModelOption]:
    """Return shared model options for a provider and selection mode."""
    return MODEL_OPTIONS[provider.lower()][mode]


def get_web_model_options() -> WebProviderOptions:
    """Build a de-duplicated provider -> options mapping for the web UI."""
    payload: WebProviderOptions = {}

    for provider, mode_options in MODEL_OPTIONS.items():
        seen: set[str] = set()
        merged: list[ModelOption] = []

        for mode in ("quick", "deep"):
            for label, value in mode_options[mode]:
                if value in seen:
                    continue
                seen.add(value)
                merged.append((label, value))

        payload[provider] = merged

    return payload


def get_role_model_options() -> RoleWebOptions:
    """Return provider options independently curated for each runtime role."""
    roles: RoleWebOptions = {"quick": {}, "deep": {}, "verifier": {}}
    for provider, mode_options in MODEL_OPTIONS.items():
        roles["quick"][provider] = list(mode_options["quick"])
        roles["deep"][provider] = list(mode_options["deep"])
        merged = []
        seen = set()
        for option in (*mode_options["deep"], *mode_options["quick"]):
            if option[1] not in seen:
                seen.add(option[1])
                merged.append(option)
        preferred = _VERIFIER_PREFERRED.get(provider)
        if preferred:
            by_id = {value: (label, value) for label, value in merged}
            verifier = [by_id[value] for value in preferred if value in by_id]
            verifier.extend(option for option in merged if option[1] == "custom")
            roles["verifier"][provider] = verifier
        else:
            roles["verifier"][provider] = list(mode_options["deep"])
    return roles


def get_model_capabilities(provider: str, model: str) -> ModelUICapabilities:
    """Describe which user-facing controls are valid for one provider/model."""
    provider = provider.strip().lower()
    model_lower = model.strip().lower()
    openai_reasoning = provider == "openai" and bool(
        re.match(r"^(?:gpt-[56]|o[134](?:-|$))", model_lower)
    )
    google_thinking = provider == "google" and model_lower.startswith("gemini-")
    no_google_temperature = provider == "google" and model_lower in {
        "gemini-3.6-flash",
        "gemini-3.5-flash-lite",
    }
    anthropic_effort = provider == "anthropic" and any(
        family in model_lower for family in ("opus", "sonnet", "fable")
    )
    return {
        "temperature": not openai_reasoning and not no_google_temperature,
        "reasoning_effort": openai_reasoning,
        "thinking_level": google_thinking,
        "anthropic_effort": anthropic_effort,
    }


def get_web_model_capabilities() -> dict[str, dict[str, ModelUICapabilities]]:
    """Return capabilities for every model visible in any role."""
    return {
        provider: {
            model: get_model_capabilities(provider, model)
            for _, model in options
        }
        for provider, options in get_web_model_options().items()
    }


def get_known_models() -> dict[str, list[str]]:
    """Build known model names from the shared CLI catalog."""
    return {
        provider: sorted(
            {
                value
                for options in mode_options.values()
                for _, value in options
            }.union(LEGACY_MODEL_IDS.get(provider, []))
        )
        for provider, mode_options in MODEL_OPTIONS.items()
    }


def normalize_model_name(provider: str, model: str) -> str:
    """Return the canonical model name for a provider-specific alias."""
    if provider.lower() == "openai":
        return OPENAI_MODEL_ALIASES.get(model, model)
    return model
