"""Shared model catalog for CLI selections and validation."""

from __future__ import annotations

ModelOption = tuple[str, str]
ProviderModeOptions = dict[str, dict[str, list[ModelOption]]]
WebProviderOptions = dict[str, list[ModelOption]]

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


# Older IDs we continue to accept for backwards compatibility, even when they
# are no longer shown as current choices in the CLI/web catalog.
LEGACY_MODEL_IDS = {
    "anthropic": [
        "claude-opus-4-7",
        "claude-opus-4-6",
        "claude-sonnet-4-6",
        "claude-sonnet-4-5",
    ],
    "google": [
        "gemini-3.1-flash-lite-preview",
        "gemini-2.5-flash-lite-preview",
    ],
    "xai": [
        "grok-4-1-fast-reasoning",
        "grok-4-1-fast-non-reasoning",
        "grok-4-0709",
        "grok-4-fast-reasoning",
        "grok-4-fast-non-reasoning",
    ],
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
        ("Qwen 3.7 Plus - Latest, balanced speed/cost", "qwen3.7-plus"),
        ("Qwen 3.6 Plus - Previous-gen balanced", "qwen3.6-plus"),
        ("Custom model ID", "custom"),
    ],
    "deep": [
        ("Qwen 3.7 Max - Latest flagship, most intelligent, 1M ctx", "qwen3.7-max"),
        ("Qwen 3.6 Max - Previous-gen flagship", "qwen3.6-max"),
        ("Qwen 3.7 Plus - Balanced alternative", "qwen3.7-plus"),
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
            ("GPT-5.4 Mini - Fast, strong coding and tool use", "gpt-5.4-mini"),
            ("GPT-5.4 Nano - Cheapest, high-volume tasks", "gpt-5.4-nano"),
            ("GPT-5.4 - Frontier reasoning model", "gpt-5.4"),
            ("GPT-4.1 - Smartest non-reasoning model", "gpt-4.1"),
        ],
        "deep": [
            ("GPT-5.5 - Flagship for complex reasoning and coding", "gpt-5.5"),
            ("GPT-5.5 Pro - Most capable, expensive ($30/$180 per 1M tokens)", "gpt-5.5-pro"),
            ("GPT-5.4 - Affordable frontier reasoning model", "gpt-5.4"),
            ("GPT-5.4 Pro - Most capable for complex workflows", "gpt-5.4-pro"),
            ("GPT-5.2 - Strong reasoning, cost-effective", "gpt-5.2"),
            ("GPT-5.4 Mini - Fast, strong coding and tool use", "gpt-5.4-mini"),
        ],
    },
    "anthropic": {
        "quick": [
            ("Claude Sonnet 5 - Best speed and intelligence balance", "claude-sonnet-5"),
            ("Claude Haiku 4.5 - Fastest with near-frontier intelligence", "claude-haiku-4-5"),
            ("Claude Opus 4.8 - Frontier agentic coding and reasoning", "claude-opus-4-8"),
        ],
        "deep": [
            ("Claude Fable 5 - Most capable, long-running agents", "claude-fable-5"),
            ("Claude Opus 4.8 - Frontier agentic coding and reasoning", "claude-opus-4-8"),
            ("Claude Sonnet 5 - Near-frontier intelligence at Sonnet cost", "claude-sonnet-5"),
            ("Claude Opus 4.7 - Previous frontier, long-running agents", "claude-opus-4-7"),
            ("Claude Haiku 4.5 - Fastest with near-frontier intelligence", "claude-haiku-4-5"),
        ],
    },
    "google": {
        "quick": [
            ("Gemini 3.5 Flash - Stable frontier Flash model", "gemini-3.5-flash"),
            ("Gemini 3.1 Flash-Lite - Frontier-class low-cost model", "gemini-3.1-flash-lite"),
            ("Gemini 2.5 Flash-Lite - Fastest low-cost Gemini", "gemini-2.5-flash-lite"),
            ("Gemini 2.5 Flash - Balanced, stable", "gemini-2.5-flash"),
            ("Gemini 3 Flash Preview - Fast frontier preview", "gemini-3-flash-preview"),
        ],
        "deep": [
            ("Gemini 3.1 Pro Preview - Advanced agentic reasoning", "gemini-3.1-pro-preview"),
            ("Gemini 3.5 Flash - Stable frontier Flash model", "gemini-3.5-flash"),
            ("Gemini 2.5 Pro - Stable advanced reasoning", "gemini-2.5-pro"),
            ("Gemini 2.5 Flash - Balanced, stable", "gemini-2.5-flash"),
            ("Gemini 3 Flash Preview - Fast frontier preview", "gemini-3-flash-preview"),
        ],
    },
    "xai": {
        "quick": [
            ("Grok Build 0.1 - Fast coding and agentic workflows", "grok-build-0.1"),
            ("Grok 4.3 - Current flagship Grok model", "grok-4.3"),
            ("Grok 4.20 (Non-Reasoning) - Speed-optimized", "grok-4.20-0309-non-reasoning"),
        ],
        "deep": [
            ("Grok 4.3 - Current flagship Grok model", "grok-4.3"),
            ("Grok 4.20 (Reasoning) - Previous-gen reasoning", "grok-4.20-0309-reasoning"),
            ("Grok 4.20 Multi-Agent - Multi-agent reasoning", "grok-4.20-multi-agent-0309"),
            ("Grok Build 0.1 - Fast coding and agentic workflows", "grok-build-0.1"),
        ],
    },
    "deepseek": {
        "quick": [
            ("DeepSeek V4 Flash - Latest fast model, thinking + non-thinking", "deepseek-v4-flash"),
            ("Custom model ID", "custom"),
        ],
        "deep": [
            ("DeepSeek V4 Pro - Latest flagship", "deepseek-v4-pro"),
            ("DeepSeek V4 Flash - Fast, supports thinking", "deepseek-v4-flash"),
            ("Custom model ID", "custom"),
        ],
    },
    "qwen": _QWEN_MODELS,
    "qwen-cn": _QWEN_MODELS,
    "glm": _GLM_MODELS,
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
    "mistral": _CUSTOM_ONLY,
    "kimi": _CUSTOM_ONLY,
    "groq": _CUSTOM_ONLY,
    "nvidia": _CUSTOM_ONLY,
    "bedrock": _CUSTOM_ONLY,
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
