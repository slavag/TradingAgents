"""Provider-native prompt-prefix hints; never a response or market-data cache."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import HumanMessage

_CLAUDE_MIN_CACHE_TOKENS = {
    "claude-sonnet-4-5": 1024,
    "claude-sonnet-4-6": 1024,
    "claude-opus-4-5": 4096,
    "claude-opus-4-6": 4096,
    "claude-opus-4-7": 2048,
    "claude-opus-4-8": 1024,
    "claude-opus-5": 512,
    "claude-fable-5": 512,
    "claude-sonnet-5": 1024,
    "claude-sonnet-5-5": 512,
    "claude-haiku-4-5": 4096,
    "claude-haiku-5-5": 512,
}


@dataclass(frozen=True)
class PromptCachePolicy:
    provider: str
    model: str
    expected_reuse: bool = False
    enabled: bool = True
    base_url: str | None = None


def invoke_with_prompt_cache(
    llm: Any, stable_prefix: str, dynamic_suffix: str, policy: PromptCachePolicy
) -> Any:
    """Mark an eligible reusable prefix while preserving the plain-call fallback."""
    prompt = stable_prefix + dynamic_suffix
    if not policy.enabled or not policy.expected_reuse or not dynamic_suffix:
        return llm.invoke(prompt)

    provider = policy.provider.lower()
    if provider == "anthropic" and policy.base_url is None:
        minimum = _CLAUDE_MIN_CACHE_TOKENS.get(policy.model.lower())
        if minimum is not None and len(stable_prefix) >= minimum * 4:
            return llm.invoke([HumanMessage(content=[
                {"type": "text", "text": stable_prefix,
                 "cache_control": {"type": "ephemeral"}},
                {"type": "text", "text": dynamic_suffix},
            ])])
    if provider == "openai" and policy.base_url is None:
        key = "ta-" + hashlib.sha256(stable_prefix.encode("utf-8")).hexdigest()[:24]
        return llm.invoke(prompt, prompt_cache_key=key)
    return llm.invoke(prompt)
