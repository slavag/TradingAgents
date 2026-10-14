# Provider-Native Prompt Caching Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax. Execute inline in the approved primary checkout; no subagent, paid live run, or commit is authorized.

**Goal:** Reuse eligible provider-side prompt prefixes without replaying market-sensitive answers, and expose actual cache reads/writes before claiming savings.

**Architecture:** A pure prompt policy receives stable/dynamic text and a provider/model identity. Noneligible calls retain the exact original string shape; eligible repeated native Anthropic calls mark only the stable content block for a 5-minute cache; eligible repeated native OpenAI calls may use a stable non-sensitive key if the installed integration passes it through. Existing Azure/Gemini implicit caching remains provider-managed. A callback records cache token usage and publishes aggregate counts through CLI/web results.

**Tech Stack:** Python 3.13, LangChain chat models/callbacks, Pydantic graph config, pytest, Ruff.

**Approved spec:** `docs/superpowers/specs/2026-10-12-provider-native-prompt-caching-design.md`

---

## File responsibilities

- `cli/stats_handler.py`, `cli/main.py`: cache-read/write/unknown usage accounting and CLI display.
- `tradingagents/web/service.py`: per-ticker aggregate usage in the existing job result JSON.
- `tradingagents/llm_clients/prompt_cache.py`: pure eligibility and request-shaping helper; no local result cache.
- `tradingagents/agents/researchers/{bull,bear}_researcher.py`: stable source context before changing debate suffix.
- `tradingagents/agents/risk_mgmt/{aggressive,conservative,neutral}_debator.py`: same split for multi-round risk discussion.
- `tradingagents/graph/setup.py`, `tradingagents/graph/trading_graph.py`, `tradingagents/default_config.py`: pass a provider/model/reuse policy; default one-round calls remain uncached on Anthropic.
- `tests/test_stats_handler.py`, `tests/test_prompt_cache.py`, focused agent/graph/web tests: RED→GREEN contracts.

### Task 1: Cache usage is observable without a false savings claim

**Files:** Modify `cli/stats_handler.py`, `cli/main.py`, `tradingagents/web/service.py`; create/modify `tests/test_stats_handler.py`, `tests/test_web_service.py`.

- [ ] **Step 1: RED tests.** Feed `StatsCallbackHandler.on_llm_end` three `LLMResult` objects with `AIMessage.usage_metadata`: one has `input_token_details={"cache_read": 120, "cache_creation": 40}`, one has the details with zeros, and one has no cache details. Assert total input/output counts remain the sum of normal tokens, read/write counts are 120/40, reported calls are 2, and unknown calls are 1. Verify concurrent callbacks do not lose counts. In web serialization assert an aggregate usage object contains only numeric/status fields, never prompt text.
- [ ] **Step 2: Run `pytest -q tests/test_stats_handler.py tests/test_web_service.py -k cache_usage` and confirm the missing fields fail.**
- [ ] **Step 3: Add fields and extraction under the existing lock.** Keep current `tokens_in`/`tokens_out` semantics; cache reads/writes are subsets, not extra input. Count `input_token_details` presence separately from zero-valued details.

```python
# StatsCallbackHandler.__init__
self.cache_read_tokens = 0
self.cache_creation_tokens = 0
self.cache_usage_reported_calls = 0
self.cache_usage_unreported_calls = 0

# In on_llm_end, after acquiring self._lock and adding normal tokens:
details = usage_metadata.get("input_token_details")
if isinstance(details, dict) and ("cache_read" in details or "cache_creation" in details):
    self.cache_usage_reported_calls += 1
    self.cache_read_tokens += int(details.get("cache_read") or 0)
    self.cache_creation_tokens += int(details.get("cache_creation") or 0)
else:
    self.cache_usage_unreported_calls += 1

# Include all four fields in get_stats(); do not sum them into tokens_in.
```

- [ ] **Step 4: In CLI stats, display `Cache read: N · cache write: N` only if reported-call count is positive.** If unknown calls exist, append `cache detail unavailable for K calls`; never display an estimated dollar saving.
- [ ] **Step 5: In web `_run_job`, set `result["llm_usage"] = stats_handler.get_stats()` before appending it.** Add `llm_usage` to both completed and failed `serialize_result` branches. Do not include raw request/provider response. Test both paths.
- [ ] **Step 6: Run targeted stats/web tests and scoped Ruff.**

### Task 2: Pure provider policy with exact fallback

**Files:** Create `tradingagents/llm_clients/prompt_cache.py`, `tests/test_prompt_cache.py`.

- [ ] **Step 1: RED tests using a recording fake chat model.** The plain call for OpenAI/Azure/Google/DeepSeek or a single Anthropic round must receive exactly `stable_prefix + dynamic_suffix` as one string and no `cache_control`. A repeated eligible Anthropic call must receive one `HumanMessage` containing two text blocks, with `cache_control={"type":"ephemeral"}` only on the stable block. Unknown model, custom base URL, disabled policy, and too-short prefix use the plain path. Repeated native OpenAI receives a stable hash-based `prompt_cache_key` only when a local payload test proves the installed ChatOpenAI passes it through; no key goes to compatible third parties.
- [ ] **Step 2: Run `pytest -q tests/test_prompt_cache.py` and confirm RED.**
- [ ] **Step 3: Implement `PromptCachePolicy` and `invoke_with_prompt_cache` without a local cache.** The model allowlist/minimums should be a small explicit mapping based on current Anthropic documentation; an unknown model is a no-op. A conservative character threshold is only a preflight filter—the provider's cache usage fields remain the authority on a real hit. Use 5-minute default only; do not use 1-hour TTL.

```python
import hashlib
from dataclasses import dataclass

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

def _documented_anthropic_minimum(model: str) -> int | None:
    return _CLAUDE_MIN_CACHE_TOKENS.get(model.lower())

@dataclass(frozen=True)
class PromptCachePolicy:
    provider: str
    model: str
    expected_reuse: bool = False
    enabled: bool = True
    base_url: str | None = None

def invoke_with_prompt_cache(llm, stable_prefix: str, dynamic_suffix: str,
                             policy: PromptCachePolicy):
    prompt = stable_prefix + dynamic_suffix
    if not policy.enabled or not policy.expected_reuse or not dynamic_suffix:
        return llm.invoke(prompt)
    if policy.provider == "anthropic" and policy.base_url is None:
        minimum = _documented_anthropic_minimum(policy.model)
        if minimum is not None and len(stable_prefix) >= minimum * 4:
            return llm.invoke([HumanMessage(content=[
                {"type": "text", "text": stable_prefix,
                 "cache_control": {"type": "ephemeral"}},
                {"type": "text", "text": dynamic_suffix},
            ])])
    if policy.provider == "openai" and policy.base_url is None:
        cache_key = "ta-" + hashlib.sha256(stable_prefix.encode()).hexdigest()[:24]
        return llm.invoke(prompt, prompt_cache_key=cache_key)
    return llm.invoke(prompt)
```

- [ ] **Step 4: Verify the installed native OpenAI payload for `prompt_cache_key` without network access.** If the installed integration drops or rejects it, keep native OpenAI on the plain string path and record that as a limitation; do not upgrade dependencies as a side effect. Run all helper tests and scoped Ruff.

### Task 3: Repeated research/risk prompts use the policy

**Files:** Modify the five researcher/debator files, `tradingagents/graph/setup.py`, `tradingagents/graph/trading_graph.py`, `tradingagents/default_config.py`; test focused agent, graph, and config modules.

- [ ] **Step 1: RED tests.** For a fake model, run a bull or risk node twice with the same evidence and different history. Assert the stable text is identical and the dynamic text differs; joining the two matches the prior prompt content/order. With `max_debate_rounds=1` or `max_risk_discuss_rounds=1`, assert no Anthropic cache marker. With 2 rounds, an allowlisted model, and sufficient prefix, assert one marker. Assert invalid boolean environment override fails loudly and disabling app-managed Anthropic caching leaves other provider calls unchanged.
- [ ] **Step 2: Run focused tests and confirm RED.**
- [ ] **Step 3: Add `anthropic_prompt_cache` to `DEFAULT_CONFIG` (true) and `_ENV_OVERRIDES` as `TRADINGAGENTS_ANTHROPIC_PROMPT_CACHE`.** It only controls app-managed explicit Anthropic breakpoints; docs must say provider implicit caches are independent.
- [ ] **Step 4: Pass `PromptCachePolicy` from `TradingAgentsGraph` through `GraphSetup` to the five debate factories.** Set `expected_reuse` from their corresponding round count `>1`; keep optional default policy on direct factory calls so existing integrations remain compatible. Do not cache Portfolio Manager, analyst tool responses, or final recommendations in this pass.
- [ ] **Step 5: Split each prompt at its changing history field; keep exact text order.** The stable prefix ends immediately before `Conversation history of the debate:` or `Here is the current conversation history:`; the suffix contains that history, latest opposing arguments, trailing instructions, and language instruction. Replace only `llm.invoke(prompt)` with `invoke_with_prompt_cache(llm, stable_prefix, dynamic_suffix, policy)`.
- [ ] **Step 6: Run focused agent/graph/config tests, then verify decision-integrity and structured-output tests.** Do not broaden to other agents without measured reuse evidence.

### Task 4: Regression, documentation, and limits

**Files:** Modify README/config documentation and existing SDD ledger only after tests establish actual behavior.

- [ ] Run `pytest -q`, scoped `ruff check`, and `git diff --check`.
- [ ] In tests, demonstrate identical plain-text prompt content and no cache parameters on unsupported providers. Confirm CLI/web usage serialization includes read/write/unknown counts and no prompt text.
- [ ] Document native implicit versus app-managed explicit caching, the default one-round no-op, 5-minute retention, provider eligibility, and why cache tokens are not dollar savings. Do not claim live savings without cache-read/write usage from a repeated real call.
- [ ] Do not run a paid API validation or SMXT. Note that live cost reduction remains unverified without a user-requested repeated Anthropic/OpenAI run. Do not commit/push/PR without separate authorization.
