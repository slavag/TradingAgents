# Provider-Native Prompt Caching With Measured Savings

## Goal and scope

Reduce repeated LLM input work in TradingAgents without replaying a stale investment decision. Preserve every prompt's substantive instructions, evidence, tool access, and decision-integrity checks. Cache only provider-side prompt prefixes, never complete model answers or market observations in a local result cache. No guaranteed savings are claimed for a one-off request.

This is separate from the investor-deck final-slide change. The current checkout uses LangChain chat clients for native Anthropic, native OpenAI, Azure OpenAI, Google Gemini, Bedrock, and several OpenAI-compatible third-party/local services. Its default research/risk debate counts are one round. Its `StatsCallbackHandler` records aggregate input/output tokens but discards cache-read/write details. The installed LangChain Anthropic/OpenAI integrations expose cache-related request/usage fields; implementations must be tested against these installed versions, not only newer documentation.

## Selected approach and alternatives

Use a small provider-aware prompt helper for repeated debate calls and expand token telemetry. It receives an identical stable prefix (role instructions plus that run's source context) and a changing suffix (conversation history and latest rebuttal). It concatenates the same text for all providers so prompt meaning and ordering do not change. For Anthropic only, when a supported model and a graph configuration predict at least two uses of that exact prefix within the short cache window, send the stable prefix as a separate text content block with one 5-minute `cache_control: ephemeral` breakpoint. Never mark a one-use dynamic suffix for caching. If the preconditions fail, call the model exactly as before. Keep the requested model's normal safety/structured-output behavior.

Native OpenAI and Azure OpenAI prompt caching and eligible Gemini implicit caching are already provider features. Preserve/reuse stable prefixes and read provider-reported cache tokens; do not send a blanket `cache_control` or `prompt_cache_key` to OpenAI-compatible third parties. A native OpenAI/Azure cache key may be used only if the installed integration supports it, a repeated prefix is demonstrated, and its value is a stable non-sensitive role/prefix identifier. Google explicit cache objects and Bedrock explicit cache points are out of scope for this first pass because they have separate lifecycles/model constraints (and the Bedrock extra is absent locally). Unsupported providers continue unchanged with cache metrics marked unavailable, not zero savings.

Two alternatives were rejected: forcing an Anthropic breakpoint on every call can increase cost when the prompt is never reused (the app defaults to one debate round); local response memoization can replay an obsolete decision even when market data or evidence changes. A broader shared dossier cached across different agent roles would reorder instruction/evidence trust boundaries and needs a separate design and regression effort.

## Provider and cost rules

- Anthropic: only eligible native Claude clients receive an explicit 5-minute breakpoint, and only when a repeated invocation with the same prefix is expected. The prefix must meet the model's documented minimum cacheable length. Unknown models, one-round roles, too-short prefixes, custom base URLs, and unsupported integration versions remain uncached. Do not choose a 1-hour TTL by default; its cache-write cost is higher.
- OpenAI/Azure: retain provider implicit caching. Preserve prefix ordering in repeated calls, but avoid adding explicit breakpoints unless the installed SDK/model pair is verified separately. Do not rely on a cache key to make nonidentical prefixes match.
- Gemini: retain eligible provider implicit caching and collect read usage when exposed. Do not create persistent explicit cached-content objects or storage charges in this pass.
- Bedrock and other OpenAI-compatible endpoints: no vendor-specific cache parameters without a verified capability and contract. The helper calls them with the original string prompt.

Official behavior references: [OpenAI prompt caching](https://developers.openai.com/api/docs/guides/prompt-caching), [Anthropic prompt caching](https://platform.claude.com/docs/en/build-with-claude/prompt-caching), [Azure OpenAI prompt caching](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/prompt-caching), [Gemini optimization](https://ai.google.dev/gemini-api/docs/optimization), and [Bedrock prompt caching](https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-caching.html).

## Components and data flow

1. Add one helper under `tradingagents/llm_clients/` that accepts `(llm, stable_prefix, dynamic_suffix, expected_reuse)` and selects the Anthropic content-block shape only under the rules above. The plain path invokes `llm` with the byte-equivalent concatenated prompt string. No module-global cache or hidden state is introduced.
2. Split the existing bull/bear researcher and three risk-debator prompt constructions at the changing debate-history boundary. Keep role/safety instructions before evidence and preserve the exact textual order. Pass a reuse hint from graph configuration; `max_debate_rounds`/`max_risk_discuss_rounds` of one disables explicit Anthropic cache writes. A later task may target additional high-reuse call sites only after measured evidence.
3. Extend `StatsCallbackHandler` to aggregate `cache_read_tokens` and `cache_creation_tokens` from `AIMessage.usage_metadata.input_token_details`, without double-counting them as extra input. Preserve existing `tokens_in`/`tokens_out` semantics. Track how many responses reported cache detail versus omitted it, so a mixed-provider run does not present an unknown as zero. Surface these fields in CLI stats and the web job's existing read-only status/result payload when available. Do not log prompt text, cache keys, API credentials, or raw provider responses.
4. Treat missing usage fields as unavailable for a provider/run. Do not turn `0 cached tokens` into a claim of cost savings. Report tokens, not a dollar estimate, because rates differ across models, providers, tiers, and cache-write durations.

## Failure and privacy behavior

Cache configuration is an optimization, never a correctness dependency. Unsupported models and short or one-use prefixes use the original request shape. Provider validation errors caused by caching should be surfaced with a clear provider/model context; do not silently repeat a request that may already have completed. A short-lived provider-side cache changes retention relative to an uncached Anthropic call, but nothing is persisted locally by this feature. App-managed Anthropic caching can be disabled with a configuration switch; provider-native implicit caching is controlled by the provider and must not be mislabeled as disabled by that switch.

## Verification

Use TDD with stub chat models to prove exact prompt-content equivalence, Anthropic-only breakpoint placement, single-use and unsupported-provider no-op behavior, and stable source/evidence ordering. Test usage extraction for Anthropic/OpenAI/Gemini metadata variants, missing fields, and concurrent callbacks. Test CLI/web serialization without exposing prompt text. Run full pytest, scoped Ruff, and `git diff --check`. A live repeated-call validation should compare provider-reported cache reads/writes before making a cost claim; no API spend or SMXT generation is required for deterministic tests. Do not commit, push, or open a PR without separate authorization.
