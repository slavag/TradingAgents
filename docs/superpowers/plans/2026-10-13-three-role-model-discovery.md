# Three-Role Model Catalog and Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give all supported providers current, source-backed model choices for the existing quick, deep, and verifier roles, plus safe, explicit account-model discovery in web and CLI.

**Architecture:** Keep the shared curated catalog as the only source of recommendations and compatibility options. Add a small server-side discovery module that reads existing credential mappings, calls fixed listing endpoints, normalizes bounded results, and returns non-secret status; web and CLI consume that service without changing a configured model. This is one integrated plan because the catalog, capability gates, and two selectors share IDs and selection rules.

**Tech Stack:** Python 3.13, FastAPI, Requests, pytest, Ruff, vanilla JavaScript/HTML/CSS.

**Spec:** `docs/superpowers/specs/2026-10-13-three-role-model-discovery-design.md`

## Global Constraints

- Exactly three existing runtime roles: `quick`, `deep`, and `verifier`; no per-agent configuration.
- Text/chat-capable IDs only; every curated ID carries `current`, `preview`, or `legacy`, an official source URL, and checked date `2026-10-13`.
- Keep `get_model_options`, `get_role_model_options`, `get_web_model_options`, `get_known_models`, and `get_model_capabilities` compatible with their present callers.
- Never replace explicit, environment, configured-fallback, or promoted model selections during catalog load or refresh; retain undiscovered choices as unverified.
- Discovery is user-initiated and read-only; no inference call, page-load request, batch-job request, browser-supplied URL, secret, prompt, or raw provider error in response/logs.
- Hosted discovery uses fixed HTTPS endpoints and no redirects; Ollama listing is permitted only on loopback. Azure, Bedrock, generic compatible, remote Ollama, and unverified provider listing APIs use curated/custom choices.
- Keep all existing work and changes in the dirty primary checkout. No paid model calls, SMXT generation, commit, push, or PR without separate authorization.

## Review Focus

- A saved model absent from new recommendations or account discovery remains selected and clearly marked unverified; pin in Tasks 1 and 4.
- An unknown provider, browser-supplied URL, foreign Origin, or redirect cannot turn discovery into a credential-bearing SSRF; pin in Tasks 2 and 3.
- A valid provider response containing image/audio/embedding models cannot make them selectable as recommended chat models; pin in Task 2.
- Missing credentials, 401, timeout, oversized/malformed JSON, and pagination loops leave curated/custom options usable without exposing exception bodies; pin in Task 2.
- A refresh of one provider/role cannot mutate another role or silently change the selected model while a prior refresh resolves late; pin in Task 4.

---

### Task 1: Source-backed catalog, role ranking, and capability parity

**Files:**
- Modify: `tradingagents/llm_clients/model_catalog.py`
- Modify: `tradingagents/llm_clients/capabilities.py`
- Modify: `tradingagents/llm_clients/openai_client.py` (reasoning-model parameter gate)
- Modify: `tradingagents/llm_clients/prompt_cache.py` (verified Anthropic cache thresholds)
- Modify: `tradingagents/llm_clients/validators.py` only if a new current ID is rejected by strict validation
- Test: `tests/test_web_model_catalog.py`, `tests/test_model_validation.py`, `tests/test_capabilities.py`, `tests/test_provider_registry.py`, `tests/test_prompt_cache.py`

**Interfaces:**
- Preserve the existing tuple-returning functions and `MODEL_OPTIONS` for legacy callers.
- Produce `get_catalog_metadata() -> dict[str, dict[str, dict[str, str]]]` mapping provider → model ID → `{lifecycle, source_url, checked_at, rationale}` and `get_role_model_options() -> RoleWebOptions` with independently ordered quick/deep/verifier options. Metadata is display-only; validation still uses exact IDs.

- [ ] **Step 1: Write failing catalog and parity tests.** Add tests that assert `set(get_web_model_options()) == set(PROVIDER_API_KEY_ENV)`, each role covers the same keys, each non-custom curated ID has all metadata keys and a provider-owned official HTTPS source, no recommended ID is `legacy`, and the top choices include documented current examples: OpenAI `gpt-6.1-sol`/`gpt-6-luna`, Anthropic `claude-sonnet-5-5`/`claude-opus-5-5`, Google `gemini-3.8-flash`/`gemini-3.1-pro-preview`, xAI `grok-4.7`, DeepSeek `deepseek-flash`/`deepseek-v4-pro`, Meta `muse-spark-1.3`, Qwen `qwen3.8-flash`/`qwen3.8-max`, MiniMax `MiniMax-M3`. Assert Contributor Muse IDs never appear in first-choice recommendations and `muse-wonder-1.0` is preview. Assert GPT-6 reasoning parameters are routed without unsupported temperature and that new Claude IDs only receive prompt-cache breakpoints after their official token thresholds are verified. Update old tests that incorrectly require withdrawn models as current; retain their explicitly configured/legacy acceptance tests.

```python
def test_catalog_metadata_and_provider_parity():
    assert set(get_web_model_options()) == set(PROVIDER_API_KEY_ENV)
    for role, providers in get_role_model_options().items():
        assert role in {"quick", "deep", "verifier"}
        assert set(providers) == set(PROVIDER_API_KEY_ENV)
    for provider, models in get_catalog_metadata().items():
        for model_id, item in models.items():
            assert model_id and item["lifecycle"] in {"current", "preview", "legacy"}
            assert item["source_url"].startswith("https://")
            assert item["checked_at"] == "2026-10-13"
            assert item["rationale"]
```

- [ ] **Step 2: Run the RED tests.** Run `pytest -q tests/test_web_model_catalog.py tests/test_model_validation.py tests/test_capabilities.py tests/test_provider_registry.py tests/test_prompt_cache.py`. Expected: new metadata/parity/current-ID assertions fail against the old catalog.
- [ ] **Step 3: Implement the catalog and preserve compatibility.** Use one typed record per curated ID, with role ranks and support notes, and derive the existing tuple lists from it. Promote only documented, text/chat and tool/structured-output-suitable IDs; keep older IDs accepted in `LEGACY_MODEL_IDS`. Give each supported key, including `azure`, `bedrock`, `openai_compatible`, `qwen-cn`, `glm-cn`, `minimax-cn`, `mistral`, `kimi`, `groq`, and `nvidia`, an explicit catalog entry (a labeled custom/deployment entry is valid where stable account-independent IDs cannot be verified). Use the official pages named in the spec and provider documentation already linked in the current catalog; do not infer IDs from release names or add Contributor/preview models as default choices. Remove hard-coded price claims from labels. Update the OpenAI GPT-6 UI reasoning detection in `model_catalog.py`, the `_OPENAI_REASONING_MODEL` gate in `openai_client.py`, and any per-model parameter gates changed by new IDs. Add new Anthropic IDs to `_CLAUDE_MIN_CACHE_TOKENS` only when official minimums are confirmed; otherwise keep them on the safe no-breakpoint path. Keep old named IDs accepted for persisted runs. Do not change `TradingAgentsGraph` or default configuration values.

```python
@dataclass(frozen=True)
class CatalogModel:
    model_id: str
    label: str
    lifecycle: Literal["current", "preview", "legacy"]
    source_url: str
    checked_at: str
    rationale: str
    roles: tuple[str, ...]

def get_catalog_metadata() -> dict[str, dict[str, dict[str, str]]]:
    return {
        provider: {
            entry.model_id: {
                "lifecycle": entry.lifecycle,
                "source_url": entry.source_url,
                "checked_at": entry.checked_at,
                "rationale": entry.rationale,
            }
            for entry in entries if entry.model_id != "custom"
        }
        for provider, entries in CATALOG.items()
    }
```

- [ ] **Step 4: Run GREEN tests.** Run the same five test files. Expected: all pass, including no silent rewrite of old IDs, GPT-6 parameter parity, safe prompt-cache fallback, and verifier ordering.

### Task 2: Bounded, fixed-endpoint discovery service

**Files:**
- Create: `tradingagents/llm_clients/model_discovery.py`
- Test: `tests/test_model_discovery.py`

**Interfaces:**
- Produce `DiscoveryResult` with `provider: str`, `status: Literal["fresh", "cached", "unavailable", "unsupported"]`, `models: tuple[str, ...]`, `fetched_at: str | None`, `warning: str | None`.
- Produce `discover_models(provider: str, *, force: bool = False) -> DiscoveryResult`; reject unknown keys, never accept a URL argument, and never return credential or raw response text. `force=True` means user-triggered refresh; the short cache may still coalesce an in-flight identical request.

- [ ] **Step 1: Write failing service tests with stubbed Requests.** Parameterize OpenAI, Anthropic, Google, xAI, DeepSeek, Meta, OpenRouter, Mistral, Groq, and loopback Ollama schemas. Assert exact IDs, only chat/text modalities, stable deduplication, bounded count, timestamps, and cache hit. Add fixed-host/no-redirect assertions, unknown/unsupported provider, non-loopback `OLLAMA_BASE_URL`, missing key, 401, timeout, oversize body, malformed payload, and cyclic pagination. Assert all failures produce known warning codes, no raw exception body/key, and no call to `chat.completions`, `messages.create`, or analogous inference methods.

```python
def test_missing_key_uses_curated_fallback_without_network(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    result = discover_models("openai", force=True)
    assert result.status == "unavailable"
    assert result.warning == "missing_credentials"
    assert result.models == ()

def test_redirect_never_replays_authorization(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    response = MagicMock(status_code=302, headers={"Location": "https://evil.invalid/models"})
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    request = Mock(return_value=response)
    monkeypatch.setattr(requests, "get", request)
    result = discover_models("openai", force=True)
    assert result.warning == "provider_unavailable"
    assert request.call_count == 1
    assert request.call_args.kwargs["allow_redirects"] is False
```

- [ ] **Step 2: Run RED tests.** Run `pytest -q tests/test_model_discovery.py`. Expected: import/function failures.
- [ ] **Step 3: Implement fixed adapter registry and response envelopes.** Use `PROVIDER_API_KEY_ENV` for credential lookup and literal endpoint constants from official API docs: OpenAI `/v1/models`, Anthropic `/v1/models`, Gemini `models.list`, xAI `/v1/language-models`, DeepSeek `/models`, Meta `/v1/models`, OpenRouter `/api/v1/models`, Mistral `/v1/models`, Groq `/openai/v1/models`, and local Ollama `/api/tags`. Use provider-specific auth headers (including `x-goog-api-key` for Gemini); never a browser URL. Prefer low-level `requests.get` with `(3, 7)` connect/read timeouts, `allow_redirects=False`, `stream=True`, and a 2 MiB byte cap before JSON decoding. Bound accepted IDs to 250, length to 200 characters, pages to 5; cache only successful normalized results for 5 minutes, keyed by provider plus in-memory SHA-256 credential fingerprint so account changes cannot reuse the wrong list. Do not log request URLs, headers, fingerprints, response bodies, or exception text; log only provider and warning code. For Anthropic/Google documented pagination, stop at cap or repeated cursor. xAI's language-model listing is preferred to generic multimodal listing. Unsupported providers return `unsupported` with no network call. Treat unknown OpenRouter candidates as selectable, not recommended. For Ollama, parse/validate host with `urllib.parse`, accept only `localhost`, `127.0.0.1`, or `[::1]` on the fixed API port, and never accept redirects. Filter using explicit provider metadata where possible; when metadata is insufficient, exclude known non-chat ID families conservatively and label resulting IDs merely available/unverified.

```python
@dataclass(frozen=True)
class DiscoveryResult:
    provider: str
    status: Literal["fresh", "cached", "unavailable", "unsupported"]
    models: tuple[str, ...]
    fetched_at: str | None
    warning: str | None

def discover_models(provider: str, *, force: bool = False) -> DiscoveryResult:
    key = provider.strip().lower()
    if key not in PROVIDER_API_KEY_ENV:
        raise ValueError("unknown_provider")
    adapter = DISCOVERY_ADAPTERS.get(key)
    if adapter is None:
        return DiscoveryResult(key, "unsupported", (), None, "listing_not_supported")
    return adapter.list_models(force=force)
```

`DISCOVERY_ADAPTERS` is a literal provider-key → `ListingAdapter` registry; `ListingAdapter.list_models(*, force: bool) -> DiscoveryResult` owns that provider's fixed endpoint, auth, schema parsing, cache, and bounded warnings. No `base_url` or `endpoint` parameter exists on the public service.

- [ ] **Step 4: Run GREEN tests.** Run `pytest -q tests/test_model_discovery.py`; expected all tests pass with no live network or inference calls.

### Task 3: Local-only web discovery route and complete provider list

**Files:**
- Modify: `tradingagents/web/app.py`
- Modify: `tradingagents/web/static/index.html`
- Test: `tests/test_web_model_discovery.py`, `tests/test_web_model_catalog.py`

**Interfaces:**
- Consume `discover_models(provider, force=True)` and return only `provider`, `status`, `models`, `fetched_at`, and bounded `warning` at `GET /api/models/{provider}/refresh` with `Cache-Control: no-store`.
- Preserve `_require_local_settings_request` as the local-access gate. Static HTML exposes every backend provider key for each of the three roles and a refresh control/status per role.

- [ ] **Step 1: Write failing route and markup tests.** With `TestClient(app)`, patch `discover_models` to return a known result; assert local request succeeds, cross-origin/remote request fails 403 before discovery, unknown provider fails 400, and no raw error/secret appears. Assert each key in `PROVIDER_API_KEY_ENV` occurs three times as provider options and each role has a refresh button/status. Check no discovery request occurs when rendering `/`.

```python
def test_refresh_is_local_only_and_no_store(monkeypatch):
    monkeypatch.setattr(web_app, "discover_models", lambda provider, force: DiscoveryResult(
        provider, "fresh", ("gpt-6.1-sol",), "2026-10-13T12:00:00Z", None
    ))
    response = TestClient(web_app.app, base_url="http://localhost").get("/api/models/openai/refresh")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["models"] == ["gpt-6.1-sol"]
```

- [ ] **Step 2: Run RED tests.** Run `pytest -q tests/test_web_model_discovery.py tests/test_web_model_catalog.py`. Expected: route/markup/parity tests fail.
- [ ] **Step 3: Implement route and markup.** Call `_require_local_settings_request(request)` before any lookup, validate against `PROVIDER_API_KEY_ENV`, then call `discover_models`. Use the existing static page and role-card structure; add missing provider keys, including both regional variants and Azure/Bedrock/generic compatible, without removing current options. Use buttons of type `button`, accessible per-role status text, and concise copy explaining curated/custom fallback. Protect the endpoint with its existing local-origin guard, never accept an endpoint from query/body, and return fixed warning codes only.

```python
@app.get("/api/models/{provider}/refresh")
def refresh_available_models(provider: str, request: Request):
    _require_local_settings_request(request)
    if provider not in PROVIDER_API_KEY_ENV:
        raise HTTPException(status_code=400, detail="Unknown model provider.")
    result = discover_models(provider, force=True)
    return JSONResponse(asdict(result), headers={"Cache-Control": "no-store"})
```

- [ ] **Step 4: Run GREEN tests.** Run both test files; expected route security, provider parity, and markup tests pass.

### Task 4: Web merge, selection preservation, and safe refresh interaction

**Files:**
- Create: `tradingagents/web/static/model_picker.js`
- Modify: `tradingagents/web/static/app.js`
- Modify: `tradingagents/web/static/index.html` (load `model_picker.js` before `app.js`)
- Modify: `tradingagents/web/static/styles.css` (only if new controls require layout adjustments)
- Modify: `tradingagents/web/app.py` (inject `get_catalog_metadata()` into the page)
- Test: `tests/test_model_picker_js.mjs`, `tests/test_web_model_discovery.py`, `tests/test_web_model_catalog.py`

**Interfaces:**
- Consume `window.TRADINGAGENTS_CATALOG_METADATA`, `ROLE_MODEL_OPTIONS`, and the Task 3 endpoint. Keep the existing `roleBindings`, `applyRoleDefaults`, `updateProviderFields`, and submission payload contracts.
- Produce `mergeModelChoices(role, provider, selectedValue, discoveredIds, roleOptions)` and `isCurrentRefresh(expectedRole, expectedProvider, expectedToken, currentRole, currentProvider, currentToken)` as pure JS helpers in `model_picker.js`, exported via `module.exports` for Node tests and `window.TradingAgentsModelPicker` for the browser: curated recommendation order first, then account-available IDs, then a retained unverified selected ID; never assign `select.value` to a different model during refresh. In `app.js`, `refreshRoleModels(role)` is the click handler that uses these helpers.

- [ ] **Step 1: Write failing JS and page tests.** Add `tests/test_model_picker_js.mjs` using Node's built-in `node:test` and `assert/strict`; load `model_picker.js` with `createRequire(import.meta.url)` and test the pure merge and stale-request token helper without a DOM dependency. Assert a legacy/custom selected value remains in the returned options, discovered IDs are appended and labeled `Available in this account`, empty discovery keeps curated/custom, another role's input is not mutated, and an old token is rejected after provider switch. In the Python page test, assert markup has role-scoped buttons and `GET /` injects catalog metadata; inspect `app.js` for `escapeHtml(label)` in option creation.

```javascript
import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { mergeModelChoices } = require('../tradingagents/web/static/model_picker.js');
test('a configured model survives an empty account list', () => {
  const result = mergeModelChoices('quick', 'openai', 'saved/old', [], {
    quick: { openai: [['Current', 'gpt-6-luna'], ['Custom model ID', 'custom']] }
  });
  assert.equal(result.at(-1)[1], 'saved/old');
});
```

- [ ] **Step 2: Run RED tests.** Run `node --test tests/test_model_picker_js.mjs` and `pytest -q tests/test_web_model_discovery.py tests/test_web_model_catalog.py`. Expected: new helper/import/page tests fail.
- [ ] **Step 3: Implement pure merge and role-scoped refresh.** Change `populateModelSelect` so absent preferred values are inserted as `Configured model (unverified)` instead of falling back to the first option; `custom` selection keeps its explicit typed ID. Maintain discovered lists keyed by provider, not a global mutable menu; associate each request with its role/provider and a monotonically increasing token, ignore stale responses, disable only that refresh button while pending, and show freshness/warning without raw errors. Inject metadata JSON using the same escaping-safe template pattern as existing model JSON. Bind buttons only after `applyRoleDefaults`; do not fetch at load or analysis start. Recompute capability controls for the unchanged selected value with `get_model_capabilities`-consistent fallback, not the first recommended ID.

```javascript
function mergeModelChoices(role, provider, selectedValue, discoveredIds, roleOptions) {
  const curated = roleOptions[role]?.[provider] || [];
  const seen = new Set(curated.map(([, id]) => id));
  const merged = [...curated];
  for (const id of discoveredIds) {
    if (!seen.has(id)) { merged.push([`${id} · Available in this account`, id]); seen.add(id); }
  }
  if (selectedValue && !seen.has(selectedValue)) {
    merged.push([`${selectedValue} · Configured model (unverified)`, selectedValue]);
  }
  return merged;
}

function isCurrentRefresh(expectedRole, expectedProvider, expectedToken,
                          currentRole, currentProvider, currentToken) {
  return expectedRole === currentRole && expectedProvider === currentProvider
    && expectedToken === currentToken;
}

if (typeof module !== "undefined" && module.exports) {
  module.exports = { mergeModelChoices, isCurrentRefresh };
}
if (typeof window !== "undefined") {
  window.TradingAgentsModelPicker = { mergeModelChoices, isCurrentRefresh };
}
```

- [ ] **Step 4: Run GREEN JS and page tests.** Run `node --test tests/test_model_picker_js.mjs` and both Python test files. Expected: all pass, including late-response and cross-role isolation. Also inspect the rendered app once at desktop and narrow viewport with a mocked discovery response; no provider key or paid run required.

### Task 5: CLI explicit refresh and OpenRouter migration

**Files:**
- Modify: `cli/utils.py`
- Test: `tests/test_openrouter_model_select.py`, `tests/test_cli_model_discovery.py`

**Interfaces:**
- Consume `discover_models(provider, force=True)` and shared `get_model_options`/`get_role_model_options` metadata.
- Keep `select_shallow_thinking_agent`, `select_deep_thinking_agent`, `_select_model`, and custom ID return contracts. Replace automatic OpenRouter fetch in `select_openrouter_model` with the same explicit refresh action as other providers; preserve mainstream filtering only as a display convenience, not a hidden validity rule.

- [ ] **Step 1: Write failing CLI tests.** Assert `_select_model` makes no network request before a user chooses `Refresh available models`; a refresh invokes `discover_models` only for the selected provider; credentials/errors are never printed; failed refresh returns to curated/custom choices; custom ID and cancellation keep prior behavior. Rewrite the existing OpenRouter newest-first test to test `model_discovery` normalization and the explicit-refresh menu instead of unconditional `_fetch_openrouter_models`.

```python
def test_no_discovery_until_explicit_refresh(monkeypatch):
    discover = Mock(side_effect=AssertionError("unexpected network"))
    monkeypatch.setattr(utils, "discover_models", discover)
    monkeypatch.setattr(utils.questionary, "select", lambda *a, **k: Mock(ask=lambda: "custom"))
    monkeypatch.setattr(utils.questionary, "text", lambda *a, **k: Mock(ask=lambda: "my/model"))
    assert utils._select_model("openrouter", "quick") == "my/model"
    discover.assert_not_called()
```

- [ ] **Step 2: Run RED tests.** Run `pytest -q tests/test_openrouter_model_select.py tests/test_cli_model_discovery.py`. Expected: explicit-refresh tests fail against auto-fetching CLI.
- [ ] **Step 3: Implement one selection loop.** Start from curated role options (or custom-only if no independently verified list), append `Refresh available models` as an action, call the shared service only when chosen, merge account IDs beneath curated choices, show a bounded warning and timestamp, then re-prompt. Define `merge_curated_and_available(provider: str, mode: str, models: tuple[str, ...]) -> list[questionary.Choice]` in `cli/utils.py`; it preserves curated order, deduplicates exact IDs, labels appended IDs as available/unverified, and retains custom choice. Preserve custom entry, user-selected exact ID, cancel behavior, and mode labels; never persist a recommendation over a configured/default model. Do not import Requests directly into CLI for model listing after migration.

```python
while True:
    choice = questionary.select(label, choices=choices).ask()
    if choice is None:
        raise SystemExit(1)
    if choice == "refresh_models":
        result = discover_models(provider, force=True)
        choices = merge_curated_and_available(provider, mode, result.models)
        continue
    return _prompt_custom_model_id() if choice == "custom" else choice
```

```python
def merge_curated_and_available(provider: str, mode: str, models: tuple[str, ...]):
    curated = get_model_options(provider, mode)
    seen = {model_id for _, model_id in curated}
    merged = [questionary.Choice(label, value=model_id) for label, model_id in curated]
    merged.extend(questionary.Choice(f"{model_id} (available; unverified)", value=model_id)
                  for model_id in models if model_id not in seen)
    return merged
```

- [ ] **Step 4: Run GREEN CLI tests.** Re-run the two test files; expected all pass, including existing custom/cancel behavior.

### Task 6: Documentation, security regression, and full offline verification

**Files:**
- Modify: `README.md` or the existing provider-setup documentation section linked from the web form
- Test: `tests/test_model_discovery.py`, `tests/test_web_model_discovery.py`, `tests/test_cli_model_discovery.py`

**Interfaces:** No new runtime API. Document which providers support listing, how `Refresh available models` works, why some IDs are merely available/unverified, and why Contributor/preview models are not defaults.

- [ ] **Step 1: Add failure-path regression tests.** Assert an HTTP 302/303/307 does not follow to a foreign host, a loopback-looking hostname such as `localhost.evil.invalid` is refused, the Google query-key never appears in exception/log/JSON text, a second account key does not reuse the first account's cached list, unsupported providers perform zero network calls, and a model-list payload with 10,000 entries is capped before UI use. Use only stubbed responses.
- [ ] **Step 2: Run RED tests.** Run `pytest -q tests/test_model_discovery.py tests/test_web_model_discovery.py tests/test_cli_model_discovery.py`; expected: any newly added edge-case test fails until the owning implementation is tightened.
- [ ] **Step 3: Fix the exact failing boundaries and document the UX.** Update only the relevant adapter/route/selector code for a failing test; add a short operator-facing provider-discovery table, server-side credential note, refresh/cache behavior, and unsupported-provider custom-ID guidance in existing docs. No automatic discovery or default-model migration.
- [ ] **Step 4: Run verification.** Run `pytest -q`; `ruff check tradingagents/llm_clients tradingagents/web cli tests/test_model_discovery.py tests/test_web_model_discovery.py tests/test_cli_model_discovery.py`; `git diff --check`; and `node --test tests/test_model_picker_js.mjs`. Record exact pass/fail counts and inspect `git diff --stat` plus `git status --short` to separate pre-existing edits from this feature. Do not run paid model inference, PENG/SMXT analysis, or a live credentialed discovery request.

## Execution notes

Before editing shared files, inspect the dirty diff and preserve unrelated investor-deck, SEC, and prompt-cache work. Tasks 1–3 establish interfaces; Tasks 4–5 depend on them and should be implemented serially in this checkout. Test examples above are acceptance anchors; when an existing test fixture uses different import conventions, adapt the fixture without weakening the assertion. The user approved the spec, not a commit: leave implementation uncommitted unless separately requested.
