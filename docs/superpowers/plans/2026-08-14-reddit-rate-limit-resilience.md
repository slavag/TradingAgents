# Reddit Rate-Limit Resilience Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Reddit RSS collection tolerate realistic 429 windows without multiplying requests across retries, subreddits, or repeated ticker analyses.

**Architecture:** Keep the public formatter API stable and add three private mechanisms inside `reddit.py`: a bounded retry scheduler, a lock-protected process-wide cooldown, and a five-minute successful-result cache. A private exhaustion exception crosses only the `_fetch_subreddit` boundary so the aggregate sweep can stop while direct low-level callers retain their existing fail-open list result.

**Tech Stack:** Python standard library (`random`, `threading`, `time`, `urllib`), pytest, `unittest.mock`.

---

### Task 1: Exponential retry and shared cooldown

**Files:**
- Modify: `tests/test_reddit_fallback.py:1-170`
- Modify: `tradingagents/dataflows/reddit.py:18-147`

- [ ] **Step 1: Replace the one-retry assertions with failing schedule tests**

Add an autouse state-reset fixture and tests that patch `random.uniform` to `1.0`. Verify that three 429 responses followed by success produce four HTTP attempts and sleeps of `5.0`, `15.0`, and `30.0`; verify four 429 responses exhaust the budget; retain the numeric `Retry-After` assertion.

```python
@pytest.fixture(autouse=True)
def reset_reddit_state():
    reddit._reset_rate_limit_state()
    yield
    reddit._reset_rate_limit_state()


def test_429_retries_with_bounded_exponential_schedule(self):
    err = HTTPError("url", 429, "Too Many Requests", {}, None)
    with patch.object(reddit, "urlopen", side_effect=[err, err, err, _atom_resp()]) as op, \
         patch.object(reddit.random, "uniform", return_value=1.0), \
         patch.object(reddit.time, "sleep") as slept:
        posts = reddit._fetch_subreddit_rss("NVDA", "stocks", 5, 5.0)
    assert op.call_count == 4
    assert [call.args[0] for call in slept.call_args_list] == [5.0, 15.0, 30.0]
    assert len(posts) == 2
```

- [ ] **Step 2: Run the schedule test and confirm RED**

Run: `pytest -q tests/test_reddit_fallback.py::TestRss429Backoff`

Expected: FAIL because the current implementation retries only once and has no shared-state reset helper.

- [ ] **Step 3: Implement the scheduler and cooldown primitives**

Add constants, a lock-protected monotonic cooldown deadline, reset helper, and bounded jitter calculation. Convert `_fetch_subreddit_rss` from recursion to a four-attempt loop.

```python
_RETRY_DELAYS = (5.0, 15.0, 30.0)
_BACKOFF_JITTER = 0.20
_RATE_LIMIT_LOCK = threading.Lock()
_rate_limited_until = 0.0


def _wait_for_rate_limit_cooldown() -> None:
    with _RATE_LIMIT_LOCK:
        remaining = max(0.0, _rate_limited_until - time.monotonic())
    if remaining:
        time.sleep(remaining)
```

For a 429 with retries remaining, choose `Retry-After` when valid; otherwise multiply the scheduled delay by `random.uniform(0.8, 1.2)`. Extend the shared deadline and let the next loop iteration wait on it. On the last 429, extend the cooldown using the final bounded delay and return `[]` unless the internal exhaustion flag is enabled.

- [ ] **Step 4: Run focused tests and confirm GREEN**

Run: `pytest -q tests/test_reddit_fallback.py::TestRss429Backoff`

Expected: all retry/backoff tests pass.

- [ ] **Step 5: Commit the retry layer**

```bash
git add tradingagents/dataflows/reddit.py tests/test_reddit_fallback.py
git commit -m "fix: strengthen Reddit rate-limit backoff"
```

### Task 2: Successful-result cache

**Files:**
- Modify: `tests/test_reddit_fallback.py:94-125`
- Modify: `tradingagents/dataflows/reddit.py:38-210`

- [ ] **Step 1: Write failing cache tests**

Test that two identical `_fetch_subreddit` calls invoke `_fetch_subreddit_rss` once and return independent list/dict copies. Test that empty results are not cached and that advancing monotonic time beyond 300 seconds causes a refetch.

```python
def test_successful_result_is_cached_and_copied(self):
    posts = [{"title": "cached", "source": "rss"}]
    with patch.object(reddit, "_fetch_subreddit_rss", return_value=posts) as rss, \
         patch.object(reddit.time, "monotonic", return_value=100.0):
        first = reddit._fetch_subreddit("NVDA", "stocks", 5, 5.0)
        first[0]["title"] = "mutated"
        second = reddit._fetch_subreddit("NVDA", "stocks", 5, 5.0)
    assert rss.call_count == 1
    assert second[0]["title"] == "cached"
```

- [ ] **Step 2: Run cache tests and confirm RED**

Run: `pytest -q tests/test_reddit_fallback.py -k cache`

Expected: FAIL because `_fetch_subreddit` always calls the RSS helper.

- [ ] **Step 3: Implement the five-minute cache**

Add a cache entry dataclass or tuple, a dedicated lock, copy helpers, and a cache reset invoked by the test reset helper. Cache only non-empty results, keyed by normalized ticker, subreddit, and limit.

```python
_CACHE_TTL_SECONDS = 300.0
_CACHE_LOCK = threading.Lock()
_rss_cache: dict[tuple[str, str, int], tuple[float, list[dict]]] = {}
```

- [ ] **Step 4: Run cache and complete Reddit tests**

Run: `pytest -q tests/test_reddit_fallback.py`

Expected: all Reddit tests pass.

- [ ] **Step 5: Commit the cache**

```bash
git add tradingagents/dataflows/reddit.py tests/test_reddit_fallback.py
git commit -m "feat: cache recent Reddit RSS results"
```

### Task 3: Stop the remaining subreddit sweep

**Files:**
- Modify: `tests/test_reddit_fallback.py:168-245`
- Modify: `tradingagents/dataflows/reddit.py:94-280`

- [ ] **Step 1: Write the failing exhaustion-flow test**

Patch `_fetch_subreddit` to raise the private exhaustion exception on the first subreddit. Assert it is called once even when three subreddits were requested, no inter-request sleep occurs, and the returned text explicitly says Reddit is temporarily rate limited.

```python
def test_rate_limit_exhaustion_stops_remaining_subreddits(self):
    with patch.object(
        reddit,
        "_fetch_subreddit",
        side_effect=reddit._RedditRateLimitExhausted("stocks", "NVDA"),
    ) as fetch, patch.object(reddit.time, "sleep") as slept:
        out = fetch_reddit_posts(
            "NVDA",
            subreddits=("stocks", "investing", "wallstreetbets"),
            inter_request_delay=1.0,
        )
    assert fetch.call_count == 1
    slept.assert_not_called()
    assert "temporarily unavailable due to Reddit rate limiting" in out
```

- [ ] **Step 2: Run the exhaustion test and confirm RED**

Run: `pytest -q tests/test_reddit_fallback.py -k exhaustion_stops`

Expected: FAIL because there is no exhaustion signal and the sweep continues.

- [ ] **Step 3: Implement private exhaustion propagation**

Define `_RedditRateLimitExhausted`. Add an internal keyword-only flag to `_fetch_subreddit_rss`; `_fetch_subreddit` enables it, while direct callers keep the default fail-open behavior. Catch the exception in `fetch_reddit_posts`, append an explicit placeholder, and break before the next subreddit.

- [ ] **Step 4: Run Reddit and sentiment-focused tests**

Run: `pytest -q tests/test_reddit_fallback.py tests/test_structured_agent_prompts.py`

Expected: all tests pass.

- [ ] **Step 5: Commit sweep stopping**

```bash
git add tradingagents/dataflows/reddit.py tests/test_reddit_fallback.py
git commit -m "fix: stop Reddit sweep after rate-limit exhaustion"
```

### Task 4: Full verification and documentation alignment

**Files:**
- Modify if necessary: `tradingagents/dataflows/reddit.py:1-16`
- Verify: `docs/superpowers/specs/2026-08-14-reddit-rate-limit-resilience-design.md`

- [ ] **Step 1: Update the module documentation**

Ensure the top-level docstring describes bounded exponential retries, shared cooldown, sweep stopping, and the short cache rather than the previous one-retry behavior.

- [ ] **Step 2: Run formatting and complete verification**

Run:

```bash
git diff --check
pytest -q
```

Expected: no whitespace errors and the complete suite passes, with only previously known skips/warnings.

- [ ] **Step 3: Inspect scope**

Run:

```bash
git status --short
git diff --stat
git diff -- tradingagents/dataflows/reddit.py tests/test_reddit_fallback.py
```

Expected: Reddit implementation/tests contain only the planned changes; the pre-existing GPT-5.6 catalog/test edits remain separate.

- [ ] **Step 4: Commit final documentation adjustments if any**

```bash
git add tradingagents/dataflows/reddit.py tests/test_reddit_fallback.py
git commit -m "docs: describe resilient Reddit collection"
```
