# Reddit Rate-Limit Resilience Design

## Objective

Prevent Reddit RSS rate limits from producing repeated immediate failures across
subreddits and concurrent ticker analyses, while keeping Reddit an optional,
fail-open sentiment source.

## Current Problem

The fetcher waits five seconds and retries once for each subreddit. A repeated
429 then returns an empty list, after which the caller proceeds to the next
subreddit. This can issue more requests during the same IP-level rate-limit
window and provides no reuse when the same ticker is analyzed repeatedly.

## Selected Design

### Bounded exponential retry

Each RSS request may make one initial attempt and at most three retries. When
Reddit does not provide `Retry-After`, retry delays follow 5, 15, and 30 seconds
with bounded random jitter. A numeric `Retry-After` value takes precedence and
is capped at 30 seconds. Non-429 failures remain fail-open and are not retried.

### Process-wide cooldown

A lock-protected monotonic deadline records the longest active Reddit cooldown.
Every RSS request checks that deadline before contacting Reddit. A 429 extends
the deadline, allowing concurrent and subsequent requests in this process to
share the same rate-limit signal rather than retry independently.

### Stop the subreddit sweep

After the retry budget is exhausted, the internal per-subreddit fetch signals a
rate-limit exhaustion condition to the aggregate fetcher. The aggregate fetcher
stops requesting remaining subreddits and emits an explicit rate-limited
placeholder. Direct callers of the low-level RSS helper retain the existing
fail-open `[]` contract.

### Short successful-result cache

Non-empty per-subreddit results are cached in process for five minutes by
ticker, subreddit, and requested limit. Cache reads and writes are lock
protected, use monotonic time, and copy post dictionaries so callers cannot
mutate cached state. Failures and empty responses are not cached.

## Compatibility and Limits

- The public `fetch_reddit_posts` signature and ordinary formatted output stay
  unchanged.
- Historical requests continue to avoid both network calls and sleeping.
- Reddit remains optional; rate-limit exhaustion never aborts the analysis.
- State is process-local. Multiple web worker processes do not share cooldowns
  or cache entries.
- The implementation does not introduce OAuth or persistent storage.

## Verification

Unit tests will cover the 5/15/30 retry schedule, jitter bounds, `Retry-After`,
retry exhaustion, shared cooldown, stopping the remaining subreddit sweep,
successful-result caching, failure non-caching, and existing formatting and
historical-data behavior. The complete test suite must remain green.
