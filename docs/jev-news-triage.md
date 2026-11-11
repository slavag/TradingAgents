# Opt-in Jev ticker-news triage

This feature is separate from the selective upstream v0.7.1 integration.

- Enable it server-side with `TRADINGAGENTS_JEV_NEWS_TRIAGE=true` and `TYPESAFE_API_KEY`. It is off by default. The existing key-triggered live social-post screen is unchanged.
- Triage only ticker-specific Yahoo and Alpha Vantage news in live runs. Global/macro news and historical runs are not sent to Jev.
- Apply each vendor's date and ticker identity checks before sending any title or summary. Do not send URLs, credentials, full reports, or portfolio decisions to Jev.
- Yahoo's ticker feed drops articles explicitly tagged for another symbol; its Search fallback still requires an exact symbol tag. Alpha Vantage's request includes the complete end date; dated, exact-ticker-tagged articles are eligible for Jev, while untagged or undated live articles remain visible and unscreened.
- One bounded `noul` relevance question checks each eligible article, at most 20 articles per retrieval. Each request has one attempt with a five-second timeout; the batch has a ten-second deadline and cancels queued work on failure. Drop only a valid, finite score below 0.10. Keep uncertain scores, untagged Alpha and undated live Yahoo articles, and all articles if Jev fails or returns malformed scores. Show a concise screening status.
- Keep the publication date and source URL for every retained article. Do not let Jev alter investment ratings, targets, source dates, or report evidence.
- Verify with offline vendor and Jev fixtures; do not make a paid or live provider request during implementation.

## Verification

The independent feature review found that undated live Yahoo articles could reach Jev and that one failed article could leave the batch waiting for every retry. Both were corrected with regressions. The final offline gate reported 1,485 Python tests passed, 2 skipped, 16 known model-catalog warnings; Ruff, 15 Node tests, and `git diff --check` passed. No live Yahoo, Alpha Vantage, or Jev call was made.
