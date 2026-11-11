# Selective upstream integration through v0.7.1

## Intent and baseline

Bring compatible upstream TradingAgents changes from the releases after the fork's v0.4.0 backport through `v0.7.1` into this customized fork. The target is upstream tag `v0.7.1` at commit `c4544913a14ed3df5f1ed7b9e8483270ce20682a`, not an unbounded merge of upstream `main`. The current fork `main` is `e7cb81a1cacec4dd18993dd441ff730490b16591` and has substantial uncommitted AMD investor-report and Discover work. Do not reset, overwrite, or silently exclude that work.

The existing local `codex/upstream-v0.6.0-integration` branch (`ee531c3`) completed a selective v0.6 integration with its own verification. Current `main` does not contain that branch in Git ancestry, but its later research workspace and report stack overlap some of the same behavior. Treat v0.6 as **previously integrated work to reconcile**, not work to blindly repeat or a branch to merge wholesale. The upstream comparison spans roughly 258 commits after the fork's common ancestor, so file-level conflict resolution alone is not a reliable measure of behavioral integration.

## Scope and preservation rules

The user chose **all missing releases through v0.7.1**, selectively integrated. For each release-note change and material upstream commit, record one of: already provided by the fork, adopted as-is, adapted to fork architecture, superseded, or intentionally deferred with a reason. Preserve the fork's distinct `Abstain`, `Unavailable`, and `REVIEW` states; point-in-time evidence rules; investment-decision validation; per-ticker investor/full reports and batch consolidated reports; prompt-cache and provider configuration boundaries; local-only web settings; Discover; and checkpoint/stream lifecycle. Never replace these with a thinner upstream implementation merely to make a merge clean.

Start an isolated `codex/` branch/worktree from the current committed fork tip. Copy the current tracked and untracked in-progress AMD/Discover changes into that worktree as a reversible snapshot, verifying file counts and hashes before integration. Leave the primary checkout untouched. No commit, push, PR, or merge into `main` occurs without a separate explicit user approval. Do not run paid model calls or live market-provider acceptance tests without the user's action.

## Integration stages

1. **Audit the already-integrated v0.4.1–v0.6.0 behavior against present `main`.** Use the verified v0.6 integration branch and each release diff as evidence. Mark behavior already present in the current fork as complete; only port a tested gap. Relevant areas include data-window and vendor hardening, SEC as-filed statements, portfolio input, noninteractive CLI support, safe caches, analyst parallelism and all-ticker settlement behind opt-in flags, report provenance, HTML output, and symbol/news/fundamental fixes. Keep the fork's current report generators and web workflow; do not adopt upstream's package reorganization wholesale.
2. **Integrate v0.7.0 selectively.** Prioritize first-close outcome scoring and settlement semantics, plus compatible model/provider and packaging fixes. The upstream ChatGPT-plan provider is opt-in: no login or token import occurs merely because a model is selected or the app starts. Review its credential storage, CLI/web boundaries, and license/dependency implications before exposing it. Keep the upstream background release/update check **off by default** in this fork unless separately approved; no silent network contact with a release server. Existing settled outcomes are not rewritten, and a mixed-old/new backtest is labeled rather than treated as directly comparable.
3. **Integrate v0.7.1 fixes.** Cover unpadded model-date normalization before SEC queries, completed-close and long-holiday settlement, backtest identity/final-pass errors, finite portfolio values and broker aliases, significant-digit prices, period-aligned SEC statements and sub-million figures, same-folder report replacement, invalid date-window correction, and applicable Azure/Claude/provider fixes. Adapt each to fork-specific modules and add focused regressions for the original failure mode.

The release's packaging/dependency changes are adopted only where required by an included feature and verified against the fork's supported Python/environment matrix. A release-note feature that is incompatible with the fork is documented as a deliberate omission, not silently marked complete.

Jev follows upstream v0.7.1's key-gated behavior by the user's 2026-11-08 instruction: `TYPESAFE_API_KEY` alone enables screening of newly fetched live social posts; the older `TRADINGAGENTS_JEV_SCREENING` flag is not required. Preserve the fork's historical safety boundary: historical social feeds do not provide archived posts and never send current posts to Jev. No sign-in, vendor call, or post export occurs merely from app startup.

## Verification and handoff

Before each stage, capture the isolated workspace baseline and an upstream-disposition ledger. The current dirty checkout's first `pytest -q` run on 2026-11-07 had seven pre-existing Discover market-evidence test failures because fixed October 15 quote fixtures are now stale under the live clock; freeze that test clock without weakening production freshness rules before judging integration regressions. For accepted changes, prove a regression fails before implementation and passes after it where feasible. Run focused tests, then full `pytest -q`, `ruff check .`, relevant Node/web tests, `git diff --check`, package/import smoke, and conflict-marker checks. Verify point-in-time and decision/output behavior on offline fixtures. Do not trigger live LLM, provider, or broker calls for QA. Report any optional dependency skip, warning, unsupported provider, and untested live path explicitly.

The handoff is a reviewable isolated diff with a concise ledger mapping each release theme to implemented, already-present, superseded, or deferred status; test evidence; and a clear separation from the preserved AMD/Discover snapshot. The work remains uncommitted until the user requests a specific commit action, and `main` remains unchanged until a separately approved integration decision.

## Source references

- [v0.5.2 release](https://github.com/TauricResearch/TradingAgents/releases/tag/v0.5.2)
- [v0.6.0 release](https://github.com/TauricResearch/TradingAgents/releases/tag/v0.6.0)
- [v0.7.0 release](https://github.com/TauricResearch/TradingAgents/releases/tag/v0.7.0)
- [v0.7.1 release](https://github.com/TauricResearch/TradingAgents/releases/tag/v0.7.1)
