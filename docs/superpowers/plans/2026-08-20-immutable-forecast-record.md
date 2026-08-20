# Immutable Forecast Record Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist every completed Portfolio Manager outcome as an immutable,
content-addressed forecast record with explicit provenance, missingness, and
point-in-time semantics that later scoring and portfolio projects can consume.

**Architecture:** Introduce a forecasting domain package that owns frozen
Pydantic schemas and canonical hashing. Preserve the typed Portfolio Manager
decision in graph state, then let the shared report writer construct and persist
the record from that snapshot plus run metadata. Legacy states without the typed
snapshot use a conservative label parser and record missing fields explicitly.

**Tech Stack:** Python 3.10+, Pydantic v2, `hashlib`, canonical JSON, pytest,
existing shared report-tree writer.

**Spec:** `docs/superpowers/specs/2026-08-14-decision-integrity-foundation-design.md`

## Global Constraints

- Records are frozen and use tuples or frozen child models for nested values.
- Record IDs are SHA-256 hashes of canonical payload JSON and exclude the ID itself.
- Datetimes must be timezone-aware; data cutoffs retain date-level precision when that is all the run knows.
- Missing market, probability, benchmark, prompt, or vendor information stays absent and is named in `missing_fields`.
- Existing target confidence remains an uncalibrated evidence-strength score; it never becomes a probability.
- A point target is not promoted into a p10/p50/p90 distribution or target range.
- No new LLM or market-data request is introduced.
- Future Projects B-E remain out of scope.

---

### Task 1: Frozen Forecast Domain Schema

**Files:**
- Create: `tradingagents/forecasting/__init__.py`
- Create: `tradingagents/forecasting/schemas.py`
- Create: `tests/test_forecast_record.py`

**Interfaces:**
- Produces: `ForecastDecisionStatus`, `AdjustmentBasis`, `DataQuality`,
  `ReferencePriceSnapshot`, `DirectionProbabilities`, `ForecastDistribution`,
  `ModelIdentity`, `ForecastProvenance`, `ForecastRecordPayload`, and
  `ForecastRecord`.
- Contract: all models use `ConfigDict(frozen=True)`; collection fields are
  tuples; UTC offsets are mandatory; probabilities sum to one; distributions
  satisfy `p10 <= p50 <= p90`; target bounds are both absent or both present.

- [x] **Step 1: Write failing schema tests**

Add tests that prove mutation is rejected, naive datetimes fail, probability
triples must sum to one, quantiles must be ordered, target bounds are
all-or-nothing, and non-actionable records cannot carry a rating.

- [x] **Step 2: Run the schema tests and confirm RED**

Run: `python -m pytest tests/test_forecast_record.py -k "schema or immutable" -q`

Expected: collection fails because `tradingagents.forecasting.schemas` does not
exist.

- [x] **Step 3: Implement the frozen schema**

Use decimal-valued financial fields and explicit validation. The payload must
contain:

```python
class ForecastRecordPayload(BaseModel):
    model_config = ConfigDict(frozen=True)
    schema_version: Literal[1] = 1
    canonical_symbol: str
    quote_currency: str | None
    asset_type: str
    as_of: datetime
    data_cutoff: date
    reference_price: ReferencePriceSnapshot | None
    horizon_text: str | None
    horizon_sessions: int | None
    expected_return: Decimal | None
    expected_excess_return: Decimal | None
    distribution: ForecastDistribution | None
    direction_probabilities: DirectionProbabilities | None
    central_target: Decimal | None
    target_low: Decimal | None
    target_high: Decimal | None
    invalidation_conditions: tuple[str, ...]
    evidence_ids: tuple[str, ...]
    missing_fields: tuple[str, ...]
    data_quality: DataQuality
    data_quality_notes: tuple[str, ...]
    provenance: ForecastProvenance
    decision_status: ForecastDecisionStatus
    rating: str | None
    status_reason: str | None
    target_validation_status: str
    target_validation_reason: str | None
    recommendation_confidence_score: int | None
    target_confidence_score: int | None
```

`ForecastRecord` extends the payload with `record_id: str` matching
`sha256:<64 lowercase hex characters>`.

- [x] **Step 4: Run schema tests and Ruff**

Run: `python -m pytest tests/test_forecast_record.py -k "schema or immutable" -q`

Run: `python -m ruff check tradingagents/forecasting tests/test_forecast_record.py`

Expected: all selected tests and static checks pass.

- [x] **Step 5: Commit the schema boundary**

```bash
git add -- tradingagents/forecasting/__init__.py tradingagents/forecasting/schemas.py tests/test_forecast_record.py
git commit -m "feat: define immutable forecast records"
```

---

### Task 2: Canonical Hashing and Deterministic Factory

**Files:**
- Create: `tradingagents/forecasting/record_factory.py`
- Modify: `tests/test_forecast_record.py`

**Interfaces:**
- Consumes: `ForecastRecordPayload`.
- Produces: `canonical_payload_json(payload) -> str`,
  `forecast_record_id(payload) -> str`,
  `create_forecast_record(payload) -> ForecastRecord`, and
  `normalize_horizon_sessions(text) -> int | None`.
- Contract: equivalent payloads always serialize and hash identically;
  different payloads produce different IDs; exact single-value day/week/month/
  year horizons normalize to 1/5/21/252 trading sessions; ranges remain unknown.

- [x] **Step 1: Write failing hash and horizon tests**

Use two independently constructed equal payloads and literal expected canonical
JSON ordering. Assert equal hashes, a changed evidence ID changes the hash, and
`3 months` becomes 63 sessions while `3-6 months` remains `None`.

- [x] **Step 2: Run factory tests and confirm RED**

Run: `python -m pytest tests/test_forecast_record.py -k "hash or canonical or horizon" -q`

Expected: import failure because the factory functions do not exist.

- [x] **Step 3: Implement canonical creation**

Serialize `payload.model_dump(mode="json")` through `json.dumps` with
`sort_keys=True`, `separators=(",", ":")`, and `ensure_ascii=False`; hash the
UTF-8 bytes. Construct the record only from the validated payload dump plus the
computed ID.

- [x] **Step 4: Run factory and schema tests**

Run: `python -m pytest tests/test_forecast_record.py -q`

Expected: all record tests pass.

- [x] **Step 5: Commit the factory boundary**

```bash
git add -- tradingagents/forecasting/record_factory.py tests/test_forecast_record.py
git commit -m "feat: create content-addressed forecast records"
```

---

### Task 3: Decision Snapshot and State-to-Record Mapping

**Files:**
- Modify: `tradingagents/agents/utils/agent_states.py`
- Modify: `tradingagents/agents/managers/portfolio_manager.py`
- Modify: `tradingagents/forecasting/record_factory.py`
- Modify: `tests/test_forecast_record.py`
- Modify: `tests/test_portfolio_manager_integrity.py`

**Interfaces:**
- Produces in graph state: `portfolio_decision: dict` using
  `PortfolioDecision.model_dump(mode="json")`.
- Produces: `forecast_record_from_state(final_state, ticker, run_metadata,
  generated_at=None) -> ForecastRecord`.
- Compatibility: legacy states without `portfolio_decision` parse only stable
  final-decision labels; absent fields are listed in `missing_fields`.

- [x] **Step 1: Write failing typed-snapshot and mapping tests**

Assert Portfolio Manager results include the serialized decision. Build one
actionable state with an accepted target and verified reference price; assert
the record contains its status, rating, central target, computed central-case
return, invalidation condition, evidence fingerprint, and model identity. Add a
legacy Hold state and assert it produces a record with explicit missingness.

- [x] **Step 2: Run mapping tests and confirm RED**

Run: `python -m pytest tests/test_forecast_record.py tests/test_portfolio_manager_integrity.py -k "snapshot or from_state or legacy" -q`

Expected: assertions fail because graph state has no typed snapshot and the
state factory does not exist.

- [x] **Step 3: Preserve the typed decision in graph state**

Add `portfolio_decision` to `AgentState`. Include the JSON-mode dump in every
Portfolio Manager result, including unavailable upstream failures.

- [x] **Step 4: Implement conservative state mapping**

Use the typed snapshot when present. For legacy state, parse only stable labels
such as `Decision Status`, `Rating`, `Price Target`, and `Target Validation`.
Hash evidence sections into sorted evidence IDs. Use run metadata for model and
configuration provenance. Compute expected return only from an accepted central
target and a verified positive reference price. Never synthesize probabilities,
quantiles, target ranges, benchmark returns, quote currency, or source timestamps.

- [x] **Step 5: Run mapping and neighboring decision tests**

Run: `python -m pytest tests/test_forecast_record.py tests/test_portfolio_manager_integrity.py tests/test_decision_integrity.py -q`

Expected: all selected tests pass.

- [x] **Step 6: Commit decision integration**

```bash
git add -- tradingagents/agents/utils/agent_states.py tradingagents/agents/managers/portfolio_manager.py tradingagents/forecasting/record_factory.py tests/test_forecast_record.py tests/test_portfolio_manager_integrity.py
git commit -m "feat: map portfolio decisions to forecast records"
```

---

### Task 4: Append-Only Report Persistence

**Files:**
- Modify: `tradingagents/reporting.py`
- Modify: `tests/test_reporting.py`
- Modify: `tests/test_forecast_record.py`

**Interfaces:**
- Consumes: `forecast_record_from_state` and optional run metadata.
- Produces: `forecast_record.json` beside `run_manifest.json` in every saved
  report tree.
- Contract: if an existing file has the same record ID, persistence is
  idempotent; if it has a different ID, writing fails rather than overwriting an
  immutable record.

- [x] **Step 1: Write failing persistence tests**

Assert report writing creates a valid record file, repeated identical writing is
safe, and a changed completed state cannot overwrite the existing record path.

- [x] **Step 2: Run persistence tests and confirm RED**

Run: `python -m pytest tests/test_reporting.py tests/test_forecast_record.py -k "forecast_record or append_only" -q`

Expected: the record file is absent and overwrite protection does not exist.

- [x] **Step 3: Persist with immutable overwrite protection**

Build the record before writing. Compare an existing file's `record_id`; accept
an exact match and raise `FileExistsError` for a mismatch. Serialize with sorted
keys, two-space indentation, and one trailing newline.

- [x] **Step 4: Run reporting and record tests**

Run: `python -m pytest tests/test_reporting.py tests/test_forecast_record.py -q`

Expected: all selected tests pass.

- [x] **Step 5: Commit persistence separately**

```bash
git add -- tradingagents/reporting.py tests/test_reporting.py tests/test_forecast_record.py
git commit -m "feat: persist immutable forecast records"
```

---

### Task 5: Project A Verification and Roadmap Status

**Files:**
- Modify: `docs/superpowers/plans/2026-08-14-decision-integrity-foundation.md`
- Modify: `docs/superpowers/plans/2026-08-20-immutable-forecast-record.md`

- [x] **Step 1: Run focused Project A verification**

Run: `python -m pytest tests/test_forecast_record.py tests/test_reporting.py tests/test_portfolio_manager_integrity.py tests/test_decision_integrity.py -q`

- [x] **Step 2: Run the complete suite and Ruff**

Run: `python -m pytest -q`

Run: `python -m ruff check tradingagents/forecasting tradingagents/reporting.py tradingagents/agents/utils/agent_states.py tradingagents/agents/managers/portfolio_manager.py tests/test_forecast_record.py tests/test_reporting.py tests/test_portfolio_manager_integrity.py`

- [x] **Step 3: Inspect repository scope**

Run: `git diff --check`

Run: `git status --short`

Expected: no whitespace errors and only Project A documentation remains after
the production commits.

- [x] **Step 4: Mark Project A complete without changing Projects B-E**

Check the completed Project A and plan steps. Keep outcome scoring,
walk-forward evaluation, portfolio optimization, and role-based model promotion
explicitly pending.

- [x] **Step 5: Commit verification documentation**

Verification on 2026-08-20: `73 passed` in the focused Project A suite and
`792 passed, 2 skipped` in the complete suite. Ruff and `git diff --check`
completed without errors. The optional skips were the missing Bedrock dependency
and absent DeepSeek live API key.

```bash
git add -- docs/superpowers/plans/2026-08-14-decision-integrity-foundation.md docs/superpowers/plans/2026-08-20-immutable-forecast-record.md
git commit -m "docs: complete immutable forecast record project"
```
