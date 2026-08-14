# Position-Aware Recommendations Design

## Goal

Replace the ambiguous single-label recommendation with a position-aware decision
that tells the reader both what the investment thesis is and what to do when they
already own the instrument versus when they do not. Conditional recommendations
must include concrete confirmation, alternative-entry or exit, and invalidation
instructions. Consolidated HTML reports must make bullish and bearish direction
immediately scannable without relying on color alone.

## Selected Approach

Extend the existing structured Portfolio Manager contract and carry the new
fields through its deterministic finalization, markdown rendering, target-profile
extraction, web serialization, and consolidated report output.

Two alternatives were rejected for this release:

- Heuristically extracting position guidance from prose would preserve the
  current model schema, but would be brittle and provider-dependent.
- Attaching a verbatim evidence quote to every conditional level would provide
  the strongest provenance, but duplicates much of the planned immutable
  `ForecastRecord`. That remains part of future Option 3.

## Decision Contract

Every actionable Portfolio Manager decision adds:

- `thesis`: Strongly Bullish, Bullish, Neutral, Bearish, or Strongly Bearish;
- `existing_position_action`: Add, Hold, Trim, or Exit;
- `existing_position_summary`: concise sizing and action guidance;
- `new_position_action`: Buy, Conditional Buy, Wait, Avoid, Conditional Sell,
  or Sell;
- `new_position_summary`: concise prospective-position guidance; and
- `conditional_plan`: required for Conditional Buy and Conditional Sell and
  absent for other prospective actions.

The conditional plan contains:

- `confirmation`: the primary condition that permits the action;
- `alternative`: an optional pullback, failed-rally, or other alternate setup;
- `invalidation`: the condition that weakens or cancels the setup.

For Conditional Buy, the prompt explicitly asks for a sustained breakout or
other confirmation, an optional pullback accumulation setup, and the downside
invalidation. For Conditional Sell, it asks for a sustained breakdown or other
bearish confirmation, an optional failed-rally or rebound exit setup, and the
upside invalidation. The instructions may contain price ranges when those ranges
already exist in the supplied evidence; the model must not invent levels.

Abstain and Unavailable decisions do not carry position actions or a conditional
plan. Legacy saved decisions without the new fields remain readable, but newly
generated actionable decisions must include the complete position-aware bundle.

## Rendering

Portfolio Manager markdown adds stable labeled fields after the existing
investment thesis:

- Thesis
- Existing Position
- Existing Position Guidance
- New Position
- New Position Guidance
- Conditional Confirmation, Alternative, and Invalidation when applicable

The consolidated markdown summary gains Thesis, Existing Position, and New
Position columns. Each per-stock section includes a dedicated Position Plan.

The consolidated HTML report gains a three-part decision compass:

- a bull icon for bullish thesis states;
- balanced scales for Neutral;
- a bear icon for bearish thesis states.

The compass also displays separate existing-position and new-position cards.
Conditional plans render as three visually distinct rows: confirmation,
alternative, and invalidation. Text labels and accessible SVG titles accompany
all icons, so direction is not communicated by green/red alone.

The styling remains consistent with the current editorial report: deep ink,
warm paper, teal bullish accents, coral bearish accents, and gold neutral or
conditional accents. The icons are inline SVG and introduce no external assets.

## Data Flow

1. Portfolio Manager returns a structured `PortfolioDecisionDraft`.
2. Pydantic validates action and conditional-plan combinations.
3. Deterministic decision finalization copies the complete position bundle.
4. Markdown rendering emits stable labels.
5. Target-profile extraction reads those labels for CLI and web compatibility.
6. Web serialization returns the structured recommendation fields.
7. Consolidated markdown and HTML consume the same result dictionary.

No extra LLM call or market-data request is introduced.

## Error Handling and Compatibility

- Conditional Buy or Conditional Sell without a conditional plan fails schema
  validation and becomes an explicit Unavailable structured response.
- A non-conditional action carrying a conditional plan is rejected to prevent
  stale or contradictory instructions.
- Actionable decisions missing any required position-aware field are rejected.
- Non-actionable decisions carrying action fields are rejected.
- Existing target-validation behavior remains unchanged. A rejected target is
  displayed prominently, and position instructions do not repopulate the empty
  target metrics.
- Older reports without position-aware labels fall back to their existing
  five-tier decision presentation.

## Testing

Tests are written first and must cover:

- actionable and non-actionable schema combinations;
- Conditional Buy and Conditional Sell plan requirements;
- rejection of contradictory conditional plans;
- finalization preserving the structured position bundle;
- Portfolio Manager prompt and rendered markdown labels;
- target-profile extraction and web serialization;
- consolidated markdown columns and per-stock plan;
- consolidated HTML bull, neutral, and bear icons;
- conditional confirmation, alternative, and invalidation rendering;
- HTML escaping for every model-provided field; and
- legacy result dictionaries without the new fields.

Focused tests must pass before the broader project suite is run.

## Future Option 3

The immutable `ForecastRecord` remains in the roadmap. Its condition objects will
upgrade the three conditional strings into typed levels with timestamps,
supporting evidence identifiers, data-quality flags, and outcome-resolution
rules. This release intentionally establishes the UI and decision semantics
without claiming calibrated probabilities or implementing portfolio optimization.

## Non-Goals

This release does not:

- claim that bullish or bearish icons improve predictive accuracy;
- infer the user's current holdings;
- execute orders or recommend exact portfolio weights;
- add a new market-data or analyst-consensus vendor;
- validate every number inside conditional prose independently; or
- implement the immutable Forecast Record or calibrated probabilities.
