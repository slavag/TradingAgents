"""Portfolio Manager: synthesises the risk-analyst debate into the final decision.

Uses LangChain's ``with_structured_output`` so the LLM produces a typed draft
in a single call. The draft is finalized through deterministic evidence checks
before rendering. A provider or schema failure is explicit and non-actionable;
unchecked free text never becomes a final portfolio decision.
"""

from __future__ import annotations

from pydantic import ValidationError

from tradingagents.agents.schemas import (
    DecisionStatus,
    PortfolioDecision,
    PortfolioDecisionDraft,
    PortfolioOutlookRepair,
    PortfolioTargetRepair,
    TargetValidationStatus,
    render_pm_decision,
)
from tradingagents.agents.utils.agent_utils import (
    get_agent_context_from_state,
    get_language_instruction,
)
from tradingagents.agents.utils.decision_integrity import (
    build_decision_evidence,
    finalize_portfolio_decision,
    sanitize_decision_sizing,
    verified_price_context,
    verified_target_candidates,
)
from tradingagents.agents.utils.evidence import evidence_is_admissible
from tradingagents.agents.utils.outlooks import validate_outlooks
from tradingagents.agents.utils.structured import (
    NO_EXTERNAL_TOOLS,
    StructuredOutputFailure,
    bind_structured,
    invoke_structured_required,
    stage_is_unavailable,
)


def _manager_result(risk_debate_state: dict, decision: PortfolioDecision, outlooks=None) -> dict:
    final_trade_decision = render_pm_decision(decision)
    return {
        "risk_debate_state": {
            "judge_decision": final_trade_decision,
            "history": risk_debate_state["history"],
            "aggressive_history": risk_debate_state["aggressive_history"],
            "conservative_history": risk_debate_state["conservative_history"],
            "neutral_history": risk_debate_state["neutral_history"],
            "latest_speaker": "Judge",
            "current_aggressive_response": risk_debate_state[
                "current_aggressive_response"
            ],
            "current_conservative_response": risk_debate_state[
                "current_conservative_response"
            ],
            "current_neutral_response": risk_debate_state["current_neutral_response"],
            "count": risk_debate_state["count"],
        },
        "final_trade_decision": final_trade_decision,
        "portfolio_decision": decision.model_dump(mode="json"),
        "portfolio_outlooks": [row.model_dump(mode="json") for row in outlooks or []],
    }


def _target_ready(decision: PortfolioDecision) -> bool:
    return (
        decision.target_validation_status is TargetValidationStatus.ACCEPTED
        and isinstance(decision.target_method, str)
        and bool(decision.target_method.strip())
        and isinstance(decision.target_risks, str)
        and bool(decision.target_risks.strip())
    )


def create_portfolio_manager(llm):
    structured_llm = bind_structured(
        llm,
        PortfolioDecisionDraft,
        "Portfolio Manager",
        fallback_to_text=False,
    )

    def portfolio_manager_node(state) -> dict:
        from tradingagents.related_companies import admitted_related_sources

        instrument_context = get_agent_context_from_state(
            state, "portfolio_manager", include_reports=True
        )

        history = state["risk_debate_state"]["history"]
        risk_debate_state = state["risk_debate_state"]
        research_plan = state["investment_plan"]
        trader_plan = state["trader_investment_plan"]

        if stage_is_unavailable(trader_plan, "Trader Proposal"):
            return _manager_result(
                risk_debate_state,
                PortfolioDecision.unavailable("upstream_trader_unavailable"),
            )

        past_context = state.get("past_context", "")
        lessons_line = (
            f"- Lessons from prior decisions and outcomes:\n{past_context}\n"
            if past_context
            else ""
        )
        target_candidates = verified_target_candidates(state)
        candidate_lines = "\n".join(f"- {line}" for line in target_candidates) or "- None verified"

        source_rows = "\n".join(
            f"{record.get('evidence_id')}: {record.get('title') or record.get('source')}"
            for record in [*(state.get("shared_evidence") or []), *admitted_related_sources(state, str(state.get("trade_date") or state.get("as_of_date") or ""))]
            if record.get("evidence_id") and evidence_is_admissible(
                record, str(state.get("trade_date") or state.get("as_of_date") or ""),
            )
        )

        prompt = f"""As the Portfolio Manager, synthesize the risk analysts' debate and deliver the final trading decision.

{instrument_context}

---

**Rating Scale** (use exactly one):
- **Buy**: Strong conviction to enter or add to position
- **Overweight**: Favorable outlook, gradually increase exposure
- **Hold**: Maintain current position, no action needed
- **Underweight**: Reduce exposure, take partial profits
- **Sell**: Exit position or avoid entry

**Context:**
- Research Manager's investment plan: **{research_plan}**
- Trader's transaction proposal: **{trader_plan}**
{lessons_line}
**Risk Analysts Debate History:**
{history}

Verified target candidates:
{candidate_lines}

Admitted source record IDs for the outlooks:
{source_rows}

---

**Consistency Protocol:**
- Evaluate the supplied direct-competitor and supplier/customer evidence before choosing the view. Explain material findings that change the recommendation, target assumptions, risk assessment or confidence, including counterarguments and coverage limitations. Related-company prices and financials are contextual evidence, never the main stock's reference price or observed target candidates.
- Determine the thesis from evidence before choosing position actions. Treat upstream recommendations as arguments, not independent votes.
- Compare the independent thesis with the Research Manager and Trader conclusions, and explain material disagreements from the underlying evidence rather than resolving them by vote count.
- Do not move more than one tier away from a well-supported Research Manager rating unless the evidence identifies a factual thesis invalidation or a materially different horizon.
- Use the supplied portfolio context when present. Do not assume whether the user already owns the instrument when no portfolio context was supplied. The rating expresses the forward risk/reward view; describe separate actions for existing and prospective positions when relevant.
- The optional holdings context does not establish current portfolio NAV or a risk budget. Do not invent a numeric portfolio weight or NAV percentage; give conditional sizing guidance instead.
- Existing Position must choose Add, Hold, Trim, or Exit for a reader who already owns the instrument, with concise sizing and execution guidance.
- New Position must choose Buy, Conditional Buy, Wait, Avoid, Conditional Sell, or Sell for a reader considering a new long or short position.
- Conditional Buy requires a confirmation trigger, an optional pullback entry, and a downside invalidation grounded in the supplied evidence.
- Conditional Sell requires a breakdown trigger, an optional failed-rally exit or short entry, and an upside invalidation grounded in the supplied evidence.
- Wait requires an explicit watch plan: what evidence or price behavior permits entry, an optional alternate setup, and what invalidates or materially weakens the setup. Do not return a generic instruction to revisit later.
- A bullish or strongly bullish thesis with future entry conditions is Conditional Buy, not Wait. A bearish or strongly bearish thesis with future sell or short conditions is Conditional Sell, not Wait.
- Recommendation Confidence is required for every actionable decision. Score the evidence strength for the recommendation from 0 to 100 independently of target confidence; it must remain meaningful even when no numeric target can be validated.
- Never invent an observed price level or source quote. Forward price estimates are permitted with explicit model assumptions, a verified input price, and independent target confidence. Keep entry conditions grounded in supplied evidence.
- Apply the same rating standard to materially identical evidence across runs.
- Daily OHLCV references and indicators exclude extended-hours trading. If a separate verified market-reaction marker is supplied, consider its session, timestamp and move as additional information, not a replacement daily close or proof of news causation. Reassess gap/execution risk when news postdates the daily price.
- For a Source level target, use an exact verified candidate value and copy its row verbatim as the supporting quote. For a Model estimate, project a central-case price from the verified close, current research, and explicit forward assumptions. The projected target need not already appear in a source. Set target_basis to Model estimate, explain the calculation in target_assumptions, and cite the verified Close row as the supporting quote. Never present assumptions as observed facts. Use Model estimate when source levels cannot support a forward target.
- A Source level target range may instead use two exact verified rows as its low and high bounds. Explain why those anchors form a plausible forward scenario for the stated horizon; a Hold range may bracket the close. Label technical scenarios as such, not as fair value.
- Every proposed target or range needs a method, risks, target confidence, and a full rationale. Target confidence measures evidence strength, not probability; low confidence is acceptable when clearly labeled.
- Provide a complete target bundle for every completed analysis, including Hold and low-confidence recommendations. Choose exactly one shape: a point target OR a range; set all unused shape fields to null. Express uncertainty in the independent recommendation and target confidence scores, with no minimum score required. A target validation issue must not change the recommendation.
- For Buy/Overweight, the target should normally be above the reference price; for Sell/Underweight, below it. Use a Model estimate with explicit assumptions when no exact source-level target supports the horizon. A missing verified input price is a data failure; low confidence is not.
- Use Actionable with a five-tier rating for completed research. A balanced outlook can support Hold with low recommendation confidence. Never omit the recommendation solely because target confidence is low or a target is difficult to estimate. Never use Unavailable; the application reserves that status for technical failures.

**Forward investment outlooks (required in outlooks):**
- Provide exactly two independently reasoned outlooks: months=6 and months=12 from {state.get('trade_date') or state.get('as_of_date')}. Do not copy the tactical target or relabel a short-term technical scenario.
- Each horizon needs its own rating, recommendation_confidence_score, point target or range with confidence_score, method, explicit assumptions, risks and supporting input quote(s), summary, drivers, upside_case, downside_case and invalidation.
- These are forward estimates, not company guidance. Use target_basis=Model estimate when projecting from verified inputs; copy the verified Close row as the input quote and explain your forecast calculation and assumptions. Different horizons may have different views; explain why.
- source_ids must cite actual supplied evidence record IDs. Never invent a record ID, company fact, catalyst or announced date. Do not return only a horizon label or an unsupported price. Low confidence is acceptable and distinct from recommendation confidence.

Be decisive and ground every conclusion in specific evidence from the analysts.

{NO_EXTERNAL_TOOLS}{get_language_instruction()}"""

        outlook_rows = []
        try:
            draft = invoke_structured_required(
                structured_llm,
                prompt,
                "Portfolio Manager",
            )
            if not isinstance(draft, PortfolioDecisionDraft):
                raise StructuredOutputFailure("structured_response_invalid")
            outlook_rows = draft.outlooks
            draft = sanitize_decision_sizing(draft)
            evidence = build_decision_evidence(state)
            reference_price = verified_price_context(state)["close"]
            decision = finalize_portfolio_decision(
                draft,
                evidence,
                reference_price=reference_price,
                eligible_quotes=target_candidates,
            )
            if (draft.status is DecisionStatus.ACTIONABLE
                    and not _target_ready(decision) and target_candidates):
                repair_llm = bind_structured(
                    llm, PortfolioTargetRepair, "Target Repair",
                    fallback_to_text=False,
                )
                repair_prompt = f"""Fill only the missing or invalid target bundle for the existing Portfolio Manager decision.

Original rating: {draft.rating.value}
Original thesis: {draft.investment_thesis}
Validation reason: {decision.target_validation_reason or 'target_not_proposed'}
Verified candidate price rows:
{candidate_lines}

When no source-level target supports the horizon, return a Model estimate with explicit target_assumptions and calculation, using the verified Close row as its input supporting quote. The projected price need not appear in that quote. Return exactly one complete shape: a point target OR a range. Set every unused shape field to null. Include horizon, exact supporting row(s), method, risks, rationale, and uncalibrated target confidence. Low confidence is acceptable; do not omit a target because confidence is low. Use the observed rows as anchors, not automatic forecasts. Do not revise the rating, position actions, thesis, company facts, or evidence. Return target fields only. Represent weak or conflicting evidence with low target confidence and explicit risks; do not use a null bundle as a substitute for an uncertain estimate.

{NO_EXTERNAL_TOOLS}{get_language_instruction()}"""
                try:
                    repair = invoke_structured_required(
                        repair_llm, repair_prompt, "Target Repair",
                    )
                    if not isinstance(repair, PortfolioTargetRepair):
                        raise StructuredOutputFailure("structured_response_invalid")
                    repaired_draft = sanitize_decision_sizing(
                        draft.model_copy(update=repair.model_dump()),
                    )
                    decision = finalize_portfolio_decision(
                        repaired_draft,
                        evidence,
                        reference_price=reference_price,
                        eligible_quotes=target_candidates,
                    )
                except (StructuredOutputFailure, ValidationError, TypeError, ValueError):
                    # A target-only failure must not erase the PM recommendation.
                    pass
        except StructuredOutputFailure as exc:
            decision = PortfolioDecision.unavailable(exc.code)
        except (ValidationError, TypeError, ValueError):
            decision = PortfolioDecision.unavailable("structured_response_invalid")
        outlooks = []
        if state.get("trade_date") or state.get("as_of_date"):
            try:
                outlooks = validate_outlooks(outlook_rows, state)
                if (decision.status is DecisionStatus.ACTIONABLE and target_candidates
                        and any(record.get("evidence_id") for record in state.get("shared_evidence") or [])
                        and any(row.status != "available" for row in outlooks)):
                    chain = bind_structured(llm, PortfolioOutlookRepair, "Outlook Repair", fallback_to_text=False)
                    reasons = "; ".join(f"{row.months} months: {row.reason or row.status}" for row in outlooks)
                    repair_prompt = f"""Complete only the missing or invalid six- and twelve-month investment outlooks.
Analysis date: {state.get('trade_date') or state.get('as_of_date')}
Validation: {reasons}
Tactical view (must not be changed): {decision.executive_summary}
Research: {decision.investment_thesis}
Verified price rows:\n{candidate_lines}
Source record IDs:\n{source_rows}
Return outlooks only, with months=6 and months=12. Each needs an independent rating, recommendation confidence, numeric target/range and target confidence, summary, drivers, upside/downside cases and invalidation. Use explicit Model estimate assumptions and calculation for forward targets, anchored to the exact verified Close row. The forecast need not already appear in a source. Include horizon-specific method, risks, rationale and actual source_ids. Do not change tactical rating, target, guidance or facts. Low confidence is acceptable.
{NO_EXTERNAL_TOOLS}{get_language_instruction()}"""
                    try:
                        repaired = invoke_structured_required(chain, repair_prompt, "Outlook Repair")
                        if isinstance(repaired, PortfolioOutlookRepair):
                            candidate_outlooks = validate_outlooks(repaired.outlooks, state)
                            # A horizon-only repair cannot degrade a previously valid horizon.
                            outlooks = [old if old.status == "available" else new
                                        for old, new in zip(outlooks, candidate_outlooks, strict=True)]
                    except (StructuredOutputFailure, ValidationError, TypeError, ValueError):
                        pass
            except (ValidationError, TypeError, ValueError):
                # Horizon failures do not invalidate the independently completed tactical decision.
                outlooks = []
        return _manager_result(risk_debate_state, decision, outlooks)

    return portfolio_manager_node
