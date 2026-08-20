"""Portfolio Manager: synthesises the risk-analyst debate into the final decision.

Uses LangChain's ``with_structured_output`` so the LLM produces a typed draft
in a single call. The draft is finalized through deterministic evidence checks
before rendering. A provider or schema failure is explicit and non-actionable;
unchecked free text never becomes a final portfolio decision.
"""

from __future__ import annotations

from pydantic import ValidationError

from tradingagents.agents.schemas import (
    PortfolioDecision,
    PortfolioDecisionDraft,
    render_pm_decision,
)
from tradingagents.agents.utils.agent_utils import (
    get_instrument_context_from_state,
    get_language_instruction,
)
from tradingagents.agents.utils.decision_integrity import (
    build_decision_evidence,
    extract_verified_reference_price,
    finalize_portfolio_decision,
)
from tradingagents.agents.utils.structured import (
    NO_EXTERNAL_TOOLS,
    StructuredOutputFailure,
    bind_structured,
    invoke_structured_required,
    stage_is_unavailable,
)


def _manager_result(risk_debate_state: dict, decision: PortfolioDecision) -> dict:
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
    }


def create_portfolio_manager(llm):
    structured_llm = bind_structured(
        llm,
        PortfolioDecisionDraft,
        "Portfolio Manager",
        fallback_to_text=False,
    )

    def portfolio_manager_node(state) -> dict:
        instrument_context = get_instrument_context_from_state(state)

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

---

**Consistency Protocol:**
- Determine the thesis from evidence before choosing position actions. Treat upstream recommendations as arguments, not independent votes.
- Compare the independent thesis with the Research Manager and Trader conclusions, and explain material disagreements from the underlying evidence rather than resolving them by vote count.
- Do not move more than one tier away from a well-supported Research Manager rating unless the evidence identifies a factual thesis invalidation or a materially different horizon.
- Do not assume whether the user already owns the instrument. The rating expresses the forward risk/reward view; describe separate actions for existing and prospective positions when relevant.
- Existing Position must choose Add, Hold, Trim, or Exit for a reader who already owns the instrument, with concise sizing and execution guidance.
- New Position must choose Buy, Conditional Buy, Wait, Avoid, Conditional Sell, or Sell for a reader considering a new long or short position.
- Conditional Buy requires a confirmation trigger, an optional pullback entry, and a downside invalidation grounded in the supplied evidence.
- Conditional Sell requires a breakdown trigger, an optional failed-rally exit or short entry, and an upside invalidation grounded in the supplied evidence.
- Wait requires an explicit watch plan: what evidence or price behavior permits entry, an optional alternate setup, and what invalidates or materially weakens the setup. Do not return a generic instruction to revisit later.
- A bullish or strongly bullish thesis with future entry conditions is Conditional Buy, not Wait. A bearish or strongly bearish thesis with future sell or short conditions is Conditional Sell, not Wait.
- Recommendation Confidence is required for every actionable decision. Score the evidence strength for the recommendation from 0 to 100 independently of target confidence; it must remain meaningful even when no numeric target can be validated.
- Never invent a price level. Use price ranges only when the supplied evidence contains those levels; otherwise express the condition without a number.
- Apply the same rating standard to materially identical evidence across runs.
- When the supplied analysis contains a verified reference price and usable valuation or technical levels, provide a decision-consistent central-case price target, its time horizon, an uncalibrated evidence-strength score, and a short rationale citing those levels.
- When you provide a target, include a short verbatim supporting quote copied from the supplied evidence. The quote must contain the exact target number and nearby price-level context.
- For Buy/Overweight, the target should normally be above the reference price; for Sell/Underweight, below it. Use null only when the supplied evidence genuinely cannot support a numeric target, never as a shortcut.
- Use Actionable with a five-tier rating when the evidence supports a direction. Use Abstain with a null rating when it does not. Never use Unavailable; the application reserves that status for technical failures.

Be decisive and ground every conclusion in specific evidence from the analysts.

{NO_EXTERNAL_TOOLS}{get_language_instruction()}"""

        try:
            draft = invoke_structured_required(
                structured_llm,
                prompt,
                "Portfolio Manager",
            )
            if not isinstance(draft, PortfolioDecisionDraft):
                raise StructuredOutputFailure("structured_response_invalid")
            evidence = build_decision_evidence(state)
            reference_price = extract_verified_reference_price(evidence)
            decision = finalize_portfolio_decision(
                draft,
                evidence,
                reference_price=reference_price,
            )
        except StructuredOutputFailure as exc:
            decision = PortfolioDecision.unavailable(exc.code)
        except (ValidationError, TypeError, ValueError):
            decision = PortfolioDecision.unavailable("structured_response_invalid")
        return _manager_result(risk_debate_state, decision)

    return portfolio_manager_node
