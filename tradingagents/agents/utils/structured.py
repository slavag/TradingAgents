"""Shared helpers for optional and required structured agent output.

Research Manager and Trader retain a graceful free-text fallback for providers
without structured output. The final Portfolio Manager decision uses the strict
helper instead: a binding, provider, or schema failure becomes an explicit
non-actionable decision.

Both paths bind the provider through ``with_structured_output(Schema)`` and log
internal diagnostic details. Only stable failure codes cross the strict final
decision boundary.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

# Schema-only structured output binds exactly one tool (the schema itself), so a
# model that reaches for a search tool emits an unknown tool call and the whole
# structured attempt is discarded for a free-text retry. Agents on this path
# state the constraint explicitly rather than relying on the binding alone
# (#1130).
NO_EXTERNAL_TOOLS = (
    "Use only the evidence provided in this prompt. Do not call external tools "
    "or search the web; if something is missing, say so explicitly."
)


class StructuredOutputFailure(RuntimeError):
    """Sanitized failure from a structured-required decision boundary."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def bind_structured(
    llm: Any,
    schema: type[T],
    agent_name: str,
    *,
    fallback_to_text: bool = True,
) -> Any | None:
    """Return ``llm.with_structured_output(schema)`` or ``None`` if unsupported.

    ``fallback_to_text=False`` converts any binding failure into ``None`` for a
    strict caller to handle. The default preserves the narrower legacy fallback
    behavior for Research Manager and Trader.
    """
    try:
        return llm.with_structured_output(schema)
    except Exception as exc:
        if fallback_to_text and not isinstance(
            exc,
            (NotImplementedError, AttributeError),
        ):
            raise
        next_step = (
            "falling back to free-text generation"
            if fallback_to_text
            else "the required structured decision will be unavailable"
        )
        logger.warning(
            "%s: provider does not support with_structured_output (%s); %s",
            agent_name,
            exc,
            next_step,
        )
        return None


def invoke_structured_or_freetext(
    structured_llm: Any | None,
    plain_llm: Any,
    prompt: Any,
    render: Callable[[T], str],
    agent_name: str,
) -> str:
    """Run the structured call and render to markdown; fall back to free-text on any failure.

    ``prompt`` is whatever the underlying LLM accepts (a string for chat
    invocations, a list of message dicts for chat models that take that
    shape). The same value is forwarded to the free-text path so the
    fallback sees the same input the structured call did.
    """
    if structured_llm is not None:
        try:
            result = structured_llm.invoke(prompt)
            if result is None:
                # A thinking model can answer in plain text instead of calling
                # the tool, leaving the parser with nothing to return. Treat it
                # as a structured miss and fall back, with a clear reason.
                raise ValueError("structured output returned no parsed result")
            return render(result)
        except Exception as exc:
            logger.warning(
                "%s: structured-output invocation failed (%s); retrying once as free text",
                agent_name, exc,
            )

    response = plain_llm.invoke(prompt)
    return response.content


def invoke_structured_required(
    structured_llm: Any | None,
    prompt: Any,
    agent_name: str,
) -> BaseModel:
    """Invoke a required structured response without an unchecked text fallback."""
    if structured_llm is None:
        raise StructuredOutputFailure("structured_binding_unsupported")

    try:
        result = structured_llm.invoke(prompt)
    except ValidationError as exc:
        logger.warning("%s: structured response failed schema validation: %s", agent_name, exc)
        raise StructuredOutputFailure("structured_response_invalid") from exc
    except Exception as exc:
        logger.warning("%s: structured invocation failed: %s", agent_name, exc)
        raise StructuredOutputFailure("structured_invocation_failed") from exc

    if result is None:
        raise StructuredOutputFailure("structured_response_missing")
    if not isinstance(result, BaseModel):
        logger.warning(
            "%s: structured response returned unexpected type %s",
            agent_name,
            type(result).__name__,
        )
        raise StructuredOutputFailure("structured_response_invalid")
    return result
