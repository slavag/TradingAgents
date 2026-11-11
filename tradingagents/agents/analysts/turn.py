"""One tool-using analyst turn, including a tool-free final report turn."""

import json

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

WRAP_UP = (
    "You have used every tool round this report allows. Write your final "
    "report now from the tool results above, and identify any data you could "
    "not retrieve."
)


def _as_text(messages):
    """Make a completed tool history acceptable to an unbound chat model."""
    written = []
    for message in messages:
        if isinstance(message, AIMessage) and message.tool_calls:
            said = message.content if isinstance(message.content, str) else ""
            calls = "; ".join(
                f"{call['name']}({json.dumps(call['args'], default=str)})"
                for call in message.tool_calls
            )
            written.append(AIMessage(content=f"{said}\n[Called {calls}]".strip()))
        elif isinstance(message, ToolMessage):
            written.append(HumanMessage(
                content=f"[{message.name or 'Tool'} returned]\n{message.content}"
            ))
        else:
            written.append(message)
    return written


def take_turn(prompt, llm, tools, messages):
    """Use tools normally; after the cap, request one final tool-free report."""
    if (
        messages
        and isinstance(messages[-1], HumanMessage)
        and messages[-1].content == WRAP_UP
    ):
        final_prompt = prompt.partial(tool_names="none; tool rounds are spent")
        return (final_prompt | llm).invoke(_as_text(messages))
    return (prompt | llm.bind_tools(tools)).invoke(messages)
