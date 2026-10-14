"""LLM callback accounting, including provider-reported cache usage."""

from concurrent.futures import ThreadPoolExecutor

import pytest
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, LLMResult

from cli import stats_handler
from cli.stats_handler import StatsCallbackHandler

pytestmark = pytest.mark.unit


def _response(read=None, created=None):
    details = {}
    if read is not None:
        details["cache_read"] = read
    if created is not None:
        details["cache_creation"] = created
    usage = {"input_tokens": 300, "output_tokens": 50, "total_tokens": 350}
    if details:
        usage["input_token_details"] = details
    return LLMResult(generations=[[ChatGeneration(message=AIMessage(
        content="done", usage_metadata=usage,
    ))]])


def test_stats_handler_counts_cache_reads_writes_and_unknown_usage_separately():
    handler = StatsCallbackHandler()

    handler.on_llm_end(_response(read=120, created=40))
    handler.on_llm_end(_response(read=0, created=0))
    handler.on_llm_end(_response())

    stats = handler.get_stats()
    assert stats["tokens_in"] == 900
    assert stats["tokens_out"] == 150
    assert stats["cache_read_tokens"] == 120
    assert stats["cache_creation_tokens"] == 40
    assert stats["cache_usage_reported_calls"] == 2
    assert stats["cache_usage_unreported_calls"] == 1


def test_stats_handler_cache_counts_are_thread_safe():
    handler = StatsCallbackHandler()
    response = _response(read=25, created=0)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(handler.on_llm_end, [response] * 100))

    stats = handler.get_stats()
    assert stats["tokens_in"] == 30_000
    assert stats["cache_read_tokens"] == 2_500
    assert stats["cache_usage_reported_calls"] == 100


def test_cache_usage_label_distinguishes_zero_hits_from_unreported_details():
    handler = StatsCallbackHandler()
    handler.on_llm_end(_response(read=0, created=0))
    handler.on_llm_end(_response())

    label = stats_handler.format_cache_usage(handler.get_stats())

    assert "0 read" in label and "0 write" in label
    assert "1 call unreported" in label


def test_missing_usage_metadata_is_counted_as_unreported_cache_detail():
    handler = StatsCallbackHandler()
    handler.on_llm_end(LLMResult(generations=[[ChatGeneration(message=AIMessage(
        content="done",
    ))]]))

    assert handler.get_stats()["cache_usage_unreported_calls"] == 1
