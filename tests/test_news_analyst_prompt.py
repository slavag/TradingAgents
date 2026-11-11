"""Keep the news analyst prompt aligned with the model-visible tool schema."""
import inspect

import pytest

import tradingagents.agents.analysts.news_analyst as na
from tradingagents.agents.utils.news_data_tools import get_news


@pytest.mark.unit
def test_get_news_injects_the_run_ticker_instead_of_accepting_a_model_query():
    arg_names = set(get_news.args.keys())
    assert "ticker" not in arg_names
    assert "query" not in arg_names
    assert {"start_date", "end_date"} <= arg_names


@pytest.mark.unit
def test_news_prompt_matches_get_news_signature():
    src = inspect.getsource(na)
    assert "get_news(start_date, end_date)" in src
    assert "get_news(ticker" not in src
    assert "get_news(query" not in src
