"""Model-supplied tool arguments must not escape the run's ticker or date."""

from unittest.mock import patch

import pytest
from langchain_core.messages import AIMessage
from langgraph.prebuilt import ToolNode

from tradingagents.agents.utils import (
    core_stock_tools,
    fundamental_data_tools,
    macro_data_tools,
    market_data_validation_tools,
    news_data_tools,
    technical_indicators_tools,
)
from tradingagents.dataflows.errors import VendorUnavailableError

pytestmark = pytest.mark.unit
TRADE_DATE = "2026-08-14"


def test_stock_tool_schema_hides_the_run_ticker_and_date():
    fields = core_stock_tools.get_stock_data.tool_call_schema.model_json_schema()["properties"]
    assert "symbol" not in fields
    assert "trade_date" not in fields
    assert set(fields) == {"start_date", "end_date"}


def test_stock_tool_bounds_and_canonicalizes_model_dates():
    with patch.object(core_stock_tools, "route_to_vendor", return_value="ok") as routed:
        result = core_stock_tools.get_stock_data.func(
            "NVDA", "2026-8-1", "2026-9-1", trade_date=TRADE_DATE
        )
    assert result == "ok"
    routed.assert_called_once_with("get_stock_data", "NVDA", "2026-08-01", TRADE_DATE)


def test_reversed_stock_window_returns_correction_without_vendor_call():
    with patch.object(core_stock_tools, "route_to_vendor", return_value="wrong") as routed:
        result = core_stock_tools.get_stock_data.func(
            "NVDA", "2026-08-12", "2026-08-01", trade_date=TRADE_DATE
        )
    assert not routed.called
    assert "start_date" in result and "end_date" in result


@pytest.mark.parametrize("module,tool_name", [
    (core_stock_tools, "get_stock_data"),
    (news_data_tools, "get_news"),
])
def test_malformed_end_date_returns_correction_without_vendor_call(module, tool_name):
    tool = getattr(module, tool_name)
    with patch.object(module, "route_to_vendor", return_value="wrong") as routed:
        result = tool.func(
            "NVDA", "2026-08-01", "garbage", trade_date=TRADE_DATE
        )
    assert not routed.called
    assert "end_date" in result


@pytest.mark.parametrize("module,tool,args", [
    (fundamental_data_tools, fundamental_data_tools.get_fundamentals, ("NVDA", "garbage")),
    (fundamental_data_tools, fundamental_data_tools.get_balance_sheet, ("NVDA", "quarterly", "garbage")),
    (fundamental_data_tools, fundamental_data_tools.get_cashflow, ("NVDA", "quarterly", "garbage")),
    (fundamental_data_tools, fundamental_data_tools.get_income_statement, ("NVDA", "quarterly", "garbage")),
    (technical_indicators_tools, technical_indicators_tools.get_indicators, ("NVDA", "rsi", "garbage")),
    (macro_data_tools, macro_data_tools.get_macro_indicators, ("cpi", "garbage", None)),
    (news_data_tools, news_data_tools.get_global_news, ("garbage", None, None)),
])
def test_malformed_direct_date_never_queries_vendor(module, tool, args):
    with patch.object(module, "route_to_vendor", return_value="wrong") as routed:
        result = tool.func(*args, trade_date=TRADE_DATE)
    assert not routed.called
    assert "date" in result


def test_malformed_snapshot_date_never_builds_market_snapshot():
    with patch.object(market_data_validation_tools, "build_verified_market_snapshot",
                      return_value="wrong") as snapshot:
        result = market_data_validation_tools.get_verified_market_snapshot.func(
            "NVDA", "garbage", 30, trade_date=TRADE_DATE
        )
    snapshot.assert_not_called()
    assert "date" in result


def test_tool_node_injects_run_ticker_not_model_ticker():
    call = AIMessage(content="", tool_calls=[{
        "name": "get_stock_data", "args": {
            "start_date": "2026-08-01", "end_date": "2026-08-20"
        }, "id": "call-1",
    }])
    state = {"company_of_interest": "NVDA", "trade_date": TRADE_DATE,
             "messages": [call]}
    with patch.object(core_stock_tools, "route_to_vendor", return_value="ok") as routed:
        result = ToolNode([core_stock_tools.get_stock_data]).invoke(state)
    assert result["messages"][0].content == "ok"
    routed.assert_called_once_with("get_stock_data", "NVDA", "2026-08-01", TRADE_DATE)


def test_news_and_fundamental_tools_bound_dates_to_the_run():
    with patch.object(news_data_tools, "route_to_vendor", return_value="news") as routed_news:
        result_news = news_data_tools.get_news.func(
            "NVDA", "2026-8-1", "2026-8-20", trade_date=TRADE_DATE
        )
    with patch.object(fundamental_data_tools, "route_to_vendor", return_value="facts") as routed_facts:
        result_facts = fundamental_data_tools.get_income_statement.func(
            "NVDA", "quarterly", "2026-8-20", trade_date=TRADE_DATE
        )
    assert result_news == "news" and result_facts == "facts"
    routed_news.assert_called_once_with("get_news", "NVDA", "2026-08-01", TRADE_DATE)
    routed_facts.assert_called_once_with("get_income_statement", "NVDA", "quarterly", TRADE_DATE)


def test_unavailable_news_vendor_returns_explicit_placeholder_to_agent():
    with patch.object(news_data_tools, "route_to_vendor",
                      side_effect=VendorUnavailableError("private request details")):
        result = news_data_tools.get_news.func(
            "NVDA", "2026-08-01", "2026-08-14", trade_date=TRADE_DATE
        )
    assert result.startswith("DATA_UNAVAILABLE:")
    assert "private request details" not in result


def test_remaining_market_and_macro_tools_use_the_run_date():
    with patch.object(technical_indicators_tools, "route_to_vendor", return_value="rsi") as indicator:
        technical_indicators_tools.get_indicators.func(
            "NVDA", "rsi", "2026-8-20", 30, trade_date=TRADE_DATE
        )
    with patch.object(market_data_validation_tools, "build_verified_market_snapshot",
                      return_value="snapshot") as snapshot:
        market_data_validation_tools.get_verified_market_snapshot.func(
            "NVDA", "2026-8-20", 30, trade_date=TRADE_DATE
        )
    with patch.object(macro_data_tools, "route_to_vendor", return_value="macro") as macro:
        macro_data_tools.get_macro_indicators.func(
            "cpi", "2026-8-20", None, trade_date=TRADE_DATE
        )
    with patch.object(news_data_tools, "route_to_vendor", return_value="global") as global_news:
        news_data_tools.get_global_news.func(
            "2026-8-20", None, None, trade_date=TRADE_DATE
        )
    indicator.assert_called_once_with("get_indicators", "NVDA", "rsi", TRADE_DATE, 30)
    snapshot.assert_called_once_with("NVDA", TRADE_DATE, 30)
    macro.assert_called_once_with("get_macro_indicators", "cpi", TRADE_DATE, None)
    global_news.assert_called_once_with("get_global_news", TRADE_DATE, None, None)


@pytest.mark.parametrize("tool,field", [
    (technical_indicators_tools.get_indicators, "symbol"),
    (market_data_validation_tools.get_verified_market_snapshot, "symbol"),
    (news_data_tools.get_insider_transactions, "ticker"),
])
def test_remaining_ticker_tools_hide_model_supplied_ticker(tool, field):
    schema = tool.tool_call_schema.model_json_schema()["properties"]
    assert field not in schema


def test_macro_tool_advertises_euro_area_aliases():
    description = (
        macro_data_tools.get_macro_indicators.tool_call_schema.model_json_schema()
        ["properties"]["indicator"]["description"]
    )
    assert "ecb_deposit_rate" in description
    assert "euro_hicp" in description
