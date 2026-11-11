"""Config isolation: get/set must not leak nested-dict references."""

import copy
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch

import pytest

import tradingagents.default_config as default_config
from tradingagents.dataflows.config import get_config, run_config, set_config
from tradingagents.graph.trading_graph import TradingAgentsGraph


@pytest.mark.unit
class DataflowsConfigIsolationTests(unittest.TestCase):
    def setUp(self):
        set_config(copy.deepcopy(default_config.DEFAULT_CONFIG))

    def test_get_config_returns_deep_copy(self):
        cfg = get_config()
        cfg["data_vendors"]["core_stock_apis"] = "alpha_vantage"
        cfg["tool_vendors"]["get_stock_data"] = "alpha_vantage"

        fresh = get_config()
        self.assertEqual(fresh["data_vendors"]["core_stock_apis"], "yfinance")
        self.assertNotIn("get_stock_data", fresh["tool_vendors"])

    def test_set_config_does_not_alias_caller_nested_dicts(self):
        custom = copy.deepcopy(default_config.DEFAULT_CONFIG)
        custom["data_vendors"]["core_stock_apis"] = "alpha_vantage"
        custom["tool_vendors"]["get_stock_data"] = "alpha_vantage"

        set_config(custom)

        custom["data_vendors"]["core_stock_apis"] = "yfinance"
        custom["tool_vendors"]["get_stock_data"] = "yfinance"

        fresh = get_config()
        self.assertEqual(fresh["data_vendors"]["core_stock_apis"], "alpha_vantage")
        self.assertEqual(fresh["tool_vendors"]["get_stock_data"], "alpha_vantage")

    def test_partial_nested_update_preserves_existing_defaults(self):
        set_config(
            {
                "data_vendors": {
                    "core_stock_apis": "alpha_vantage",
                }
            }
        )

        fresh = get_config()
        self.assertEqual(fresh["data_vendors"]["core_stock_apis"], "alpha_vantage")
        self.assertEqual(fresh["data_vendors"]["technical_indicators"], "yfinance")
        self.assertEqual(fresh["data_vendors"]["fundamental_data"], "yfinance")
        self.assertEqual(fresh["data_vendors"]["news_data"], "yfinance")

    def test_nested_dict_updates_merge_one_level_deep(self):
        set_config({"tool_vendors": {"get_stock_data": "alpha_vantage"}})
        set_config({"tool_vendors": {"get_news": "alpha_vantage"}})

        fresh = get_config()
        self.assertEqual(fresh["tool_vendors"]["get_stock_data"], "alpha_vantage")
        self.assertEqual(fresh["tool_vendors"]["get_news"], "alpha_vantage")


def test_parallel_run_configs_do_not_cross_vendor_settings():
    before = get_config()["data_vendors"]["news_data"]
    rendezvous = Barrier(2)

    def selected_vendor(vendor):
        with run_config({"data_vendors": {"news_data": vendor}}):
            rendezvous.wait(timeout=5)
            return get_config()["data_vendors"]["news_data"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(selected_vendor, "alpha_vantage")
        second = pool.submit(selected_vendor, "yfinance")
        assert first.result() == "alpha_vantage"
        assert second.result() == "yfinance"
    assert get_config()["data_vendors"]["news_data"] == before


def test_constructing_a_graph_does_not_replace_another_runs_vendor_config(tmp_path):
    before = get_config()["data_vendors"]["news_data"]
    config = copy.deepcopy(default_config.DEFAULT_CONFIG)
    config["data_vendors"]["news_data"] = "alpha_vantage"
    config["data_cache_dir"] = str(tmp_path / "cache")
    config["results_dir"] = str(tmp_path / "results")
    config["memory_log_path"] = str(tmp_path / "memory.md")
    with patch.object(TradingAgentsGraph, "_create_role_llm", return_value=object()):
        TradingAgentsGraph(selected_analysts=("market",), config=config)
    assert get_config()["data_vendors"]["news_data"] == before
