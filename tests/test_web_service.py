import datetime as dt
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

from cli.stats_handler import StatsCallbackHandler
from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.web import service as web_service
from tradingagents.web.service import _build_source_only_speaking_records


class _FakeGraph:
    final_report_llm = object()
    instances = []

    def __init__(self, *args, **kwargs):
        self.stream_calls = []
        self.__class__.instances.append(self)

    def stream(self, ticker, analysis_date, *, asset_type="stock", callbacks=None):
        self.stream_calls.append(
            {
                "ticker": ticker,
                "analysis_date": analysis_date,
                "asset_type": asset_type,
                "callbacks": callbacks,
            }
        )
        yield {
            "messages": [],
            "market_report": "Market report.",
            "sentiment_report": "Social report.",
            "news_report": "News report.",
            "fundamentals_report": "Fundamentals report.",
            "investment_debate_state": {"judge_decision": "Research decision."},
            "trader_investment_plan": "Trader plan.",
            "risk_debate_state": {"judge_decision": "Portfolio decision."},
            "final_trade_decision": "Buy",
        }

    def process_signal(self, signal):
        return "Buy"


class _EmptyUnderlyingStreamGraph(TradingAgentsGraph):
    final_report_llm = object()

    def __init__(self, selected_analysts, *, config, debug, callbacks):
        self.selected_analysts = tuple(selected_analysts)
        self.config = config
        self.debug = debug
        self.callbacks = callbacks
        self.curr_state = None
        self.ticker = None
        self.memory_log = MagicMock()
        self.memory_log.get_past_context.return_value = ""
        self.resolve_instrument_context = MagicMock(return_value="AAA identity")
        self._resolve_pending_entries = MagicMock()
        self._log_state = MagicMock()
        self.process_signal = MagicMock(return_value="Hold")
        self.propagator = MagicMock()
        self.propagator.create_initial_state.return_value = {}
        self.propagator.get_graph_args.return_value = {
            "stream_mode": "values",
            "config": {},
        }
        self.graph = MagicMock()
        self.graph.stream.return_value = iter(())
        self.workflow = MagicMock()


class WebServiceTests(unittest.TestCase):
    def test_web_graph_config_defaults_to_repeatable_temperature(self):
        config = web_service.build_graph_config({})

        self.assertEqual(config["temperature"], 0.0)

    def test_web_graph_config_accepts_explicit_temperature(self):
        config = web_service.build_graph_config({"temperature": 0.25})

        self.assertEqual(config["temperature"], 0.25)

    def test_source_only_records_keep_tape_populated_when_prices_are_empty(self):
        records = _build_source_only_speaking_records(
            ["BNED", "RUNN"],
            {
                "BNED": ["Stock Traders Daily News Release"],
                "RUNN": ["Stock Traders Daily News Release", "ApeWisdom"],
            },
            top_n=10,
            lookback_days=30,
        )

        self.assertEqual([record["ticker"] for record in records], ["RUNN", "BNED"])
        self.assertEqual(records[0]["source_count"], 2)
        self.assertEqual(records[0]["score"], 2.0)
        self.assertIsNone(records[0]["price"])
        self.assertEqual(records[0]["lookback_days"], 30)
        self.assertFalse(records[0]["market_open"])

    def test_us_market_open_detection_uses_regular_session(self):
        self.assertTrue(
            web_service._is_us_market_open(
                dt.datetime(2026, 7, 6, 15, 0, tzinfo=dt.timezone.utc)
            )
        )
        self.assertFalse(
            web_service._is_us_market_open(
                dt.datetime(2026, 7, 5, 15, 0, tzinfo=dt.timezone.utc)
            )
        )

    def test_job_enters_final_report_state_before_consolidated_generation(self):
        with TemporaryDirectory() as tmpdir:
            _FakeGraph.instances.clear()
            job_id = "job-final-report-state"
            payload = {
                "tickers": "AAA",
                "analysis_date": "2026-07-05",
                "analysts": ["market"],
                "research_depth": 1,
                "quick_provider": "openai",
                "quick_thinker": "gpt-5.4-mini",
                "deep_provider": "openai",
                "deep_thinker": "gpt-5.4",
                "final_report_provider": "openai",
                "final_report_model": "gpt-5.4-mini",
                "save_reports": False,
            }
            observed = {}
            tmp_path = Path(tmpdir)

            with web_service._JOB_LOCK:
                web_service._JOBS[job_id] = {
                    "id": job_id,
                    "status": "queued",
                    "created_at": "2026-07-05T00:00:00",
                    "updated_at": "2026-07-05T00:00:00",
                    "total": 1,
                    "completed": 0,
                    "tickers": ["AAA"],
                    "analysis_date": "2026-07-05",
                    "current_ticker": None,
                    "progress_message": "Queued.",
                    "results": [],
                    "progress_rows": [],
                    "recent_events": [],
                    "current_report": None,
                    "consolidated_markdown": None,
                    "consolidated_html": None,
                    "consolidated_paths": None,
                    "error": None,
                }

            def build_markdown(results, analysis_date, summary_llm=None):
                snapshot = web_service.get_job(job_id)
                observed["current_ticker"] = snapshot["current_ticker"]
                observed["progress_message"] = snapshot["progress_message"]
                observed["summary_llm"] = summary_llm
                return "markdown"

            with (
                patch.object(web_service, "TradingAgentsGraph", _FakeGraph),
                patch.object(
                    web_service,
                    "estimate_target_profile",
                    return_value={
                        "price_target": 101.0,
                        "confidence_score": 75,
                        "target_horizon": "1 month",
                        "target_summary": "Target summary.",
                        "reference_price": 100.0,
                    },
                ),
                patch.object(web_service, "save_report_to_disk", return_value=tmp_path / "ticker.md"),
                patch.object(web_service, "build_consolidated_report", side_effect=build_markdown),
                patch.object(web_service, "build_consolidated_report_html", return_value="html"),
                patch.object(
                    web_service,
                    "save_consolidated_report",
                    return_value={
                        "markdown": tmp_path / "consolidated.md",
                        "html": tmp_path / "consolidated.html",
                    },
                ),
            ):
                web_service._run_job(job_id, payload)

            self.assertIsNone(observed["current_ticker"])
            self.assertEqual(observed["progress_message"], "Building final report.")
            self.assertIsNone(observed["summary_llm"])
            stream_call = _FakeGraph.instances[0].stream_calls[0]
            self.assertEqual(stream_call["ticker"], "AAA")
            self.assertEqual(stream_call["analysis_date"], "2026-07-05")
            self.assertEqual(stream_call["asset_type"], "stock")
            self.assertEqual(len(stream_call["callbacks"]), 1)
            self.assertIsInstance(stream_call["callbacks"][0], StatsCallbackHandler)
            self.assertFalse(hasattr(web_service, "_stream_graph_in_analysis_context"))

    def test_empty_graph_execution_is_reported_as_failure_not_decision(self):
        with TemporaryDirectory() as tmpdir:
            job_id = "job-empty-graph"
            payload = {
                "tickers": "AAA",
                "analysis_date": "2026-01-10",
                "analysts": ["market"],
                "research_depth": 1,
                "quick_provider": "openai",
                "quick_thinker": "gpt-5.4-mini",
                "deep_provider": "openai",
                "deep_thinker": "gpt-5.4",
                "final_report_provider": "openai",
                "final_report_model": "gpt-5.4-mini",
                "save_reports": False,
            }
            with web_service._JOB_LOCK:
                web_service._JOBS[job_id] = {
                    "id": job_id,
                    "status": "queued",
                    "created_at": "2026-01-10T00:00:00",
                    "updated_at": "2026-01-10T00:00:00",
                    "total": 1,
                    "completed": 0,
                    "tickers": ["AAA"],
                    "analysis_date": "2026-01-10",
                    "current_ticker": None,
                    "progress_message": "Queued.",
                    "results": [],
                    "progress_rows": [],
                    "recent_events": [],
                    "current_report": None,
                    "consolidated_markdown": None,
                    "consolidated_html": None,
                    "consolidated_paths": None,
                    "error": None,
                }

            tmp_path = Path(tmpdir)
            with (
                patch.object(
                    web_service,
                    "TradingAgentsGraph",
                    _EmptyUnderlyingStreamGraph,
                ),
                patch.object(web_service, "build_graph_config", return_value={
                    "checkpoint_enabled": False,
                    "data_cache_dir": tmpdir,
                    "max_debate_rounds": 1,
                    "max_risk_discuss_rounds": 1,
                }),
                patch.object(web_service, "build_consolidated_report", return_value="markdown"),
                patch.object(web_service, "build_consolidated_report_html", return_value="html"),
                patch.object(
                    web_service,
                    "save_consolidated_report",
                    return_value={
                        "markdown": tmp_path / "consolidated.md",
                        "html": tmp_path / "consolidated.html",
                    },
                ),
            ):
                web_service._run_job(job_id, payload)

            result = web_service.get_job(job_id)["results"][0]
            self.assertIsNone(result["decision"])
            self.assertEqual(result["error_kind"], "runtime_error")
            self.assertEqual(
                result["error"],
                "Graph execution produced no value chunks for AAA on 2026-01-10.",
            )


if __name__ == "__main__":
    unittest.main()
