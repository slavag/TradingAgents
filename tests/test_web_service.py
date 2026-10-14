import datetime as dt
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

from cli.stats_handler import StatsCallbackHandler
from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.investor_deck import DeckSlide, InvestorDeck
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
    def test_web_ticker_input_resolves_trailing_sentence_period(self):
        self.assertEqual(
            web_service.normalize_tickers("AEHR, MXL, PENG."),
            ["AEHR", "MXL", "PENG"],
        )

    def test_serialize_result_preserves_target_validation_metadata(self):
        result = {
            "ticker": "TEST",
            "analysis_date": "2026-08-14",
            "decision": "Buy",
            "price_target": 999.0,
            "confidence_score": 88,
            "recommendation_confidence_score": 82,
            "run_id": "job-current",
            "snapshot_mode": "live_current_day",
            "evidence_fingerprint": "sha256:current",
            "comparison_status": "evidence_changed",
            "previous_run_id": "job-prior",
            "target_horizon": None,
            "target_summary": None,
            "supporting_quote": "Resistance: 120.",
            "target_validation_status": "Rejected",
            "target_rejection_reason": "supporting_quote_not_in_evidence",
            "thesis": "Bullish",
            "existing_position_action": "Hold",
            "existing_position_summary": "Keep a medium position.",
            "new_position_action": "Conditional Buy",
            "new_position_summary": "Wait for confirmation.",
            "conditional_confirmation": "Buy above 120.",
            "conditional_alternative": "Accumulate near 100.",
            "conditional_invalidation": "A break below 90 weakens the setup.",
            "results_dir": "/tmp/test",
            "report_path": "/tmp/test/report.md",
            "llm_usage": {
                "cache_read_tokens": 120, "cache_creation_tokens": 40,
                "cache_usage_reported_calls": 2, "cache_usage_unreported_calls": 1,
                "prompt": "private source text must not escape",
            },
            "final_state": {},
        }

        serialized = web_service.serialize_result(result)

        self.assertEqual(serialized["target_validation_status"], "Rejected")
        self.assertEqual(
            serialized["target_rejection_reason"],
            "supporting_quote_not_in_evidence",
        )
        self.assertIsNone(serialized["supporting_quote"])
        self.assertIsNone(serialized["price_target"])
        self.assertIsNone(serialized["confidence_score"])
        self.assertEqual(serialized["price_target_label"], "No validated target")
        self.assertEqual(serialized["recommendation_confidence_score"], 82)
        self.assertEqual(serialized["recommendation_confidence_label"], "82/100 (uncalibrated)")
        self.assertEqual(serialized["comparison_status"], "evidence_changed")
        self.assertEqual(serialized["previous_run_id"], "job-prior")
        self.assertEqual(serialized["llm_usage"]["cache_read_tokens"], 120)
        self.assertNotIn("prompt", str(serialized["llm_usage"]))
        self.assertEqual(
            serialized["target_status_label"],
            "Rejected: supporting quote not in evidence",
        )
        self.assertEqual(serialized["thesis"], "Bullish")
        self.assertEqual(serialized["existing_position_action"], "Hold")
        self.assertEqual(serialized["new_position_action"], "Conditional Buy")
        self.assertEqual(serialized["conditional_confirmation"], "Buy above 120.")

    def test_serialize_result_preserves_validated_range_context(self):
        result = {
            "ticker": "PENG", "analysis_date": "2026-10-09",
            "decision": "Hold", "final_state": {}, "results_dir": "/tmp/test",
            "report_path": "/tmp/test/report.md",
            "price_target": None, "target_low": 72.83, "target_high": 77.10,
            "confidence_score": 18, "target_horizon": "3 months",
            "target_summary": "Demand could revisit resistance.",
            "target_method": "Technical scenario",
            "target_risks": "Breakout may fail.",
            "supporting_quote_low": "| Low | 72.83 |",
            "supporting_quote_high": "| High | 77.10 |",
            "target_validation_status": "Accepted",
        }

        serialized = web_service.serialize_result(result)

        self.assertEqual((serialized["target_low"], serialized["target_high"]), (72.83, 77.10))
        self.assertEqual(serialized["target_method"], "Technical scenario")
        self.assertEqual(serialized["target_risks"], "Breakout may fail.")
        self.assertEqual(serialized["target_horizon"], "3 months")
        self.assertEqual(serialized["confidence_score"], 18)
        self.assertIn("72.83", serialized["price_target_label"])
        self.assertIn("77.10", serialized["price_target_label"])

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
            job_id = "c" * 32
            payload = {
                "tickers": "AAA, BBB",
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
            payload["save_reports"] = True
            payload["export_path"] = str(tmp_path / "export")
            save_report = MagicMock(wraps=web_service.save_report_to_disk)
            deck = InvestorDeck(
                ticker="AAA", analysis_date="2026-07-05", headline="AAA company research",
                synthesis_status="source_only", synthesis_reason="unsupported_interpretation",
                sources={}, slides=[DeckSlide(
                    topic="business", title="Business model", takeaway="Source-backed view",
                )],
            )
            def build_ticker_deck(_state, ticker, _date, _llm):
                if ticker == "BBB":
                    raise RuntimeError("optional deck synthesis failed")
                return deck

            build_deck = MagicMock(side_effect=build_ticker_deck)
            save_consolidated = MagicMock(return_value={
                "markdown": tmp_path / "consolidated.md",
                "html": tmp_path / "consolidated.html",
            })

            with web_service._JOB_LOCK:
                web_service._JOBS[job_id] = {
                    "id": job_id,
                    "status": "queued",
                    "created_at": "2026-07-05T00:00:00",
                    "updated_at": "2026-07-05T00:00:00",
                    "total": 2,
                    "completed": 0,
                    "tickers": ["AAA", "BBB"],
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
                patch.object(web_service, "RESULTS_ROOT", tmp_path),
                patch.object(web_service, "utc_report_timestamp", return_value="20261008T132500Z", create=True),
                patch.object(web_service, "build_investor_deck", build_deck, create=True),
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
                patch.object(web_service, "save_report_to_disk", save_report),
                patch.object(web_service, "build_consolidated_report", side_effect=build_markdown),
                patch.object(web_service, "build_consolidated_report_html", return_value="html"),
                patch.object(web_service, "save_consolidated_report", save_consolidated),
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
            self.assertEqual(save_report.call_count, 4)
            self.assertEqual(build_deck.call_count, 2)
            self.assertEqual(save_consolidated.call_count, 2)
            self.assertEqual(save_consolidated.call_args.kwargs["tickers"], ["AAA", "BBB"])
            self.assertEqual(
                save_consolidated.call_args.kwargs["report_timestamp"], "20261008T132500Z"
            )
            report_call = save_report.call_args_list[0]
            self.assertEqual(
                report_call.args[2],
                tmp_path
                / "AAA"
                / "2026-07-05"
                / "runs"
                / job_id
                / "web_report",
            )
            self.assertEqual(report_call.kwargs["run_metadata"]["run_id"], job_id)
            self.assertEqual(
                report_call.kwargs["run_metadata"]["snapshot_mode"],
                "historical_frozen",
            )
            self.assertEqual(
                save_report.call_args_list[2].args[2],
                tmp_path
                / "BBB"
                / "2026-07-05"
                / "runs"
                / job_id
                / "web_report",
            )
            for ticker in ("AAA", "BBB"):
                report_dir = tmp_path / ticker / "2026-07-05" / "runs" / job_id / "web_report"
                self.assertTrue((report_dir / f"{ticker}_full_20261008T132500Z.md").is_file())
                html_report = report_dir / f"{ticker}_full_20261008T132500Z.html"
                self.assertIn(
                    f'class="ticker">{ticker}</span>',
                    html_report.read_text(encoding="utf-8"),
                )
                investor_report = report_dir / f"{ticker}_investor_20261008T132500Z.html"
                investor_html = investor_report.read_text()
                if ticker == "AAA":
                    self.assertIn("AAA company research", investor_html)
                else:
                    self.assertIn("Research deck unavailable", investor_html)
                    self.assertIn(
                        f'/api/jobs/{job_id}/reports/BBB/full', investor_html,
                    )
                custom_dir = tmp_path / "export" / ticker
                self.assertTrue((custom_dir / f"{ticker}_full_20261008T132500Z.md").is_file())
                self.assertTrue((custom_dir / f"{ticker}_investor_20261008T132500Z.html").is_file())
            results = web_service.get_job(job_id)["results"]
            self.assertEqual(
                [(item["investor_deck_synthesis_status"], item["investor_deck_synthesis_reason"])
                 for item in results],
                [("source_only", "unsupported_interpretation"), ("unavailable", None)],
            )
            self.assertTrue(all(result["investor_report_path"] for result in results))
            self.assertTrue(all(result["custom_investor_report_path"] for result in results))
            self.assertEqual(
                web_service.get_job(job_id)["report_timestamp"], "20261008T132500Z"
            )

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
