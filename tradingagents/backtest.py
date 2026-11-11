"""Independent historical analysis cells scored by the fork's forecast registry.

This runs one fresh graph per ticker/date. It evaluates saved decisions; it does
not simulate fills, cash flows, or a trading portfolio.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from tradingagents.dataflows.symbol_utils import normalize_symbol
from tradingagents.dataflows.utils import safe_ticker_component
from tradingagents.evaluation.registry import EvaluationRegistry
from tradingagents.evaluation.runtime import (
    EvaluationBatchSummary,
    EvaluationRunStatus,
    EvaluationScope,
    OutcomePriceProvider,
    YFinanceOutcomePriceProvider,
    evaluate_report_trees,
)
from tradingagents.forecasting.schemas import ForecastDecisionStatus, ForecastRecord
from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.portfolio.state import PortfolioContext
from tradingagents.reporting import write_report_tree

logger = logging.getLogger(__name__)


def _canonical_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("grid dates must be in YYYY-MM-DD format") from exc
    if parsed.isoformat() != value:
        raise ValueError("grid dates must be in YYYY-MM-DD format")
    return parsed


def iter_grid(start_date: str, end_date: str, every_n_days: int = 1) -> list[str]:
    start = _canonical_date(start_date)
    end = _canonical_date(end_date)
    if isinstance(every_n_days, bool) or every_n_days < 1:
        raise ValueError("every_n_days must be at least 1")
    if end < start:
        raise ValueError("the grid ends before it starts")
    last = min(end, date.today())
    result = []
    while start <= last:
        result.append(start.isoformat())
        start += timedelta(days=every_n_days)
    return result


def _new_run_id() -> str:
    return uuid.uuid4().hex


def _config_fingerprint(config: dict) -> str:
    """Pin every run setting without storing secrets or ephemeral output paths."""
    def canonical(value):
        if isinstance(value, dict):
            result = {}
            for key, child in value.items():
                if not isinstance(key, str):
                    raise ValueError("backtest config keys must be strings")
                if re.search(r"api.?key|token|secret|password|credential", key, re.I):
                    raise ValueError("backtest config must not contain inline secret fields")
                result[key] = canonical(child)
            return result
        if isinstance(value, (list, tuple)):
            return [canonical(child) for child in value]
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("backtest config numbers must be finite")
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        raise ValueError("backtest config contains an unsupported value")

    selected = {
        key: value for key, value in config.items()
        if key not in {"results_dir", "memory_log_path"}
    }
    text = json.dumps(
        canonical(selected), sort_keys=True, separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(text.encode()).hexdigest()


def _claim_run_dir(root: Path, run_id: str | None) -> tuple[str, Path, bool]:
    if root.is_symlink():
        raise ValueError("backtest results root cannot be a symlink")
    root.mkdir(parents=True, exist_ok=True)
    if run_id is not None:
        safe_ticker_component(run_id)
        path = root / run_id
        if path.is_symlink():
            raise ValueError("backtest run ID cannot be a symlink")
        existed = path.exists()
        path.mkdir(exist_ok=True)
        if path.resolve().parent != root.resolve():
            raise ValueError("backtest run ID escapes results root")
        return run_id, path, existed
    for _ in range(8):
        candidate = safe_ticker_component(_new_run_id())
        path = root / candidate
        try:
            path.mkdir()
        except FileExistsError:
            continue
        return candidate, path, False
    raise FileExistsError("could not claim a unique backtest run directory")


def _safe_run_child(root: Path, *components: str) -> Path:
    """Reject symlinked or escaped components under a claimed run directory."""
    if root.is_symlink():
        raise ValueError("backtest run path cannot be a symlink")
    boundary = root.resolve()
    path = root
    for component in components:
        path = path / component
        if path.is_symlink():
            raise ValueError("backtest child path cannot be a symlink")
        if not path.resolve().is_relative_to(boundary):
            raise ValueError("backtest child path escapes results root")
    return path


@dataclass
class BacktestResult:
    run_id: str
    run_dir: Path
    cells_run: int = 0
    skipped: int = 0
    failures: list[tuple[str, str, str]] = field(default_factory=list)
    settlement_failures: list[tuple[str, str, str]] = field(default_factory=list)
    evaluation: EvaluationBatchSummary | None = None


@dataclass(frozen=True)
class RatingScore:
    count: int
    hit_rate: float | None
    mean_gross_return: Decimal | None
    mean_excess_return: Decimal | None


@dataclass(frozen=True)
class BacktestSummary:
    resolved: int
    pending: int
    unscored: int
    by_rating: dict[str, RatingScore]
    retryable_errors: int = 0
    invalid: int = 0

    def render(self) -> str:
        lines = [
            f"Resolved: {self.resolved} · pending: {self.pending} · "
            f"provider errors: {self.retryable_errors} · invalid: {self.invalid} "
            f"· unscored: {self.unscored}",
        ]
        for rating, score in self.by_rating.items():
            hit = f"{score.hit_rate:.0%}" if score.hit_rate is not None else "n/a"
            gross = (
                f"{score.mean_gross_return:+.2%}"
                if score.mean_gross_return is not None else "n/a"
            )
            lines.append(
                f"- {rating}: {score.count} scored, direction hit rate {hit}, "
                f"mean gross return {gross}"
            )
        lines.append(
            "Forecast scores use each saved record's recorded reference price; "
            "memory reflections use the first subsequent close. These bases are not pooled."
        )
        lines.append(
            "Independent decisions only: no simulated fills, cash ledger, or "
            "portfolio return. Historical text coverage and model sampling may vary."
        )
        return "\n".join(lines)


def summarize(source: BacktestResult | str | Path) -> BacktestSummary:
    """Summarize immutable evaluation scores from one backtest report tree."""
    root = source.run_dir if isinstance(source, BacktestResult) else Path(source)
    if not root.is_dir():
        raise FileNotFoundError(f"no backtest report directory at {root}")
    scored = []
    pending = 0
    unscored = 0
    retryable_errors = 0
    invalid = 0
    evaluation = source.evaluation if isinstance(source, BacktestResult) else None
    eval_status = (
        {item.record_id: item.status for item in evaluation.results if item.record_id}
        if evaluation is not None else {}
    )
    for path in sorted(_safe_run_child(root, "cells").rglob("forecast_record.json")):
        try:
            forecast = ForecastRecord.model_validate_json(
                path.read_text(encoding="utf-8")
            )
        except Exception:
            unscored += 1
            continue
        if not _record_matches_cell(forecast, path):
            unscored += 1
            continue
        if (
            forecast.decision_status is not ForecastDecisionStatus.ACTIONABLE
            or forecast.reference_price is None
            or forecast.horizon_sessions is None
            or forecast.rating is None
        ):
            unscored += 1
            continue
        try:
            score = EvaluationRegistry(path.parent).read_score()
        except Exception:
            unscored += 1
            continue
        if score is None:
            status = eval_status.get(forecast.record_id)
            if status is EvaluationRunStatus.RETRYABLE_PROVIDER_ERROR:
                retryable_errors += 1
            elif status is EvaluationRunStatus.INVALID_RECORD:
                invalid += 1
            else:
                pending += 1
        elif score.record_id != forecast.record_id or score.gross_return is None:
            unscored += 1
        else:
            scored.append((forecast.rating, score))

    by_rating = {}
    for rating in sorted({rating for rating, _ in scored}):
        group = [score for group_rating, score in scored if group_rating == rating]
        comparable = [score.direction_correct for score in group if score.direction_correct is not None]
        gross = [score.gross_return for score in group if score.gross_return is not None]
        excess = [score.excess_return for score in group if score.excess_return is not None]
        by_rating[rating] = RatingScore(
            count=len(group),
            hit_rate=(sum(comparable) / len(comparable)) if comparable else None,
            mean_gross_return=sum(gross) / len(gross) if gross else None,
            mean_excess_return=sum(excess) / len(excess) if excess else None,
        )
    return BacktestSummary(
        resolved=len(scored), pending=pending, unscored=unscored,
        by_rating=by_rating,
        retryable_errors=max(retryable_errors, evaluation.retryable_errors)
        if evaluation is not None else retryable_errors,
        invalid=max(invalid, evaluation.invalid) if evaluation is not None else invalid,
    )


def _scorable_report_roots(run_dir: Path) -> tuple[Path, ...]:
    roots = []
    for path in sorted(_safe_run_child(run_dir, "cells").rglob("forecast_record.json")):
        try:
            record = ForecastRecord.model_validate_json(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if (
            _record_matches_cell(record, path)
            and
            record.decision_status is ForecastDecisionStatus.ACTIONABLE
            and record.reference_price is not None
            and record.horizon_sessions is not None
        ):
            roots.append(path.parent)
    return tuple(roots)


def _record_matches_cell(record: ForecastRecord, path: Path) -> bool:
    return (
        record.canonical_symbol == path.parent.name
        and record.data_cutoff.isoformat() == path.parent.parent.name
    )


def run_backtest(
    tickers: list[str],
    dates: list[str],
    config: dict,
    *,
    run_id: str | None = None,
    portfolio: PortfolioContext | dict | None = None,
    asset_type: str = "stock",
    selected_analysts=("market", "social", "news", "fundamentals"),
    progress: Callable[[int, int, str, str], None] | None = None,
    price_provider: OutcomePriceProvider | None = None,
) -> BacktestResult:
    """Run each unique cell once, resuming only a matching saved sweep."""
    book = PortfolioContext.model_validate(portfolio) if portfolio is not None else None
    symbols = tuple(dict.fromkeys(
        safe_ticker_component(normalize_symbol(ticker)) for ticker in tickers
    ))
    days = tuple(dict.fromkeys(_canonical_date(value).isoformat() for value in dates))
    if any(day > date.today().isoformat() for day in days):
        raise ValueError("backtest dates cannot be in the future")
    analysts = tuple(selected_analysts)
    config_fingerprint = _config_fingerprint(config)
    root = Path(config["results_dir"]).expanduser().resolve() / "backtest"
    claimed_id, run_dir, existed = _claim_run_dir(root, run_id)
    manifest = {
        "schema_version": 1,
        "tickers": symbols,
        "dates": days,
        "asset_type": asset_type,
        "selected_analysts": analysts,
        "portfolio_fingerprint": book.fingerprint() if book is not None else None,
        "config_fingerprint": config_fingerprint,
    }
    manifest_path = run_dir / "backtest_manifest.json"
    if existed:
        try:
            saved = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError("existing backtest run has no valid manifest") from exc
        if saved != json.loads(json.dumps(manifest)):
            raise ValueError("backtest resume inputs differ from the original run")
    else:
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    result = BacktestResult(run_id=claimed_id, run_dir=run_dir)
    cells = [(ticker, day) for ticker in symbols for day in days]
    for index, (ticker, day) in enumerate(cells, 1):
        cell_dir = _safe_run_child(run_dir, "cells", day, ticker)
        staging_root = _safe_run_child(run_dir, ".staging")
        if (cell_dir / "forecast_record.json").exists():
            result.skipped += 1
            continue
        if progress is not None:
            progress(index, len(cells), ticker, day)
        cell_dir.parent.mkdir(parents=True, exist_ok=True)
        _safe_run_child(run_dir, "cells", day, ticker)
        staging_root.mkdir(exist_ok=True)
        _safe_run_child(run_dir, ".staging")
        stage_dir = _safe_run_child(run_dir, ".staging", safe_ticker_component(_new_run_id()))
        stage_dir.mkdir()
        run_config = {
            **config,
            "results_dir": str(stage_dir / "work"),
            "memory_log_path": str(stage_dir / "memory.md"),
        }
        try:
            graph = TradingAgentsGraph(analysts, config=run_config)
            state, _signal = graph.propagate(
                ticker, day, asset_type=asset_type, portfolio=book,
            )
            write_report_tree(
                state, ticker, stage_dir,
                run_metadata={"backtest_run_id": claimed_id},
            )
            if not (stage_dir / "forecast_record.json").exists():
                raise ValueError("backtest report omitted forecast record")
            try:
                result.settlement_failures.extend(graph.settle_all_pending().failed)
            except Exception as exc:
                logger.warning(
                    "Backtest settlement for %s %s failed (%s)",
                    ticker, day, type(exc).__name__,
                )
                result.settlement_failures.append(
                    (ticker, day, type(exc).__name__)
                )
            if cell_dir.exists():
                raise FileExistsError("backtest cell was completed concurrently")
            _safe_run_child(run_dir, "cells", day, ticker)
            stage_dir.rename(cell_dir)
            result.cells_run += 1
        except Exception as exc:
            logger.warning(
                "Backtest cell %s %s failed (%s)", ticker, day, type(exc).__name__
            )
            result.failures.append((ticker, day, type(exc).__name__))

    roots = _scorable_report_roots(run_dir)
    if roots:
        provider = price_provider or YFinanceOutcomePriceProvider(
            available_through=date.today() - timedelta(days=1)
        )
        result.evaluation = evaluate_report_trees(
            run_dir, provider, scope=EvaluationScope(report_roots=roots)
        )
    return result
