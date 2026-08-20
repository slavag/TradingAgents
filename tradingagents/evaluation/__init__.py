"""Deterministic forecast outcome resolution and scoring."""

from tradingagents.evaluation.calibration import (
    CalibrationBin,
    CalibrationSummary,
    summarize_calibration,
)
from tradingagents.evaluation.leaderboard import (
    ConfigurationIdentity,
    LeaderboardEntry,
    RoleLeaderboard,
    build_role_leaderboard,
)
from tradingagents.evaluation.outcomes import (
    OutcomeResolutionStatus,
    PriceObservation,
    ResolvedOutcome,
    resolve_forecast_outcome,
)
from tradingagents.evaluation.registry import EvaluationArtifact, EvaluationRegistry
from tradingagents.evaluation.scoring import (
    ForecastScore,
    RealizedDirection,
    score_forecast,
)

__all__ = [
    "CalibrationBin",
    "CalibrationSummary",
    "ConfigurationIdentity",
    "EvaluationArtifact",
    "EvaluationRegistry",
    "OutcomeResolutionStatus",
    "LeaderboardEntry",
    "PriceObservation",
    "ForecastScore",
    "RealizedDirection",
    "ResolvedOutcome",
    "RoleLeaderboard",
    "resolve_forecast_outcome",
    "score_forecast",
    "build_role_leaderboard",
    "summarize_calibration",
]
