"""Deterministic role leaderboards built from paired promotion decisions."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from tradingagents.evaluation.walk_forward import PairedComparison

_HASH_PATTERN = r"^sha256:[0-9a-f]{64}$"


class ConfigurationIdentity(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    configuration_id: str = Field(min_length=1)
    role: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    model: str = Field(min_length=1)
    prompt_hash: str = Field(pattern=_HASH_PATTERN)
    config_hash: str = Field(pattern=_HASH_PATTERN)


class LeaderboardEntry(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    rank: int = Field(gt=0)
    configuration: ConfigurationIdentity
    paired_coverage: Decimal = Field(ge=0, le=1)
    direction_accuracy_delta: Decimal
    mean_excess_return_delta: Decimal
    brier_regression: Decimal
    drawdown_regression: Decimal
    promoted: bool
    is_incumbent: bool
    rejection_reasons: tuple[str, ...]


class RoleLeaderboard(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    role: str
    incumbent_configuration_id: str
    selected_configuration_id: str
    entries: tuple[LeaderboardEntry, ...]


def _passing_sort_key(comparison: PairedComparison):
    return (
        -comparison.direction_accuracy_delta,
        -comparison.mean_excess_return_delta,
        comparison.brier_regression,
        comparison.drawdown_regression,
        comparison.challenger_configuration_id,
    )


def build_role_leaderboard(
    role: str,
    comparisons: tuple[PairedComparison, ...],
    configurations: tuple[ConfigurationIdentity, ...],
    *,
    incumbent_configuration_id: str,
) -> RoleLeaderboard:
    """Rank passing challengers while retaining an explicit pinned incumbent."""
    by_id = {configuration.configuration_id: configuration for configuration in configurations}
    if len(by_id) != len(configurations):
        raise ValueError("configuration IDs must be unique")
    if incumbent_configuration_id not in by_id:
        raise ValueError("incumbent configuration is missing")
    if any(configuration.role != role for configuration in configurations):
        raise ValueError("configuration role does not match leaderboard role")

    passing = []
    for comparison in comparisons:
        if comparison.role != role:
            raise ValueError("comparison role does not match leaderboard role")
        if comparison.incumbent_configuration_id != incumbent_configuration_id:
            raise ValueError("comparison incumbent does not match leaderboard incumbent")
        if comparison.challenger_configuration_id not in by_id:
            raise ValueError("challenger configuration is missing")
        if comparison.decision.promoted:
            passing.append(comparison)

    passing.sort(key=_passing_sort_key)
    entries = []
    for index, comparison in enumerate(passing, start=1):
        entries.append(
            LeaderboardEntry(
                rank=index,
                configuration=by_id[comparison.challenger_configuration_id],
                paired_coverage=comparison.challenger_coverage,
                direction_accuracy_delta=comparison.direction_accuracy_delta,
                mean_excess_return_delta=comparison.mean_excess_return_delta,
                brier_regression=comparison.brier_regression,
                drawdown_regression=comparison.drawdown_regression,
                promoted=True,
                is_incumbent=False,
                rejection_reasons=(),
            )
        )

    entries.append(
        LeaderboardEntry(
            rank=len(entries) + 1,
            configuration=by_id[incumbent_configuration_id],
            paired_coverage=Decimal("1"),
            direction_accuracy_delta=Decimal("0"),
            mean_excess_return_delta=Decimal("0"),
            brier_regression=Decimal("0"),
            drawdown_regression=Decimal("0"),
            promoted=False,
            is_incumbent=True,
            rejection_reasons=(),
        )
    )
    selected = (
        passing[0].challenger_configuration_id
        if passing
        else incumbent_configuration_id
    )
    return RoleLeaderboard(
        role=role,
        incumbent_configuration_id=incumbent_configuration_id,
        selected_configuration_id=selected,
        entries=tuple(entries),
    )
