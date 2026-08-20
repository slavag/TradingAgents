"""Append-only promoted model defaults derived from role leaderboards."""

from decimal import Decimal

import pytest
from typer.testing import CliRunner

from cli.main import app as cli_app
from tradingagents.evaluation.leaderboard import (
    ConfigurationIdentity,
    LeaderboardEntry,
    RoleLeaderboard,
)
from tradingagents.evaluation.promotion_registry import ModelPromotionRegistry
from tradingagents.web.app import _render_index_response
from tradingagents.web.service import get_role_model_status


def identity(configuration_id="deep-new", role="deep", model="gpt-5.6-terra"):
    return ConfigurationIdentity(
        configuration_id=configuration_id,
        role=role,
        provider="openai",
        model=model,
        prompt_hash="sha256:" + "a" * 64,
        config_hash="sha256:" + "b" * 64,
    )


def leaderboard(configuration=None):
    configuration = configuration or identity()
    return RoleLeaderboard(
        role=configuration.role,
        incumbent_configuration_id="deep-old",
        selected_configuration_id=configuration.configuration_id,
        entries=(
            LeaderboardEntry(
                rank=1,
                configuration=configuration,
                paired_coverage=Decimal("0.9"),
                direction_accuracy_delta=Decimal("0.05"),
                mean_excess_return_delta=Decimal("0.01"),
                brier_regression=Decimal("-0.02"),
                drawdown_regression=Decimal("-0.01"),
                promoted=True,
                is_incumbent=False,
                rejection_reasons=(),
            ),
        ),
    )


def test_promotion_registry_writes_reads_and_repeats_identical_leaderboard(tmp_path):
    registry = ModelPromotionRegistry(tmp_path)
    value = leaderboard()

    first = registry.write_leaderboard(value)
    second = registry.write_leaderboard(value)

    assert first == tmp_path / "deep.json"
    assert second == first
    assert registry.read_leaderboard("deep") == value


def test_promotion_registry_rejects_conflicting_or_corrupt_state(tmp_path):
    registry = ModelPromotionRegistry(tmp_path)
    registry.write_leaderboard(leaderboard())
    changed = leaderboard(identity(configuration_id="deep-other", model="gpt-5.6-sol"))

    with pytest.raises(FileExistsError, match="immutable promoted leaderboard"):
        registry.write_leaderboard(changed)

    corrupt_root = tmp_path / "corrupt"
    corrupt_root.mkdir()
    corrupt_root.joinpath("deep.json").write_text("not-json")
    with pytest.raises(ValueError, match="invalid promoted leaderboard"):
        ModelPromotionRegistry(corrupt_root).read_leaderboard("deep")


def test_promotion_registry_rejects_unknown_role(tmp_path):
    with pytest.raises(ValueError, match="role must be one of"):
        ModelPromotionRegistry(tmp_path).read_leaderboard("portfolio_manager")


def test_selected_defaults_use_promotion_and_explicit_fallbacks(tmp_path):
    registry = ModelPromotionRegistry(tmp_path)
    promoted = identity()
    registry.write_leaderboard(leaderboard(promoted))
    fallbacks = {
        "quick": identity("quick-default", role="quick", model="gpt-5.4-mini"),
        "deep": identity("deep-default", role="deep", model="gpt-5.5"),
        "verifier": identity("verifier-default", role="verifier", model="gpt-5.4-mini"),
    }

    selected = registry.selected_defaults(fallbacks)

    assert selected["deep"] == promoted
    assert selected["quick"] == fallbacks["quick"]
    assert selected["verifier"] == fallbacks["verifier"]


def test_role_model_status_reports_promoted_and_configured_sources(tmp_path):
    ModelPromotionRegistry(tmp_path).write_leaderboard(leaderboard())

    status = get_role_model_status(promotion_root=tmp_path)

    assert status["deep"]["source"] == "promoted"
    assert status["deep"]["model"] == "gpt-5.6-terra"
    assert status["quick"]["source"] == "configured_fallback"
    assert status["verifier"]["source"] == "configured_fallback"


def test_cli_imports_leaderboard_append_only(tmp_path):
    source = tmp_path / "leaderboard.json"
    source.write_text(leaderboard().model_dump_json(indent=2))
    destination = tmp_path / "promotions"

    result = CliRunner().invoke(
        cli_app,
        [
            "import-model-promotion",
            "--input",
            str(source),
            "--promotion-root",
            str(destination),
        ],
    )

    assert result.exit_code == 0
    assert ModelPromotionRegistry(destination).read_leaderboard("deep") == leaderboard()


def test_index_serializes_promoted_decimal_metrics(tmp_path):
    ModelPromotionRegistry(tmp_path).write_leaderboard(leaderboard())

    html = _render_index_response(promotion_root=tmp_path).body.decode("utf-8")

    assert '"source": "promoted"' in html
    assert '"paired_coverage": "0.9"' in html
