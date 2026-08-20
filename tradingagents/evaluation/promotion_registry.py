"""Append-only persistence for promoted role model leaderboards."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import ValidationError

from tradingagents.evaluation.leaderboard import ConfigurationIdentity, RoleLeaderboard

_ROLES = ("quick", "deep", "verifier")


def _validate_role(role: str) -> str:
    normalized = role.strip().lower()
    if normalized not in _ROLES:
        raise ValueError(f"role must be one of: {', '.join(_ROLES)}")
    return normalized


class ModelPromotionRegistry:
    """Store one immutable promoted leaderboard per runtime role."""

    def __init__(self, root: Path):
        self.root = Path(root)

    def write_leaderboard(self, leaderboard: RoleLeaderboard) -> Path:
        role = _validate_role(leaderboard.role)
        path = self.root / f"{role}.json"
        payload = leaderboard.model_dump(mode="json")
        self.root.mkdir(parents=True, exist_ok=True)
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise FileExistsError(
                    f"invalid immutable promoted leaderboard: {path}"
                ) from exc
            if existing != payload:
                raise FileExistsError(
                    f"refusing to overwrite immutable promoted leaderboard: {path}"
                )
            return path
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return path

    def read_leaderboard(self, role: str) -> RoleLeaderboard | None:
        role = _validate_role(role)
        path = self.root / f"{role}.json"
        if not path.exists():
            return None
        try:
            leaderboard = RoleLeaderboard.model_validate_json(
                path.read_text(encoding="utf-8")
            )
        except (OSError, ValidationError) as exc:
            raise ValueError(f"invalid promoted leaderboard: {path}") from exc
        if leaderboard.role != role:
            raise ValueError(f"invalid promoted leaderboard role: {path}")
        return leaderboard

    def selected_defaults(
        self,
        fallbacks: dict[str, ConfigurationIdentity],
    ) -> dict[str, ConfigurationIdentity]:
        """Resolve promoted identities, falling back only to explicit configuration."""
        selected = {}
        for role in _ROLES:
            fallback = fallbacks.get(role)
            if fallback is None or fallback.role != role:
                raise ValueError(f"explicit fallback is required for role: {role}")
            leaderboard = self.read_leaderboard(role)
            if leaderboard is None:
                selected[role] = fallback
                continue
            match = next(
                (
                    entry.configuration
                    for entry in leaderboard.entries
                    if entry.configuration.configuration_id
                    == leaderboard.selected_configuration_id
                ),
                None,
            )
            if match is None:
                raise ValueError(f"selected configuration missing from {role} leaderboard")
            selected[role] = match
        return selected
