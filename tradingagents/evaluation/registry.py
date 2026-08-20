"""Append-only filesystem registry for forecast evaluation artifacts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from tradingagents.evaluation.outcomes import ResolvedOutcome
from tradingagents.evaluation.scoring import ForecastScore


class EvaluationArtifact(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["outcome", "score"]
    record_id: str
    content_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    path: Path


def _canonical_payload(payload: dict) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


class EvaluationRegistry:
    """Persist immutable evaluation artifacts beside a report tree."""

    def __init__(self, report_tree: Path):
        self.report_tree = Path(report_tree)
        self.evaluation_dir = self.report_tree / "evaluation"

    def _write(self, kind: Literal["outcome", "score"], value: BaseModel) -> EvaluationArtifact:
        payload = value.model_dump(mode="json")
        canonical = _canonical_payload(payload)
        content_hash = f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"
        path = self.evaluation_dir / f"{kind}.json"
        self.evaluation_dir.mkdir(parents=True, exist_ok=True)
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise FileExistsError(
                    f"invalid immutable evaluation artifact: {path}"
                ) from exc
            if existing != payload:
                raise FileExistsError(
                    f"refusing to overwrite immutable evaluation artifact: {path}"
                )
        else:
            path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        return EvaluationArtifact(
            kind=kind,
            record_id=str(payload["record_id"]),
            content_hash=content_hash,
            path=path,
        )

    def write_outcome(self, outcome: ResolvedOutcome) -> EvaluationArtifact:
        return self._write("outcome", outcome)

    def write_score(self, score: ForecastScore) -> EvaluationArtifact:
        return self._write("score", score)

    def read_outcome(self) -> ResolvedOutcome | None:
        path = self.evaluation_dir / "outcome.json"
        if not path.exists():
            return None
        return ResolvedOutcome.model_validate_json(path.read_text(encoding="utf-8"))

    def read_score(self) -> ForecastScore | None:
        path = self.evaluation_dir / "score.json"
        if not path.exists():
            return None
        return ForecastScore.model_validate_json(path.read_text(encoding="utf-8"))
