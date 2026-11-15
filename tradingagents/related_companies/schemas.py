"""Bounded discovery proposals. Every proposal is independently source-checked."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class RelationshipProof(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Literal["competitor", "supplier", "customer"]
    quote: str = Field(min_length=10, max_length=3000)
    source_id: str = Field(min_length=1, max_length=100)
    justification: str = Field(min_length=1, max_length=600)
    relationship_date: str


class RelatedCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=2, max_length=160)
    symbol: str | None = Field(default=None, max_length=24)
    proofs: list[RelationshipProof] = Field(default_factory=list, max_length=6)


class RelatedDiscovery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    candidates: list[RelatedCandidate] = Field(default_factory=list, max_length=20)
    coverage: list[str] = Field(default_factory=list, max_length=12)
