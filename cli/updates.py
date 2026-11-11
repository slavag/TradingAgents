"""Explicit, cached upstream release lookup; never called during startup."""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from pathlib import Path

import requests

RELEASES_URL = "https://api.github.com/repos/TauricResearch/TradingAgents/releases/latest"
CACHE_TTL_SECONDS = 24 * 60 * 60
REQUEST_TIMEOUT_SECONDS = 5
UPSTREAM_INTEGRATION_TARGET = "v0.7.1"


def _valid_tag(value) -> bool:
    return isinstance(value, str) and re.fullmatch(
        r"v\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?", value
    ) is not None


def latest_upstream_release(
    *, cache_path: Path | None = None, now: float | None = None,
    request_get=None, refresh: bool = False,
) -> str:
    """Return a validated release tag from a bounded request or fresh local cache."""
    cache_path = cache_path or Path.home() / ".tradingagents" / "cache" / "upstream-release.json"
    now = time.time() if now is None else now
    if not refresh:
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            age = now - cached["checked_at"]
            if 0 <= age <= CACHE_TTL_SECONDS and _valid_tag(cached.get("tag")):
                return cached["tag"]
        except (OSError, ValueError, KeyError, TypeError):
            pass

    fetch = request_get or requests.get
    response = fetch(RELEASES_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    tag = payload.get("tag_name") if isinstance(payload, dict) else None
    if not _valid_tag(tag):
        raise ValueError("upstream release tag was invalid")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = cache_path.with_name(f".{cache_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump({"tag": tag, "checked_at": now}, handle)
        os.replace(temporary, cache_path)
    finally:
        temporary.unlink(missing_ok=True)
    return tag
