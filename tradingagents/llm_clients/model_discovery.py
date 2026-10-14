"""Explicit, read-only model listing for fixed provider endpoints.

Listing never sends prompts and never trusts a caller-supplied URL. The caller
keeps curated/custom choices when a provider cannot be listed.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal
from urllib.parse import urlsplit

import requests

from tradingagents.llm_clients.api_key_env import PROVIDER_API_KEY_ENV

_MAX_BODY_BYTES = 2 * 1024 * 1024
_MAX_MODELS = 250
_MAX_PAGES = 5
_CACHE_SECONDS = 300
_TIMEOUT = (3, 7)

_NON_CHAT_MARKERS = (
    "embed", "moderation", "whisper", "transcri", "speech", "tts", "audio",
    "image", "dall-e", "rerank", "guard", "safety", "realtime", "vision-encoder",
)


@dataclass(frozen=True)
class DiscoveryResult:
    provider: str
    status: Literal["fresh", "cached", "unavailable", "unsupported"]
    models: tuple[str, ...]
    fetched_at: str | None
    warning: str | None


@dataclass(frozen=True)
class ListingAdapter:
    provider: str
    endpoint: str

    def list_models(self, *, force: bool = False) -> DiscoveryResult:
        key_name = PROVIDER_API_KEY_ENV[self.provider]
        credential = os.environ.get(key_name, "").strip() if key_name else ""
        if key_name and not credential:
            return _failure(self.provider, "missing_credentials")

        endpoint = self.endpoint
        if self.provider == "ollama":
            endpoint = _ollama_listing_url()
            if endpoint is None:
                return DiscoveryResult(self.provider, "unsupported", (), None, "listing_not_supported")

        fingerprint = hashlib.sha256(credential.encode("utf-8")).hexdigest()
        cache_key = (self.provider, endpoint, fingerprint)
        cached = _CACHE.get(cache_key)
        if not force and cached and cached[0] > time.monotonic():
            prior = cached[1]
            return DiscoveryResult(self.provider, "cached", prior.models, prior.fetched_at, None)

        try:
            models = _list_pages(self.provider, endpoint, credential)
        except _ListingFailure as exc:
            return _failure(self.provider, exc.warning)
        except requests.RequestException:
            return _failure(self.provider, "provider_unavailable")
        result = DiscoveryResult(
            self.provider, "fresh", models,
            datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            None,
        )
        _CACHE[cache_key] = (time.monotonic() + _CACHE_SECONDS, result)
        return result


class _ListingFailure(Exception):
    def __init__(self, warning: str):
        self.warning = warning
        super().__init__(warning)


def _failure(provider: str, warning: str) -> DiscoveryResult:
    return DiscoveryResult(provider, "unavailable", (), None, warning)


def _ollama_listing_url() -> str | None:
    raw = os.environ.get("OLLAMA_BASE_URL") or "http://localhost:11434/v1"
    try:
        parsed = urlsplit(raw)
        if (
            parsed.scheme != "http"
            or parsed.hostname not in {"localhost", "127.0.0.1", "::1"}
            or parsed.port != 11434
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path.rstrip("/") not in {"", "/v1"}
            or parsed.query
            or parsed.fragment
        ):
            return None
    except ValueError:
        return None
    host = "[::1]" if parsed.hostname == "::1" else "127.0.0.1"
    return f"http://{host}:11434/api/tags"


def _headers(provider: str, credential: str) -> dict[str, str]:
    if provider == "anthropic":
        return {"x-api-key": credential, "anthropic-version": "2023-06-01"}
    if provider == "google":
        return {"x-goog-api-key": credential}
    if provider == "ollama":
        return {}
    return {"Authorization": f"Bearer {credential}"}


def _read_json(response: requests.Response) -> dict:
    if response.status_code in {401, 403}:
        raise _ListingFailure("unauthorized")
    if not 200 <= response.status_code < 300:
        raise _ListingFailure("provider_unavailable")
    try:
        declared_size = int(response.headers.get("Content-Length", "0"))
    except (TypeError, ValueError):
        declared_size = 0
    if declared_size > _MAX_BODY_BYTES:
        raise _ListingFailure("response_too_large")
    body = bytearray()
    for chunk in response.iter_content(chunk_size=64 * 1024):
        body.extend(chunk)
        if len(body) > _MAX_BODY_BYTES:
            raise _ListingFailure("response_too_large")
    try:
        parsed = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise _ListingFailure("malformed_response") from exc
    if not isinstance(parsed, dict):
        raise _ListingFailure("malformed_response")
    return parsed


def _items(provider: str, payload: dict) -> list[dict]:
    field = "models" if provider in {"google", "ollama", "xai"} else "data"
    values = payload.get(field)
    if provider == "xai" and values is None:
        values = payload.get("data")
    if not isinstance(values, list) or any(not isinstance(item, dict) for item in values):
        raise _ListingFailure("malformed_response")
    return values


def _model_id(provider: str, item: dict) -> str | None:
    raw = item.get("name") if provider in {"google", "ollama"} else item.get("id")
    if not isinstance(raw, str):
        return None
    model_id = raw.removeprefix("models/") if provider == "google" else raw
    if (
        not model_id
        or len(model_id) > 200
        or model_id.strip() != model_id
        or any(ord(character) < 32 for character in model_id)
    ):
        return None
    lower = model_id.lower()
    if any(marker in lower for marker in _NON_CHAT_MARKERS):
        return None
    if provider == "openai" and not re.match(r"^(?:gpt-|o[1-9](?:-|$)|chatgpt-)", lower):
        return None
    if provider == "google":
        methods = item.get("supportedGenerationMethods")
        if methods is not None and not isinstance(methods, list):
            raise _ListingFailure("malformed_response")
        if not lower.startswith("gemini-") or (methods is not None and "generateContent" not in methods):
            return None
    if provider == "groq" and item.get("active") is False:
        return None
    if provider == "openrouter":
        architecture = item.get("architecture", {})
        if not isinstance(architecture, dict):
            raise _ListingFailure("malformed_response")
        modality = architecture.get("modality")
        if isinstance(modality, str) and not modality.endswith("->text"):
            return None
        outputs = architecture.get("output_modalities")
        if outputs is not None:
            if not isinstance(outputs, list) or any(not isinstance(value, str) for value in outputs):
                raise _ListingFailure("malformed_response")
            if "text" not in outputs:
                return None
    return model_id


def _next_page(provider: str, payload: dict) -> tuple[str, str] | None:
    if provider == "anthropic" and payload.get("has_more"):
        token = payload.get("last_id")
        if not isinstance(token, str) or not token:
            raise _ListingFailure("malformed_response")
        return "after_id", token
    if provider == "google":
        token = payload.get("nextPageToken")
        if isinstance(token, str) and token:
            return "pageToken", token
    return None


def _list_pages(provider: str, endpoint: str, credential: str) -> tuple[str, ...]:
    models: list[str] = []
    seen: set[str] = set()
    page_tokens: set[str] = set()
    params: dict[str, str] = {}
    for _ in range(_MAX_PAGES):
        with requests.get(
            endpoint, headers=_headers(provider, credential), params=params or None,
            timeout=_TIMEOUT, allow_redirects=False, stream=True,
        ) as response:
            payload = _read_json(response)
        for item in _items(provider, payload):
            model_id = _model_id(provider, item)
            if model_id and model_id not in seen:
                models.append(model_id)
                seen.add(model_id)
                if len(models) >= _MAX_MODELS:
                    return tuple(models)
        next_page = _next_page(provider, payload)
        if next_page is None:
            return tuple(models)
        parameter, token = next_page
        if token in page_tokens:
            raise _ListingFailure("malformed_response")
        page_tokens.add(token)
        params = {parameter: token}
    return tuple(models)


DISCOVERY_ADAPTERS = {
    provider: ListingAdapter(provider, endpoint)
    for provider, endpoint in {
        "openai": "https://api.openai.com/v1/models",
        "anthropic": "https://api.anthropic.com/v1/models",
        "google": "https://generativelanguage.googleapis.com/v1beta/models",
        "xai": "https://api.x.ai/v1/language-models",
        "deepseek": "https://api.deepseek.com/models",
        "meta": "https://api.meta.ai/v1/models",
        "openrouter": "https://openrouter.ai/api/v1/models",
        "mistral": "https://api.mistral.ai/v1/models",
        "groq": "https://api.groq.com/openai/v1/models",
        "ollama": "http://127.0.0.1:11434/api/tags",
    }.items()
}

_CACHE: dict[tuple[str, str, str], tuple[float, DiscoveryResult]] = {}


def discover_models(provider: str, *, force: bool = False) -> DiscoveryResult:
    """List account-visible IDs without making a generation request."""
    key = provider.strip().lower()
    if key not in PROVIDER_API_KEY_ENV:
        raise ValueError("unknown_provider")
    adapter = DISCOVERY_ADAPTERS.get(key)
    if adapter is None:
        return DiscoveryResult(key, "unsupported", (), None, "listing_not_supported")
    return adapter.list_models(force=force)
