"""Local, owner-readable data-source credentials shared by web and CLI runs."""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path

from filelock import FileLock, Timeout as FileLockTimeout

DATA_SOURCE_FIELDS = {
    "SEC_USER_AGENT": ("SEC identification", False),
    "FMP_API_KEY": ("Financial Modeling Prep", True),
    "FRED_API_KEY": ("FRED", True),
    "ALPHA_VANTAGE_API_KEY": ("Alpha Vantage", True),
}
_EMAIL = re.compile(r"[^\s@]+@[^\s@]+\.[^\s@]+")


class SettingsValidationError(ValueError):
    """An invalid settings update without sensitive input in its message."""


class SettingsStorageError(RuntimeError):
    """Settings could not be loaded or saved."""


def settings_path() -> Path:
    directory = os.getenv("TRADINGAGENTS_SETTINGS_DIR")
    root = Path(directory).expanduser() if directory else Path.home() / ".tradingagents" / "settings"
    return root / "data_sources.json"


def _read(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SettingsStorageError("Could not read saved data-source settings.") from exc
    if not isinstance(payload, dict) or any(
        key not in DATA_SOURCE_FIELDS or not isinstance(value, str)
        for key, value in payload.items()
    ):
        raise SettingsStorageError("Saved data-source settings are invalid.")
    return payload


def get_data_source_setting(key: str) -> str | None:
    if key not in DATA_SOURCE_FIELDS:
        raise SettingsValidationError("Unknown data-source setting.")
    saved = _read(settings_path())
    value = saved.get(key) or os.getenv(key)
    return value.strip() if value and value.strip() else None


def credential_status() -> list[dict]:
    saved = _read(settings_path())
    result = []
    for key, (label, secret) in DATA_SOURCE_FIELDS.items():
        saved_value = saved.get(key, "").strip()
        env_value = os.getenv(key, "").strip()
        value = saved_value or env_value
        item = {
            "key": key,
            "label": label,
            "secret": secret,
            "configured": bool(value),
            "saved": bool(saved_value),
            "origin": "saved" if saved_value else "environment" if env_value else "missing",
        }
        if not secret:
            item["value"] = value
        result.append(item)
    return result


def update_credentials(values: dict, remove: list) -> list[dict]:
    if not isinstance(values, dict) or not isinstance(remove, list):
        raise SettingsValidationError("Invalid settings update.")
    if any(not isinstance(key, str) or key not in DATA_SOURCE_FIELDS for key in [*values, *remove]):
        raise SettingsValidationError("Unknown data-source setting.")
    if set(values) & set(remove):
        raise SettingsValidationError("A setting cannot be saved and removed together.")
    cleaned = {}
    for key, value in values.items():
        if not isinstance(value, str) or len(value) > 4096 or any(ord(c) < 32 for c in value):
            raise SettingsValidationError("Settings must be single-line text under 4096 characters.")
        value = value.strip()
        if not value:
            continue
        if key == "SEC_USER_AGENT" and (len(value) > 256 or not _EMAIL.search(value)):
            raise SettingsValidationError("SEC identification must include an application name and contact email.")
        cleaned[key] = value

    path = settings_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        expected_names = {path.name, path.name + ".lock"}
        if any(
            item.name not in expected_names and not item.name.startswith(".data-sources-")
            for item in path.parent.iterdir()
        ):
            raise SettingsStorageError("The settings directory is not dedicated to TradingAgents.")
        path.parent.chmod(0o700)
        with FileLock(str(path) + ".lock", timeout=5, mode=0o600):
            payload = _read(path)
            payload.update(cleaned)
            for key in remove:
                payload.pop(key, None)
            descriptor, temporary = tempfile.mkstemp(prefix=".data-sources-", dir=path.parent)
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                    json.dump(payload, handle, sort_keys=True, indent=2)
                    handle.write("\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, path)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
    except (OSError, FileLockTimeout) as exc:
        raise SettingsStorageError("Could not save data-source settings.") from exc
    return credential_status()
