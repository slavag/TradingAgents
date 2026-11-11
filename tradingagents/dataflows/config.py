from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy

import tradingagents.default_config as default_config

# Use default config but allow it to be overridden
_config: dict | None = None
_run_config: ContextVar[dict | None] = ContextVar(
    "tradingagents_run_config", default=None
)


def initialize_config():
    """Initialize the configuration with default values."""
    global _config
    if _config is None:
        _config = deepcopy(default_config.DEFAULT_CONFIG)


def set_config(config: dict):
    """Update the configuration with custom values.

    Dict-valued keys (e.g. ``data_vendors``) are merged one level deep so a
    partial update like ``{"data_vendors": {"core_stock_apis": "alpha_vantage"}}``
    keeps the other nested keys from the default; scalar keys are replaced.
    """
    global _config
    initialize_config()
    _merge(_config, config)


def _merge(base: dict, incoming: dict) -> dict:
    for key, value in deepcopy(incoming).items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key].update(value)
        else:
            base[key] = value
    return base


@contextmanager
def run_config(config: dict):
    """Bind a graph run's vendor and data settings to its execution context."""
    scoped = _merge(deepcopy(default_config.DEFAULT_CONFIG), config)
    token = _run_config.set(scoped)
    try:
        yield
    finally:
        _run_config.reset(token)


def get_config() -> dict:
    """Get the current configuration."""
    scoped = _run_config.get()
    if scoped is not None:
        return deepcopy(scoped)
    if _config is None:
        initialize_config()
    return deepcopy(_config)


# Initialize with default config
initialize_config()
