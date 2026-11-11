"""Human-readable daily prices without discarding sub-cent or pip precision."""

import math


def price_text(value: float) -> str:
    """Format about six significant digits, retaining at least two decimals."""
    if not math.isfinite(value):
        return str(value)
    decimals = 2 if value == 0 else max(2, 5 - math.floor(math.log10(abs(value))))
    whole, _, fraction = f"{value:.{decimals}f}".partition(".")
    return f"{whole}.{fraction.rstrip('0').ljust(2, '0')}"
