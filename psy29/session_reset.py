from __future__ import annotations

from copy import deepcopy
from datetime import date
from typing import Any


def reset_for_trading_date(state: dict[str, Any], trading_date: date) -> bool:
    """Clear live-session state when its trading date changes.

    Returns True only when a reset occurred. Previous-day reference data is
    intentionally not supplied or copied here; the normal Dhan acquisition
    path owns that field for the new session.
    """
    date_value = trading_date.isoformat()
    if state.get("trading_date") == date_value:
        return False

    state["trading_date"] = date_value
    state["stocks"] = {}
    state["security_map"] = {}
    state["access_token_expiry"] = None
    state["source_status"] = "WAITING_FOR_SESSION"
    state["market_session_status"] = "PRE_OPEN"
    return True


def checkpoint_is_for_trading_date(checkpoint: Any, trading_date: date) -> bool:
    """Return whether a checkpoint explicitly belongs to today's session."""
    if not isinstance(checkpoint, dict):
        return False
    expected = trading_date.isoformat()
    return checkpoint.get("trading_date") == expected
