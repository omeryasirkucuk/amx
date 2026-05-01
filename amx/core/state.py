"""Write-through state persistence for headless and CLI AMX sessions.

Part of the **public API** — see ``docs/PUBLIC_API.md`` for the
stability contract.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from amx.config import AMXConfig

__all__ = ["StateManager"]
from amx.storage.sqlite_store import SQLiteHistoryStore


@dataclass
class StateManager:
    """Persist config-backed and SQLite-backed state immediately."""

    config: AMXConfig
    store: SQLiteHistoryStore | None = None
    namespace: str = "default"

    def set_config(self, key: str, value: Any) -> None:
        """Set a top-level config attribute and immediately persist it."""
        if not hasattr(self.config, key):
            raise KeyError(f"Unknown config key: {key}")
        setattr(self.config, key, value)
        self.config.save()

    def set_session_state(self, key: str, value: Any) -> None:
        """Write an arbitrary agent/session state value to SQLite."""
        if self.store is None:
            raise RuntimeError("SQLite history store is not initialized")
        self.store.set_session_state(self.namespace, key, value)

    def get_session_state(self, key: str, default: Any = None) -> Any:
        if self.store is None:
            return default
        return self.store.get_session_state(self.namespace, key, default)

    def record_agent_state(self, agent_name: str, state: dict[str, Any]) -> None:
        payload = {
            "agent": agent_name,
            "state": state,
            "updated_at": time.time(),
        }
        self.set_session_state(f"agent:{agent_name}", payload)


def json_state_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)
