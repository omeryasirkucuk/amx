"""Resolve the active StyleProfile from disk at agent-construction time.

The loader is intentionally tolerant: any failure (no config dir,
empty profile name, disabled flag, schema mismatch) returns None so
agents fall back to the no-style path.
"""

from __future__ import annotations

from pathlib import Path

from amx.llm.style.profile import StyleProfile


def load_active_style_profile() -> StyleProfile | None:
    try:
        from amx.config import AMXConfig
        from amx.storage.style_store import StyleStore

        cfg = AMXConfig.load()
        name = (getattr(cfg, "active_llm_profile", "") or "").strip()
        if not name:
            return None
        config_dir = getattr(cfg, "CONFIG_DIR", None) or str(Path.home() / ".amx")
        db_path = Path(config_dir) / "history.db"
        if not db_path.exists():
            return None
        row = StyleStore(db_path).get(name)
        if row is None or not row.enabled:
            return None
        return row.profile
    except Exception:
        return None
