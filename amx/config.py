"""Central configuration store shared across all AMX modules."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class DBConfig:
    host: str = "localhost"
    port: int = 5432
    user: str = "amx"
    password: str = "amx_pass"
    database: str = "SAP"

    @property
    def url(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass
class LLMConfig:
    provider: str = ""          # openai | anthropic | gemini | local | deepseek | …
    model: str = ""
    api_key: str = ""
    api_base: str | None = None
    temperature: float = 0.2
    max_tokens: int = 2048


def _db_from_mapping(m: dict[str, Any]) -> DBConfig:
    return DBConfig(
        host=str(m.get("host", "localhost")),
        port=int(m.get("port", 5432)),
        user=str(m.get("user", "amx")),
        password=str(m.get("password", "")),
        database=str(m.get("database", "SAP")),
    )


def _db_to_mapping(db: DBConfig) -> dict[str, Any]:
    return {
        "host": db.host,
        "port": db.port,
        "user": db.user,
        "password": db.password,
        "database": db.database,
    }


@dataclass
class AMXConfig:
    db: DBConfig = field(default_factory=DBConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    doc_paths: list[str] = field(default_factory=list)
    code_paths: list[str] = field(default_factory=list)
    selected_schemas: list[str] = field(default_factory=list)
    selected_tables: list[str] = field(default_factory=list)
    db_profiles: dict[str, DBConfig] = field(default_factory=dict)
    active_db_profile: str = "default"
    current_schema: str = ""
    current_table: str = ""

    CONFIG_DIR: str = field(
        default_factory=lambda: str(Path.home() / ".amx"), init=False
    )

    @classmethod
    def load(cls, path: str | None = None) -> "AMXConfig":
        cfg = cls()
        p = Path(path) if path else Path(cfg.CONFIG_DIR) / "config.yml"
        if p.exists():
            data: dict[str, Any] = yaml.safe_load(p.read_text()) or {}
            if "db" in data:
                for k, v in data["db"].items():
                    setattr(cfg.db, k, v)
            if "llm" in data:
                for k, v in data["llm"].items():
                    setattr(cfg.llm, k, v)
            cfg.doc_paths = data.get("doc_paths", [])
            cfg.code_paths = data.get("code_paths", [])
            cfg.selected_schemas = data.get("selected_schemas", [])
            cfg.selected_tables = data.get("selected_tables", [])

            profiles_raw = data.get("db_profiles") or {}
            if isinstance(profiles_raw, dict):
                for name, m in profiles_raw.items():
                    if isinstance(m, dict):
                        cfg.db_profiles[str(name)] = _db_from_mapping(m)

            cfg.active_db_profile = str(data.get("active_db_profile") or "default")
            cfg.current_schema = str(data.get("current_schema") or "")
            cfg.current_table = str(data.get("current_table") or "")

        cfg.llm.api_key = cfg.llm.api_key or os.getenv("AMX_LLM_API_KEY", "")

        # Back-compat: if profiles missing, derive from legacy `db` block.
        if not cfg.db_profiles:
            cfg.db_profiles["default"] = cfg.db
            cfg.active_db_profile = "default"
        else:
            # If config predates profiles but includes `db`, keep it as `default`
            # unless `default` already exists.
            if "default" not in cfg.db_profiles:
                cfg.db_profiles["default"] = cfg.db
            try:
                cfg.apply_active_db_profile()
            except Exception:
                # Fall back safely if active profile is invalid.
                cfg.active_db_profile = next(iter(cfg.db_profiles.keys()))
                cfg.db = cfg.db_profiles[cfg.active_db_profile]

        return cfg

    def save(self, path: str | None = None) -> Path:
        p = Path(path) if path else Path(self.CONFIG_DIR) / "config.yml"
        p.parent.mkdir(parents=True, exist_ok=True)
        # Persist current `db` into active profile so edits round-trip cleanly.
        if self.active_db_profile:
            self.db_profiles[self.active_db_profile] = self.db

        data = {
            "db": {
                "host": self.db.host,
                "port": self.db.port,
                "user": self.db.user,
                "password": self.db.password,
                "database": self.db.database,
            },
            "db_profiles": {k: _db_to_mapping(v) for k, v in self.db_profiles.items()},
            "active_db_profile": self.active_db_profile,
            "current_schema": self.current_schema,
            "current_table": self.current_table,
            "llm": {
                "provider": self.llm.provider,
                "model": self.llm.model,
                "api_key": self.llm.api_key,
                "api_base": self.llm.api_base,
                "temperature": self.llm.temperature,
                "max_tokens": self.llm.max_tokens,
            },
            "doc_paths": self.doc_paths,
            "code_paths": self.code_paths,
            "selected_schemas": self.selected_schemas,
            "selected_tables": self.selected_tables,
        }
        p.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
        return p

    def apply_active_db_profile(self) -> None:
        name = self.active_db_profile or "default"
        if name not in self.db_profiles and self.db_profiles:
            name = next(iter(self.db_profiles.keys()))
            self.active_db_profile = name
        if name in self.db_profiles:
            self.db = self.db_profiles[name]

    def set_active_db_profile(self, name: str) -> None:
        if name not in self.db_profiles:
            raise KeyError(f"Unknown DB profile: {name}")
        self.active_db_profile = name
        self.db = self.db_profiles[name]

    def upsert_db_profile(self, name: str, db: DBConfig) -> None:
        self.db_profiles[name] = db

    def remove_db_profile(self, name: str) -> None:
        if name not in self.db_profiles:
            raise KeyError(f"Unknown DB profile: {name}")
        if name == self.active_db_profile and len(self.db_profiles) == 1:
            raise ValueError("Cannot remove the last DB profile")
        del self.db_profiles[name]
        if self.active_db_profile == name:
            self.active_db_profile = next(iter(self.db_profiles.keys()))
            self.db = self.db_profiles[self.active_db_profile]
