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


@dataclass
class AMXConfig:
    db: DBConfig = field(default_factory=DBConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    doc_paths: list[str] = field(default_factory=list)
    code_paths: list[str] = field(default_factory=list)
    selected_schemas: list[str] = field(default_factory=list)
    selected_tables: list[str] = field(default_factory=list)

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
        cfg.llm.api_key = cfg.llm.api_key or os.getenv("AMX_LLM_API_KEY", "")
        return cfg

    def save(self, path: str | None = None) -> Path:
        p = Path(path) if path else Path(self.CONFIG_DIR) / "config.yml"
        p.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "db": {
                "host": self.db.host,
                "port": self.db.port,
                "user": self.db.user,
                "password": self.db.password,
                "database": self.db.database,
            },
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
