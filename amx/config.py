"""Central configuration store shared across all AMX modules."""

from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import yaml


SUPPORTED_BACKENDS = ("postgresql", "snowflake", "databricks", "bigquery")
DISABLED_PROFILE = "__none__"


# ── Prompt Detail Levels ──────────────────────────────────────────────────────


@dataclass
class PromptDetail:
    """Controls which database context fields are included in every LLM prompt.

    Reducing detail lowers input-token cost; increasing detail may improve
    inference quality for ambiguous schemas. Use ``prompt_detail_for()`` to
    get a named preset, or construct your own by overriding individual flags.
    """

    # --- Column-level fields ---
    include_samples: bool = True       # Sample values per column
    max_samples: int = 3               # How many sample values to include (when enabled)
    include_null_counts: bool = True   # null_count / row_count
    include_min_max: bool = True       # min_val / max_val
    include_cardinality: bool = False  # distinct_count + cardinality_ratio
    include_existing_col_comment: bool = True  # existing DB comment on the column

    # --- Table-level fields ---
    include_pk_fk: bool = True         # Primary key + outgoing/incoming foreign keys
    include_unique_check: bool = False  # Unique constraints + check constraints
    include_usage_stats: bool = False  # seq_scan / idx_scan / n_live_tup from pg_stat
    include_schema_db_comments: bool = False  # Schema-level and database-level comments
    include_related_comments: bool = False    # Existing comments on FK-neighbour tables

    # --- RAG agent tuning ---
    rag_table_hits: int = 5   # Doc chunks fetched for the table-level query
    rag_col_hits: int = 1     # Doc chunks fetched per column query
    rag_max_chunks: int = 8   # Hard cap on total chunks injected into the RAG prompt


PROMPT_DETAIL_LEVELS = ("minimal", "standard", "detailed", "full")


def prompt_detail_for(level: str) -> PromptDetail:
    """Return a ``PromptDetail`` preset for the given level name.

    Presets (cheapest → most expensive):

    ``minimal``
        Column names + types + null counts only. No samples, no stats, no FK
        constraints, no RAG column queries. Fastest and cheapest.

    ``standard`` (default)
        Adds samples (3 per col), min/max, PK + FK keys, existing col comments,
        and light RAG retrieval. Good balance for most schemas.

    ``detailed``
        Adds cardinality ratio, distinct count, unique/check constraints,
        schema/DB comments, related FK comments, and deeper RAG retrieval.

    ``full``
        Everything — original AMX behaviour before preset support was added.
        Use this when you need maximum context regardless of token cost.
    """
    lv = (level or "standard").lower().strip()
    if lv == "minimal":
        return PromptDetail(
            include_samples=False,
            max_samples=0,
            include_null_counts=True,
            include_min_max=False,
            include_cardinality=False,
            include_existing_col_comment=True,
            include_pk_fk=True,
            include_unique_check=False,
            include_usage_stats=False,
            include_schema_db_comments=False,
            include_related_comments=False,
            rag_table_hits=3,
            rag_col_hits=0,
            rag_max_chunks=5,
        )
    if lv == "detailed":
        return PromptDetail(
            include_samples=True,
            max_samples=5,
            include_null_counts=True,
            include_min_max=True,
            include_cardinality=True,
            include_existing_col_comment=True,
            include_pk_fk=True,
            include_unique_check=True,
            include_usage_stats=True,
            include_schema_db_comments=True,
            include_related_comments=True,
            rag_table_hits=8,
            rag_col_hits=2,
            rag_max_chunks=12,
        )
    if lv == "full":
        return PromptDetail(
            include_samples=True,
            max_samples=5,
            include_null_counts=True,
            include_min_max=True,
            include_cardinality=True,
            include_existing_col_comment=True,
            include_pk_fk=True,
            include_unique_check=True,
            include_usage_stats=True,
            include_schema_db_comments=True,
            include_related_comments=True,
            rag_table_hits=5,
            rag_col_hits=2,
            rag_max_chunks=15,
        )
    # "standard" — default
    return PromptDetail(
        include_samples=True,
        max_samples=3,
        include_null_counts=True,
        include_min_max=True,
        include_cardinality=False,
        include_existing_col_comment=True,
        include_pk_fk=True,
        include_unique_check=False,
        include_usage_stats=False,
        include_schema_db_comments=False,
        include_related_comments=False,
        rag_table_hits=5,
        rag_col_hits=1,
        rag_max_chunks=8,
    )


@dataclass
class DBConfig:
    backend: str = "postgresql"

    # Common fields (PostgreSQL / generic)
    host: str = "localhost"
    port: int = 5432
    user: str = "amx"
    password: str = "amx_pass"
    database: str = "SAP"

    # Snowflake
    account: str = ""
    warehouse: str = ""
    role: str = ""

    # Databricks
    http_path: str = ""
    access_token: str = ""
    catalog: str = ""

    # BigQuery
    project: str = ""
    dataset: str = ""
    credentials_path: str = ""

    @property
    def url(self) -> str:
        if self.backend == "snowflake":
            url = (
                f"snowflake://{quote_plus(self.user)}:{quote_plus(self.password)}"
                f"@{self.account}/{self.database}"
            )
            params: list[str] = []
            if self.warehouse:
                params.append(f"warehouse={quote_plus(self.warehouse)}")
            if self.role:
                params.append(f"role={quote_plus(self.role)}")
            if params:
                url += "?" + "&".join(params)
            return url

        if self.backend == "databricks":
            token = self.access_token or self.password
            url = f"databricks://token:{quote_plus(token)}@{self.host}:443"
            if self.database:
                url += f"/{quote_plus(self.database)}"
            params = []
            if self.http_path:
                params.append(f"http_path={quote_plus(self.http_path)}")
            if self.catalog:
                params.append(f"catalog={quote_plus(self.catalog)}")
            if params:
                url += "?" + "&".join(params)
            return url

        if self.backend == "bigquery":
            url = f"bigquery://{self.project}"
            if self.dataset:
                url += f"/{self.dataset}"
            if self.credentials_path:
                url += f"?credentials_path={quote_plus(self.credentials_path)}"
            return url

        # Default: PostgreSQL
        return (
            f"postgresql://{quote_plus(self.user)}:{quote_plus(self.password)}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def display_summary(self) -> str:
        """Short human-readable connection summary for the UI."""
        if self.backend == "snowflake":
            return f"{self.database}@{self.account} (user {self.user})"
        if self.backend == "databricks":
            cat = f" catalog={self.catalog}" if self.catalog else ""
            return f"{self.host}{cat}"
        if self.backend == "bigquery":
            ds = f".{self.dataset}" if self.dataset else ""
            return f"{self.project}{ds}"
        return f"{self.database} @ {self.host}:{self.port} (user {self.user})"


# ── Serialization helpers ─────────────────────────────────────────────────


def _db_from_mapping(m: dict[str, Any]) -> DBConfig:
    backend = str(m.get("backend", "postgresql"))
    return DBConfig(
        backend=backend,
        host=str(m.get("host", "localhost")),
        port=int(m.get("port", 5432)),
        user=str(m.get("user", "amx")),
        password=str(m.get("password", "")),
        database=str(m.get("database", "SAP")),
        account=str(m.get("account", "")),
        warehouse=str(m.get("warehouse", "")),
        role=str(m.get("role", "")),
        http_path=str(m.get("http_path", "")),
        access_token=str(m.get("access_token", "")),
        catalog=str(m.get("catalog", "")),
        project=str(m.get("project", "")),
        dataset=str(m.get("dataset", "")),
        credentials_path=str(m.get("credentials_path", "")),
    )


def _db_to_mapping(db: DBConfig) -> dict[str, Any]:
    base: dict[str, Any] = {"backend": db.backend}

    if db.backend == "postgresql":
        base.update({
            "host": db.host, "port": db.port, "user": db.user,
            "password": db.password, "database": db.database,
        })
    elif db.backend == "snowflake":
        base.update({
            "account": db.account, "user": db.user, "password": db.password,
            "database": db.database, "warehouse": db.warehouse, "role": db.role,
        })
    elif db.backend == "databricks":
        base.update({
            "host": db.host, "http_path": db.http_path,
            "access_token": db.access_token, "catalog": db.catalog,
            "database": db.database,
        })
    elif db.backend == "bigquery":
        base.update({
            "project": db.project, "dataset": db.dataset,
            "credentials_path": db.credentials_path,
        })
    else:
        base.update({
            "host": db.host, "port": db.port, "user": db.user,
            "password": db.password, "database": db.database,
        })
    return base


@dataclass
class LLMConfig:
    provider: str = ""  # openai | anthropic | gemini | local | deepseek | …
    model: str = ""
    api_key: str = ""
    api_base: str | None = None
    temperature: float = 0.2
    max_tokens: int = 4096  # reduced from 16384; reasoning models raise this automatically
    completion_mode: str = "chat_completions"  # "chat_completions" | "batch"
    n_alternatives: int = 3   # how many description alternatives per column (1–5)
    prompt_detail: str = "standard"  # minimal | standard | detailed | full
    logprob_high: float = 0.85
    logprob_medium: float = 0.50

    @property
    def prompt_detail_cfg(self) -> PromptDetail:
        """Return the resolved PromptDetail dataclass for this config's level."""
        return prompt_detail_for(self.prompt_detail)


def _llm_from_mapping(m: dict[str, Any]) -> LLMConfig:
    n_alt = int(m.get("n_alternatives", 3))
    return LLMConfig(
        provider=str(m.get("provider", "")),
        model=str(m.get("model", "")),
        api_key=str(m.get("api_key", "")),
        api_base=m.get("api_base"),
        temperature=float(m.get("temperature", 0.2)),
        max_tokens=int(m.get("max_tokens", 4096)),
        completion_mode=str(m.get("completion_mode", "chat_completions")),
        n_alternatives=max(1, min(5, n_alt)),
        prompt_detail=str(m.get("prompt_detail", "standard")),
        logprob_high=float(m.get("logprob_high", 0.85)),
        logprob_medium=float(m.get("logprob_medium", 0.50)),
    )


def _llm_to_mapping(llm: LLMConfig) -> dict[str, Any]:
    return {
        "provider": llm.provider,
        "model": llm.model,
        "api_key": llm.api_key,
        "api_base": llm.api_base,
        "temperature": llm.temperature,
        "max_tokens": llm.max_tokens,
        "completion_mode": llm.completion_mode,
        "n_alternatives": llm.n_alternatives,
        "prompt_detail": llm.prompt_detail,
        "logprob_high": llm.logprob_high,
        "logprob_medium": llm.logprob_medium,
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
    llm_profiles: dict[str, LLMConfig] = field(default_factory=dict)
    active_llm_profile: str = "default"
    doc_profiles: dict[str, list[str]] = field(default_factory=dict)
    active_doc_profile: str = ""
    code_profiles: dict[str, str] = field(default_factory=dict)
    active_code_profile: str = ""

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
                    if hasattr(cfg.db, k):
                        setattr(cfg.db, k, v)
            if "llm" in data:
                for k, v in data["llm"].items():
                    setattr(cfg.llm, k, v)
            cfg.doc_paths = list(data.get("doc_paths", []) or [])
            cfg.code_paths = list(data.get("code_paths", []) or [])
            cfg.selected_schemas = list(data.get("selected_schemas", []) or [])
            cfg.selected_tables = list(data.get("selected_tables", []) or [])

            profiles_raw = data.get("db_profiles") or {}
            if isinstance(profiles_raw, dict):
                for name, m in profiles_raw.items():
                    if isinstance(m, dict):
                        cfg.db_profiles[str(name)] = _db_from_mapping(m)

            cfg.active_db_profile = str(data.get("active_db_profile") or "default")
            cfg.current_schema = str(data.get("current_schema") or "")
            cfg.current_table = str(data.get("current_table") or "")

            llm_prof_raw = data.get("llm_profiles") or {}
            if isinstance(llm_prof_raw, dict):
                for name, m in llm_prof_raw.items():
                    if isinstance(m, dict):
                        cfg.llm_profiles[str(name)] = _llm_from_mapping(m)

            cfg.active_llm_profile = str(data.get("active_llm_profile") or "default")

            doc_prof_raw = data.get("doc_profiles") or {}
            if isinstance(doc_prof_raw, dict):
                for name, paths in doc_prof_raw.items():
                    if isinstance(paths, list):
                        cfg.doc_profiles[str(name)] = [str(x) for x in paths]
                    elif isinstance(paths, str):
                        cfg.doc_profiles[str(name)] = [paths]

            cfg.active_doc_profile = str(data.get("active_doc_profile") or "")

            code_prof_raw = data.get("code_profiles") or {}
            if isinstance(code_prof_raw, dict):
                for name, path in code_prof_raw.items():
                    if isinstance(path, str):
                        cfg.code_profiles[str(name)] = path

            cfg.active_code_profile = str(data.get("active_code_profile") or "")

        cfg.llm.api_key = cfg.llm.api_key or os.getenv("AMX_LLM_API_KEY", "")

        if not cfg.db_profiles:
            cfg.db_profiles["default"] = cfg.db
            cfg.active_db_profile = "default"
        else:
            if "default" not in cfg.db_profiles:
                cfg.db_profiles["default"] = cfg.db
            try:
                cfg.apply_active_db_profile()
            except Exception:
                cfg.active_db_profile = next(iter(cfg.db_profiles.keys()))
                cfg.db = cfg.db_profiles[cfg.active_db_profile]

        if not cfg.llm_profiles:
            cfg.llm_profiles["default"] = replace(cfg.llm)
            cfg.active_llm_profile = "default"
        else:
            if "default" not in cfg.llm_profiles:
                cfg.llm_profiles["default"] = replace(cfg.llm)
            try:
                cfg.apply_active_llm_profile()
            except Exception:
                cfg.active_llm_profile = next(iter(cfg.llm_profiles.keys()))
                cfg.llm = replace(cfg.llm_profiles[cfg.active_llm_profile])

        if not cfg.doc_profiles and cfg.doc_paths:
            cfg.doc_profiles["default"] = list(cfg.doc_paths)
            if not cfg.active_doc_profile:
                cfg.active_doc_profile = "default"

        if not cfg.code_profiles and cfg.code_paths:
            for idx, p in enumerate(cfg.code_paths):
                key = "default" if idx == 0 else f"repo{idx}"
                cfg.code_profiles[key] = p
            if not cfg.active_code_profile and cfg.code_profiles:
                cfg.active_code_profile = "default" if "default" in cfg.code_profiles else next(
                    iter(cfg.code_profiles.keys())
                )

        return cfg

    def save(self, path: str | None = None) -> Path:
        p = Path(path) if path else Path(self.CONFIG_DIR) / "config.yml"
        p.parent.mkdir(parents=True, exist_ok=True)
        if self.active_db_profile:
            self.db_profiles[self.active_db_profile] = self.db
        if self.active_llm_profile:
            self.llm_profiles[self.active_llm_profile] = replace(self.llm)

        doc_paths_yaml = self._doc_paths_for_yaml()
        code_paths_yaml = self._code_paths_for_yaml()

        data = {
            "db": _db_to_mapping(self.db),
            "db_profiles": {k: _db_to_mapping(v) for k, v in self.db_profiles.items()},
            "active_db_profile": self.active_db_profile,
            "current_schema": self.current_schema,
            "current_table": self.current_table,
            "llm": _llm_to_mapping(self.llm),
            "llm_profiles": {k: _llm_to_mapping(v) for k, v in self.llm_profiles.items()},
            "active_llm_profile": self.active_llm_profile,
            "doc_paths": doc_paths_yaml,
            "doc_profiles": {k: list(v) for k, v in self.doc_profiles.items()},
            "active_doc_profile": self.active_doc_profile,
            "code_paths": code_paths_yaml,
            "code_profiles": dict(self.code_profiles),
            "active_code_profile": self.active_code_profile,
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

    def apply_active_llm_profile(self) -> None:
        name = self.active_llm_profile or "default"
        if name not in self.llm_profiles and self.llm_profiles:
            name = next(iter(self.llm_profiles.keys()))
            self.active_llm_profile = name
        if name in self.llm_profiles:
            self.llm = replace(self.llm_profiles[name])
            self.llm.api_key = self.llm.api_key or os.getenv("AMX_LLM_API_KEY", "")

    def set_active_llm_profile(self, name: str) -> None:
        if name not in self.llm_profiles:
            raise KeyError(f"Unknown LLM profile: {name}")
        self.active_llm_profile = name
        self.llm = replace(self.llm_profiles[name])
        self.llm.api_key = self.llm.api_key or os.getenv("AMX_LLM_API_KEY", "")

    def upsert_llm_profile(self, name: str, llm: LLMConfig) -> None:
        self.llm_profiles[name] = replace(llm)

    def remove_llm_profile(self, name: str) -> None:
        if name not in self.llm_profiles:
            raise KeyError(f"Unknown LLM profile: {name}")
        if name == self.active_llm_profile and len(self.llm_profiles) == 1:
            raise ValueError("Cannot remove the last LLM profile")
        del self.llm_profiles[name]
        if self.active_llm_profile == name:
            self.active_llm_profile = next(iter(self.llm_profiles.keys()))
            self.llm = replace(self.llm_profiles[self.active_llm_profile])

    def upsert_doc_profile(self, name: str, paths: list[str]) -> None:
        self.doc_profiles[name] = list(paths)

    def remove_doc_profile(self, name: str) -> None:
        if name not in self.doc_profiles:
            raise KeyError(f"Unknown document profile: {name}")
        del self.doc_profiles[name]
        if self.active_doc_profile == name:
            self.active_doc_profile = next(iter(self.doc_profiles.keys()), "")

    def upsert_code_profile(self, name: str, path: str) -> None:
        self.code_profiles[name] = path

    def remove_code_profile(self, name: str) -> None:
        if name not in self.code_profiles:
            raise KeyError(f"Unknown codebase profile: {name}")
        del self.code_profiles[name]
        if self.active_code_profile == name:
            self.active_code_profile = next(iter(self.code_profiles.keys()), "")

    def effective_doc_paths(self) -> list[str]:
        if self.doc_profiles:
            name = self.active_doc_profile
            if name == DISABLED_PROFILE:
                return []
            if name and name in self.doc_profiles:
                return list(self.doc_profiles[name])
            if "default" in self.doc_profiles:
                return list(self.doc_profiles["default"])
            key = sorted(self.doc_profiles.keys())[0]
            return list(self.doc_profiles[key])
        return list(self.doc_paths)

    def effective_code_paths(self) -> list[str]:
        if self.code_profiles:
            name = self.active_code_profile
            if name == DISABLED_PROFILE:
                return []
            if name and name in self.code_profiles:
                return [self.code_profiles[name]]
            if "default" in self.code_profiles:
                return [self.code_profiles["default"]]
            key = sorted(self.code_profiles.keys())[0]
            return [self.code_profiles[key]]
        return list(self.code_paths)

    def resolve_doc_paths(self, profile: str | None, cli_paths: list[str]) -> list[str]:
        """Paths for docs scan/ingest: explicit CLI paths, else named profile, else active effective paths."""
        if cli_paths:
            return list(cli_paths)
        if profile:
            if profile in {"none", DISABLED_PROFILE}:
                return []
            if profile not in self.doc_profiles:
                raise KeyError(f"Unknown document profile: {profile}")
            return list(self.doc_profiles[profile])
        return self.effective_doc_paths()

    def resolve_code_path(self, profile: str | None, cli_path: str | None) -> str | None:
        """Single codebase path: explicit path, or named profile, or active profile."""
        p = (cli_path or "").strip()
        if p:
            return p
        if profile:
            if profile in {"none", DISABLED_PROFILE}:
                return None
            if profile not in self.code_profiles:
                raise KeyError(f"Unknown codebase profile: {profile}")
            return self.code_profiles[profile]
        paths = self.effective_code_paths()
        return paths[0] if paths else None

    def _doc_paths_for_yaml(self) -> list[str]:
        """Legacy `doc_paths` key: mirror active (or only) document profile."""
        return self.effective_doc_paths()

    def _code_paths_for_yaml(self) -> list[str]:
        """Legacy `code_paths` key: mirror active codebase profile(s)."""
        return self.effective_code_paths()
