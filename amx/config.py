"""Central configuration store shared across all AMX modules.

INTERNAL — not part of the public API. Programmatic users should go
through :func:`amx.init` or :class:`amx.core.AMXApplication.load`,
which return a configured application without exposing the
``AMXConfig`` dataclass shape. The shape is **not** stable across
minor versions; see ``docs/PUBLIC_API.md``.

Two names *are* stable because they leak through the on-disk config
schema contract: :data:`CONFIG_SCHEMA_VERSION` and
:exc:`ConfigSchemaTooNewError` (used by callers who load configs
themselves and want to render an actionable upgrade message).
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field, replace
from difflib import get_close_matches
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import quote_plus

import yaml

from amx.storage.secrets import (
    SecretStore,
    get_default_store,
    is_secret_reference,
    make_reference,
    parse_reference,
)

SUPPORTED_BACKENDS = (
    "postgresql",
    "snowflake",
    "databricks",
    "bigquery",
    "mysql",
    "oracle",
    "mssql",
    "redshift",
    "clickhouse",
    "duckdb",
)
DISABLED_PROFILE = "__none__"
PROFILING_MODES = ("full", "sampled", "metadata")
SUPPORTED_EMBEDDING_KINDS = ("minilm", "openai_compatible", "sentence_transformers")
DEFAULT_EMBEDDING_KIND = "minilm"

# Pre-0.11 ``DBConfig.database`` shipped with a five-year-old demo default
# (``"SAP"``) that surfaced in the UI as a phantom "SAP @ localhost:5432"
# row for users who never finished setup. The default is now empty, but
# we still detect existing YAML configs that carry the legacy value so we
# can suggest the user clear it (we never mutate their YAML — see
# §3.5 of docs/design/multi-db-plan.md). The match is per-backend: the
# legacy default leaked only into PG / Snowflake; Databricks/BigQuery
# already used their own catalog/dataset fields.
_LEGACY_DATABASE_DEFAULTS: frozenset[tuple[str, str]] = frozenset(
    {
        ("postgresql", "SAP"),
        ("snowflake", "SAP"),
    }
)


def _normalize_db_host(raw: str | None) -> str:
    """Strip URL scheme, surrounding whitespace, and trailing path/slash from a DB host.

    Users routinely paste full workspace URLs like
    ``https://dbc-xxx.cloud.databricks.com/`` into a "host" prompt. The
    Databricks SQL connector and SQLAlchemy URL builder both want the
    bare hostname — anything else turns ``host:443`` into ``host/:443``
    and the SQLAlchemy URL parser then tries ``int("")`` for the port.
    """
    host = (raw or "").strip()
    if not host:
        return ""
    for scheme in ("https://", "http://"):
        if host.lower().startswith(scheme):
            host = host[len(scheme) :]
            break
    # Drop any path component the user accidentally included.
    host = host.split("/", 1)[0]
    return host.strip()


def has_legacy_database_default(db: DBConfig) -> bool:
    """Return True when *db* still carries the historical ``database='SAP'`` default.

    Used by the CLI to surface a one-time hint suggesting the user run
    ``/edit`` to clear the value. Never mutates the config.
    """
    return (db.backend, db.database) in _LEGACY_DATABASE_DEFAULTS


# Secret-bearing fields per scope. These are externalised to the OS keyring
# on save and resolved back to plaintext on load via amx.storage.secrets.
_DB_SECRET_FIELDS = ("password", "access_token")
_LLM_SECRET_FIELDS = ("api_key",)
_EMBEDDING_SECRET_FIELDS = ("api_key",)


def _externalise_secret(
    mapping: dict[str, Any],
    field_name: str,
    keyring_key: str,
    store: SecretStore,
) -> None:
    """Move plaintext ``mapping[field_name]`` to the keyring; store a reference instead.

    Idempotent: if the value is already a reference (or empty), nothing happens.
    """
    value = mapping.get(field_name, "")
    if not isinstance(value, str) or not value:
        return
    if is_secret_reference(value):
        return
    if not store.is_available():
        # Keyring unavailable: leave plaintext in YAML — the user will see a
        # warning at load time and can choose how to proceed.
        return
    store.set(keyring_key, value)
    mapping[field_name] = make_reference(keyring_key)


def _externalise_secrets_in_data(
    data: dict[str, Any],
    *,
    active_db_profile: str,
    active_llm_profile: str,
    store: SecretStore,
) -> dict[str, Any]:
    """Replace plaintext secret fields in ``data`` with keyring references.

    ``data`` is mutated in place. The top-level ``db`` / ``llm`` mappings
    mirror the active profile, so they reuse the active profile's keyring
    key (no duplicate keychain entries).
    """
    db_profiles = data.get("db_profiles") or {}
    if isinstance(db_profiles, dict):
        for name, mapping in db_profiles.items():
            if not isinstance(mapping, dict):
                continue
            for fld in _DB_SECRET_FIELDS:
                _externalise_secret(mapping, fld, f"db_profiles/{name}/{fld}", store)

    llm_profiles = data.get("llm_profiles") or {}
    if isinstance(llm_profiles, dict):
        for name, mapping in llm_profiles.items():
            if not isinstance(mapping, dict):
                continue
            for fld in _LLM_SECRET_FIELDS:
                _externalise_secret(mapping, fld, f"llm_profiles/{name}/{fld}", store)

    db_top = data.get("db")
    if isinstance(db_top, dict) and active_db_profile:
        for fld in _DB_SECRET_FIELDS:
            _externalise_secret(db_top, fld, f"db_profiles/{active_db_profile}/{fld}", store)

    llm_top = data.get("llm")
    if isinstance(llm_top, dict) and active_llm_profile:
        for fld in _LLM_SECRET_FIELDS:
            _externalise_secret(llm_top, fld, f"llm_profiles/{active_llm_profile}/{fld}", store)

    embedding = data.get("embedding")
    if isinstance(embedding, dict):
        for fld in _EMBEDDING_SECRET_FIELDS:
            _externalise_secret(embedding, fld, f"embedding/{fld}", store)
    return data


def _resolve_secret_field(mapping: dict[str, Any], field_name: str, store: SecretStore) -> None:
    value = mapping.get(field_name, "")
    if not is_secret_reference(value):
        return
    try:
        keyring_key = parse_reference(value)
        resolved = store.get(keyring_key)
    except Exception:
        resolved = None
    mapping[field_name] = resolved or ""


def _resolve_secrets_in_data(data: dict[str, Any], store: SecretStore) -> dict[str, Any]:
    """Replace ``keyring:...`` references in ``data`` with plaintext values from the store.

    Mutates ``data`` in place. References that cannot be resolved (keyring
    unavailable, or key removed) are replaced with empty strings.
    """
    db_profiles = data.get("db_profiles") or {}
    if isinstance(db_profiles, dict):
        for mapping in db_profiles.values():
            if isinstance(mapping, dict):
                for fld in _DB_SECRET_FIELDS:
                    _resolve_secret_field(mapping, fld, store)

    llm_profiles = data.get("llm_profiles") or {}
    if isinstance(llm_profiles, dict):
        for mapping in llm_profiles.values():
            if isinstance(mapping, dict):
                for fld in _LLM_SECRET_FIELDS:
                    _resolve_secret_field(mapping, fld, store)

    db_top = data.get("db")
    if isinstance(db_top, dict):
        for fld in _DB_SECRET_FIELDS:
            _resolve_secret_field(db_top, fld, store)

    llm_top = data.get("llm")
    if isinstance(llm_top, dict):
        for fld in _LLM_SECRET_FIELDS:
            _resolve_secret_field(llm_top, fld, store)

    embedding = data.get("embedding")
    if isinstance(embedding, dict):
        for fld in _EMBEDDING_SECRET_FIELDS:
            _resolve_secret_field(embedding, fld, store)
    return data


_OPENROUTER_MODEL_NAMESPACES = (
    "openai",
    "anthropic",
    "google",
    "deepseek",
    "qwen",
    "meta-llama",
    "mistralai",
    "x-ai",
    "moonshotai",
    "openrouter",
)


class _ObservableConfig:
    """Notify the owning AMXConfig when nested config values change."""

    def __setattr__(self, name: str, value: Any) -> None:
        object.__setattr__(self, name, value)
        if name.startswith("_"):
            return
        owner = getattr(self, "_amx_owner", None)
        if owner is not None:
            owner._autosave_nested()


def _closest_provider_namespace(value: str, choices: tuple[str, ...]) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return raw
    aliases = {
        "gpt": "openai",
        "chatgpt": "openai",
        "claude": "anthropic",
        "gemini": "google",
        "llama": "meta-llama",
        "mistral": "mistralai",
        "xai": "x-ai",
        "moonshot": "moonshotai",
    }
    if raw in aliases:
        return aliases[raw]
    if raw in choices:
        return raw
    matches = get_close_matches(raw, list(choices), n=1, cutoff=0.75)
    return matches[0] if matches else raw


def normalize_llm_model(provider: str, model: str) -> str:
    """Store provider-specific model ids in a concise, non-duplicated form."""
    raw = str(model or "").strip().strip("/")
    if not raw:
        return ""
    lower = raw.lower()
    provider_norm = (provider or "").strip().lower()
    if provider_norm == "openrouter" and "/" in raw:
        head, tail = raw.split("/", 1)
        head_norm = _closest_provider_namespace(head, _OPENROUTER_MODEL_NAMESPACES)
        if tail:
            raw = f"{head_norm}/{tail.strip('/')}"
            lower = raw.lower()
    elif "/" in raw:
        head, tail = raw.split("/", 1)
        head_norm = (
            _closest_provider_namespace(head, (provider_norm,)) if provider_norm else head.lower()
        )
        if provider_norm and head_norm == provider_norm and tail:
            raw = tail.strip("/")
            lower = raw.lower()
    if provider_norm and lower.startswith(f"{provider_norm}/"):
        raw = raw[len(provider_norm) + 1 :]
        lower = raw.lower()
    if provider_norm in {"local", "kimi", "databricks_serving"} and lower.startswith("openai/"):
        raw = raw.split("/", 1)[1]
    if provider_norm == "openrouter" and lower.startswith("openrouter/"):
        raw = raw.split("/", 1)[1]
    return raw.strip().strip("/")


# ── Prompt Detail Levels ──────────────────────────────────────────────────────


@dataclass
class PromptDetail:
    """Controls which database context fields are included in every LLM prompt.

    Reducing detail lowers input-token cost; increasing detail may improve
    inference quality for ambiguous schemas. Use ``prompt_detail_for()`` to
    get a named preset, or construct your own by overriding individual flags.
    """

    # --- Column-level fields ---
    include_samples: bool = True  # Sample values per column
    max_samples: int = 3  # How many sample values to include (when enabled)
    include_null_counts: bool = True  # null_count / row_count
    include_min_max: bool = True  # min_val / max_val
    include_cardinality: bool = False  # distinct_count + cardinality_ratio
    include_existing_col_comment: bool = True  # existing DB comment on the column

    # --- Table-level fields ---
    include_pk_fk: bool = True  # Primary key + outgoing/incoming foreign keys
    include_unique_check: bool = False  # Unique constraints + check constraints
    include_usage_stats: bool = False  # seq_scan / idx_scan / n_live_tup from pg_stat
    include_schema_db_comments: bool = False  # Schema-level and database-level comments
    include_related_comments: bool = False  # Existing comments on FK-neighbour tables
    include_query_log_analysis: bool = (
        False  # SQL/code query-usage hints (table/column usage patterns)
    )

    # --- RAG agent tuning ---
    rag_table_hits: int = 5  # Doc chunks fetched for the table-level query
    rag_col_hits: int = 1  # Doc chunks fetched per column query
    rag_max_chunks: int = 8  # Hard cap on total chunks injected into the RAG prompt


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
            include_query_log_analysis=False,
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
            include_query_log_analysis=True,
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
            include_query_log_analysis=True,
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
        include_query_log_analysis=False,
        rag_table_hits=5,
        rag_col_hits=1,
        rag_max_chunks=8,
    )


@dataclass
class DBConfig(_ObservableConfig):
    backend: str = "postgresql"

    # Common fields (PostgreSQL / generic)
    host: str = "localhost"
    port: int = 5432
    # Credential defaults are empty so a fresh DBConfig (e.g., the
    # active-mirror dataclass on first install) carries no demo
    # credentials that could leak into ~/.amx/config.yml when save()
    # runs before the user has added any real profile. Pre-0.11 these
    # were "amx" / "amx_pass", which materialised as a phantom
    # localhost connection in /db-profiles.
    user: str = ""
    password: str = ""
    # ``database`` is now optional. Empty string means "no DB pinned to this
    # profile" — the user picks a database at command time (interactive
    # picker, or `--database`). Historically the default was the demo value
    # ``"SAP"``, which surfaced as a phantom localhost connection in
    # ``/db-profiles`` for users who had not finished setup. The legacy
    # value is kept readable on load (see ``LEGACY_DATABASE_DEFAULTS`` and
    # the startup hint), but new profiles never get pre-filled with it.
    database: str = ""

    # Snowflake
    account: str = ""
    warehouse: str = ""
    role: str = ""

    # Databricks
    http_path: str = ""
    access_token: str = ""
    catalog: str = ""
    tls_no_verify: bool = False
    tls_trusted_ca_file: str = ""

    # BigQuery
    project: str = ""
    dataset: str = ""
    credentials_path: str = ""

    # Oracle — service name vs SID is exposed explicitly so users can
    # match what their DBA provided. ``database`` field is reused for SID
    # when ``service_name`` is blank.
    service_name: str = ""

    # SQL Server — ``driver`` is the ODBC driver name (e.g. "ODBC Driver
    # 18 for SQL Server"), required by pyodbc. ``encrypt`` /
    # ``trust_server_certificate`` cover the most-asked TLS knobs.
    driver: str = ""
    encrypt: bool = True
    trust_server_certificate: bool = False

    # Redshift — optional; only used by IAM-auth code paths. Standard
    # username/password auth uses the inherited ``host``/``user`` fields.
    cluster_identifier: str = ""

    # ClickHouse — HTTPS toggle. HTTP port is 8123, HTTPS is 8443.
    secure: bool = False

    # Profiling guardrails
    profiling_mode: str = "full"  # full | sampled | metadata
    profiling_max_rows: int = 1_000_000  # skip full column scans above this row estimate (0=off)
    profiling_sample_size: int = 5
    # How many columns to compute null/distinct/min/max for in a single
    # bulk query. The connector chunks wide tables into batches of this
    # size — fewer queries on warehouse-billed backends, less memory
    # pressure on engines that build a separate hash per
    # ``COUNT(DISTINCT)``. 50 is the safe default; raise on
    # Databricks/Snowflake (Bloom filters), drop on MSSQL/MySQL with
    # very wide tables.
    profiling_stats_batch_size: int = 50

    def _effective_port(self, default: int) -> int:
        """Resolve the port for a non-PostgreSQL backend.

        ``port`` defaults to 5432 at the dataclass level for PostgreSQL
        back-compat. For every other backend, treat that legacy value
        as "no port set" and substitute the canonical default (3306 for
        MySQL, 1521 for Oracle, 1433 for SQL Server, …). Users who
        actively picked a port via the wizard get their value through
        unchanged.
        """
        if self.port and self.port != 5432:
            return self.port
        return default

    @property
    def url(self) -> str:
        if self.backend == "snowflake":
            # Snowflake's SQLAlchemy URL accepts no database — connect to the
            # account, let the user pick at query time. Keep ``/<database>``
            # only when pinned so the engine starts in that DB.
            url = f"snowflake://{quote_plus(self.user)}:{quote_plus(self.password)}@{self.account}"
            if self.database:
                url += f"/{quote_plus(self.database)}"
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
            # Defensive normalization: users routinely paste the
            # full workspace URL ("https://dbc-xxx.cloud.databricks.com/")
            # — without stripping the scheme and trailing slash, the
            # resulting "databricks://token:xxx@https://host/:443" URL
            # makes SQLAlchemy try int("") for the port and crash with
            # "invalid literal for int() with base 10: ''" the moment
            # you list schemas.
            host = _normalize_db_host(self.host)
            url = f"databricks://token:{quote_plus(token)}@{host}:443"
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

        if self.backend == "mysql":
            # PyMySQL dialect. ``database`` is optional — same "leave
            # blank to pick at command time" pattern as PG.
            port = self._effective_port(3306)
            url = (
                f"mysql+pymysql://{quote_plus(self.user)}:{quote_plus(self.password)}"
                f"@{self.host}:{port}"
            )
            if self.database:
                url += f"/{quote_plus(self.database)}"
            return url

        if self.backend == "oracle":
            # python-oracledb (thin mode by default). Service name is
            # preferred over SID for modern Oracle Cloud / RAC setups; we
            # fall back to ``database`` (treated as SID) when blank.
            port = self._effective_port(1521)
            base = (
                f"oracle+oracledb://{quote_plus(self.user)}:{quote_plus(self.password)}"
                f"@{self.host}:{port}"
            )
            if self.service_name:
                return f"{base}/?service_name={quote_plus(self.service_name)}"
            if self.database:
                return f"{base}/{quote_plus(self.database)}"
            return base

        if self.backend == "mssql":
            # pyodbc dialect. The ODBC driver name is mandatory at the
            # SQLAlchemy URL level — fall back to a sensible default
            # so a fresh profile can connect without the user having to
            # know the exact driver string up front.
            driver = self.driver or "ODBC Driver 18 for SQL Server"
            port = self._effective_port(1433)
            params: list[str] = [f"driver={quote_plus(driver)}"]
            if self.encrypt:
                params.append("Encrypt=yes")
            else:
                params.append("Encrypt=no")
            if self.trust_server_certificate:
                params.append("TrustServerCertificate=yes")
            url = (
                f"mssql+pyodbc://{quote_plus(self.user)}:{quote_plus(self.password)}"
                f"@{self.host}:{port}"
            )
            if self.database:
                url += f"/{quote_plus(self.database)}"
            return f"{url}?{'&'.join(params)}"

        if self.backend == "redshift":
            # Uses the redshift_connector SQLAlchemy dialect. URL shape
            # mirrors PG; cluster_identifier is appended when set so IAM
            # auth code paths can pick it up.
            port = self._effective_port(5439)
            url = (
                f"redshift+redshift_connector://{quote_plus(self.user)}:"
                f"{quote_plus(self.password)}@{self.host}:{port}"
            )
            if self.database:
                url += f"/{quote_plus(self.database)}"
            if self.cluster_identifier:
                url += f"?cluster_identifier={quote_plus(self.cluster_identifier)}"
            return url

        if self.backend == "clickhouse":
            # clickhouse-sqlalchemy with the HTTP transport — works
            # against ClickHouse Cloud and on-prem deployments. Port
            # defaults differ for HTTPS vs HTTP.
            scheme = "clickhouse+https" if self.secure else "clickhouse+http"
            port = self._effective_port(8443 if self.secure else 8123)
            url = (
                f"{scheme}://{quote_plus(self.user or 'default')}:"
                f"{quote_plus(self.password)}@{self.host}:{port}"
            )
            if self.database:
                url += f"/{quote_plus(self.database)}"
            return url

        if self.backend == "duckdb":
            # File-based or in-memory. ``database`` carries the path
            # (or ``:memory:``). No host/port/user/password.
            target = self.database or ":memory:"
            return f"duckdb:///{target}"

        # Default: PostgreSQL. When the user leaves ``database`` blank
        # (the ``/add-db-profile`` wizard advertises it as optional —
        # "leave blank to pick at command time"), fall back to the
        # ``postgres`` system database that every PostgreSQL install
        # ships with and grants CONNECT to PUBLIC by default. Without
        # this fallback, libpq silently substitutes the username as the
        # database name, which almost never exists and produces
        # ``FATAL: database "<user>" does not exist``. The user then
        # blames AMX for an "optional" promise the URL builder never
        # actually honoured.
        #
        # The user can switch databases at command time with
        # ``/database <name>`` (or by running ``/edit`` and pinning one
        # explicitly into the profile). The fallback only affects the
        # initial connection — listings, profiling, and write-back all
        # respect whatever database is currently in scope.
        url = (
            f"postgresql://{quote_plus(self.user)}:{quote_plus(self.password)}"
            f"@{self.host}:{self.port}"
        )
        url += f"/{quote_plus(self.database) if self.database else 'postgres'}"
        return url

    @property
    def display_summary(self) -> str:
        """Short human-readable connection summary for the UI."""
        unpinned_label = "(no DB pinned)"
        if self.backend == "snowflake":
            db = self.database or unpinned_label
            return f"{db}@{self.account} (user {self.user})"
        if self.backend == "databricks":
            cat = f" catalog={self.catalog}" if self.catalog else f" {unpinned_label}"
            return f"{self.host}{cat}"
        if self.backend == "bigquery":
            ds = f".{self.dataset}" if self.dataset else f" {unpinned_label}"
            return f"{self.project}{ds}"
        if self.backend == "oracle":
            target = self.service_name or self.database or unpinned_label
            port = self._effective_port(1521)
            return f"{target} @ {self.host}:{port} (user {self.user})"
        if self.backend == "mssql":
            db = self.database or unpinned_label
            port = self._effective_port(1433)
            return f"{db} @ {self.host}:{port} (user {self.user})"
        if self.backend == "mysql":
            db = self.database or unpinned_label
            port = self._effective_port(3306)
            return f"{db} @ {self.host}:{port} (user {self.user})"
        if self.backend == "redshift":
            db = self.database or unpinned_label
            port = self._effective_port(5439)
            return f"{db} @ {self.host}:{port} (user {self.user})"
        if self.backend == "clickhouse":
            db = self.database or unpinned_label
            scheme = "https" if self.secure else "http"
            port = self._effective_port(8443 if self.secure else 8123)
            return f"{db} @ {self.host}:{port} ({scheme}, user {self.user or 'default'})"
        if self.backend == "duckdb":
            return f"DuckDB file: {self.database or ':memory:'}"
        db = self.database or unpinned_label
        return f"{db} @ {self.host}:{self.port} (user {self.user})"

    def is_connection_configured(self) -> bool:
        """True when the profile has the minimum *connection* fields.

        This is the new (0.11.0) "can we even open a connection" predicate —
        a database / catalog / dataset is **not** required. PG/SF/DB will
        connect to the engine and let the user pick a database at run time;
        BigQuery requires a project (no project = no connection).
        """
        if self.backend == "postgresql":
            return bool(self.host and self.user)
        if self.backend == "snowflake":
            return bool(self.account and self.user)
        if self.backend == "databricks":
            return bool(self.host and (self.access_token or self.password))
        if self.backend == "bigquery":
            return bool(self.project)
        if self.backend == "mysql":
            return bool(self.host and self.user)
        if self.backend == "oracle":
            return bool(self.host and self.user and (self.service_name or self.database))
        if self.backend == "mssql":
            return bool(self.host and self.user)
        if self.backend == "redshift":
            return bool(self.host and self.user)
        if self.backend == "clickhouse":
            # ClickHouse defaults the user to ``default`` if blank, so a
            # bare host is enough to attempt a connection.
            return bool(self.host)
        # DuckDB: file path or ``:memory:`` is enough; an empty ``database``
        # field is interpreted as in-memory by ``DBConfig.url``. Every other
        # backend has been handled above, so a non-duckdb fallthrough means
        # "not configured".
        return self.backend == "duckdb"

    def is_database_pinned(self) -> bool:
        """True when the profile pins a specific database / catalog / dataset.

        When False, the user is expected to pick the database at command
        time (catalog picker, `--database` flag, etc.). 3-level backends
        (Databricks Unity Catalog, BigQuery datasets) treat catalog /
        dataset as the database-equivalent.
        """
        if self.backend == "postgresql":
            return bool(self.database)
        if self.backend == "snowflake":
            return bool(self.database)
        if self.backend == "databricks":
            return bool(self.catalog)
        if self.backend == "bigquery":
            return bool(self.dataset)
        if self.backend == "mysql":
            return bool(self.database)
        if self.backend == "oracle":
            return bool(self.service_name or self.database)
        if self.backend == "mssql":
            return bool(self.database)
        if self.backend == "redshift":
            return bool(self.database)
        if self.backend == "clickhouse":
            return bool(self.database)
        # DuckDB: the ``database`` field IS the file path; ``:memory:`` and
        # any explicit path both count as "pinned" because the user made an
        # active choice (vs. PG where blank means "pick later"). Every other
        # backend has been handled above, so non-duckdb fallthrough means
        # "not pinned".
        return self.backend == "duckdb"

    def is_configured(self) -> bool:
        """Back-compat: True when the profile is connection-ready.

        Pre-0.11 callers used this to gate "show profile in UI" / "drive
        ``DatabaseConnector(cfg.db)``" decisions. We deliberately drop the
        ``database``-required clauses here so an unpinned profile still
        counts as "configured" — the missing-database case is now surfaced
        separately via :meth:`is_database_pinned` and a startup hint.

        Use :meth:`is_connection_configured` in new code; this alias stays
        for the 99 existing call sites.
        """
        return self.is_connection_configured()


# ── Serialization helpers ─────────────────────────────────────────────────


def _db_from_mapping(m: dict[str, Any]) -> DBConfig:
    backend = str(m.get("backend", "postgresql"))
    return DBConfig(
        backend=backend,
        host=str(m.get("host", "localhost")),
        port=int(m.get("port", 5432)),
        user=str(m.get("user", "amx")),
        password=str(m.get("password", "")),
        # Fallback to ``""`` rather than the legacy ``"SAP"`` demo value.
        # Old YAML that already has ``database: SAP`` will still load with
        # that string; this only changes what happens when the key is
        # absent (e.g. partial profiles or older serialisations).
        database=str(m.get("database", "")),
        account=str(m.get("account", "")),
        warehouse=str(m.get("warehouse", "")),
        role=str(m.get("role", "")),
        http_path=str(m.get("http_path", "")),
        access_token=str(m.get("access_token", "")),
        catalog=str(m.get("catalog", "")),
        tls_no_verify=bool(m.get("tls_no_verify", False)),
        tls_trusted_ca_file=str(m.get("tls_trusted_ca_file", "")),
        project=str(m.get("project", "")),
        dataset=str(m.get("dataset", "")),
        credentials_path=str(m.get("credentials_path", "")),
        service_name=str(m.get("service_name", "")),
        driver=str(m.get("driver", "")),
        encrypt=bool(m.get("encrypt", True)),
        trust_server_certificate=bool(m.get("trust_server_certificate", False)),
        cluster_identifier=str(m.get("cluster_identifier", "")),
        secure=bool(m.get("secure", False)),
        profiling_mode=str(m.get("profiling_mode", "full")),
        profiling_max_rows=int(m.get("profiling_max_rows", 1_000_000)),
        profiling_sample_size=int(m.get("profiling_sample_size", 5)),
        profiling_stats_batch_size=int(m.get("profiling_stats_batch_size", 50)),
    )


def _db_to_mapping(db: DBConfig) -> dict[str, Any]:
    base: dict[str, Any] = {"backend": db.backend}

    if db.backend == "postgresql":
        base.update(
            {
                "host": db.host,
                "port": db.port,
                "user": db.user,
                "password": db.password,
                "database": db.database,
            }
        )
    elif db.backend == "snowflake":
        base.update(
            {
                "account": db.account,
                "user": db.user,
                "password": db.password,
                "database": db.database,
                "warehouse": db.warehouse,
                "role": db.role,
            }
        )
    elif db.backend == "databricks":
        base.update(
            {
                "host": db.host,
                "http_path": db.http_path,
                "access_token": db.access_token,
                "catalog": db.catalog,
                "database": db.database,
                "tls_no_verify": db.tls_no_verify,
                "tls_trusted_ca_file": db.tls_trusted_ca_file,
            }
        )
    elif db.backend == "bigquery":
        base.update(
            {
                "project": db.project,
                "dataset": db.dataset,
                "credentials_path": db.credentials_path,
            }
        )
    elif db.backend == "mysql":
        base.update(
            {
                "host": db.host,
                "port": db.port or 3306,
                "user": db.user,
                "password": db.password,
                "database": db.database,
            }
        )
    elif db.backend == "oracle":
        base.update(
            {
                "host": db.host,
                "port": db.port or 1521,
                "user": db.user,
                "password": db.password,
                "service_name": db.service_name,
                "database": db.database,
            }
        )
    elif db.backend == "mssql":
        base.update(
            {
                "host": db.host,
                "port": db.port or 1433,
                "user": db.user,
                "password": db.password,
                "database": db.database,
                "driver": db.driver,
                "encrypt": db.encrypt,
                "trust_server_certificate": db.trust_server_certificate,
            }
        )
    elif db.backend == "redshift":
        base.update(
            {
                "host": db.host,
                "port": db.port or 5439,
                "user": db.user,
                "password": db.password,
                "database": db.database,
                "cluster_identifier": db.cluster_identifier,
            }
        )
    elif db.backend == "clickhouse":
        base.update(
            {
                "host": db.host,
                "port": db.port or (8443 if db.secure else 8123),
                "user": db.user,
                "password": db.password,
                "database": db.database,
                "secure": db.secure,
            }
        )
    elif db.backend == "duckdb":
        base.update(
            {
                "database": db.database,
            }
        )
    else:
        base.update(
            {
                "host": db.host,
                "port": db.port,
                "user": db.user,
                "password": db.password,
                "database": db.database,
            }
        )
    base.update(
        {
            "profiling_mode": db.profiling_mode,
            "profiling_max_rows": int(db.profiling_max_rows),
            "profiling_sample_size": int(db.profiling_sample_size),
            "profiling_stats_batch_size": int(db.profiling_stats_batch_size),
        }
    )
    return base


@dataclass
class LLMConfig(_ObservableConfig):
    provider: str = ""  # openai | openrouter | anthropic | gemini | local | deepseek | …
    model: str = ""
    # Deprecated since PR #61: AMX is English-only. The field stays so that
    # call sites reading ``cfg.llm.language`` (legacy display strings, the
    # search-agent system prompt's "metadata language" line, etc.) keep
    # working without per-line null guards. New code must NOT branch on it
    # — all prompts and outputs are English. The /language slash command,
    # wizard prompt, and the routing entry are removed.
    language: str = "english"
    api_key: str = ""
    api_base: str | None = None
    temperature: float = 0.2
    max_tokens: int = 4096  # reduced from 16384; reasoning models raise this automatically
    completion_mode: str = "chat_completions"  # "chat_completions" | "batch"
    n_alternatives: int = 3  # how many description alternatives per column (1–5)
    column_batch_size: int = 10  # how many columns to process in one LLM call
    batch_context_column_names: int = (
        0  # how many non-batch column names to include as context (0=off, -1=all)
    )
    prompt_detail: str = "standard"  # minimal | standard | detailed | full
    # Description verbosity controls the LENGTH/DEPTH of generated
    # descriptions, separate from ``prompt_detail`` (which controls how
    # much context AMX feeds the LLM). ``brief`` = 1 sentence (current
    # behavior); ``detailed`` = 2-4 sentences with purpose, typical
    # values, and relationships when supported by evidence.
    description_verbosity: str = "brief"  # brief | detailed
    logprob_high: float = 0.85
    logprob_medium: float = 0.50
    force_logprobs: bool = True
    # Token budget for the model's internal reasoning (Anthropic extended
    # thinking). Only consumed when the model supports reasoning AND a caller
    # passes ``on_thinking`` to ``LLMProvider.chat``; otherwise ignored.
    thinking_budget: int = 1024

    @property
    def prompt_detail_cfg(self) -> PromptDetail:
        """Return the resolved PromptDetail dataclass for this config's level."""
        return prompt_detail_for(self.prompt_detail)

    def is_configured(self) -> bool:
        """True when the LLM profile has the minimum fields to dispatch a call."""
        return bool(self.provider and self.model)


def _llm_from_mapping(m: dict[str, Any]) -> LLMConfig:
    n_alt = int(m.get("n_alternatives", 3))
    provider = str(m.get("provider", ""))
    model = normalize_llm_model(provider, str(m.get("model", "")))
    # ``language`` is deprecated since PR #61 (AMX is English-only)
    # but the field stays on LLMConfig for back-compat. Existing
    # configs with ``language: turkish`` round-trip without crashing
    # — the value is read in but never used to branch any prompt.
    return LLMConfig(
        provider=provider,
        model=model,
        language=str(m.get("language", "english") or "english"),
        api_key=str(m.get("api_key", "")),
        api_base=m.get("api_base"),
        temperature=float(m.get("temperature", 0.2)),
        max_tokens=int(m.get("max_tokens", 4096)),
        completion_mode=str(m.get("completion_mode", "chat_completions")),
        n_alternatives=max(1, min(5, n_alt)),
        column_batch_size=int(m.get("column_batch_size", 10)),
        batch_context_column_names=int(m.get("batch_context_column_names", 0)),
        prompt_detail=str(m.get("prompt_detail", "standard")),
        description_verbosity=str(m.get("description_verbosity", "brief")),
        logprob_high=float(m.get("logprob_high", 0.85)),
        logprob_medium=float(m.get("logprob_medium", 0.50)),
        force_logprobs=bool(m.get("force_logprobs", True)),
        thinking_budget=int(m.get("thinking_budget", 1024)),
    )


def _llm_to_mapping(llm: LLMConfig) -> dict[str, Any]:
    return {
        "provider": llm.provider,
        "model": normalize_llm_model(llm.provider, llm.model),
        # Deprecated (AMX is English-only since PR #61) — kept in the
        # YAML for back-compat and to round-trip cleanly.
        "language": llm.language,
        "api_key": llm.api_key,
        "api_base": llm.api_base,
        "temperature": llm.temperature,
        "max_tokens": llm.max_tokens,
        "completion_mode": llm.completion_mode,
        "n_alternatives": llm.n_alternatives,
        "column_batch_size": llm.column_batch_size,
        "batch_context_column_names": llm.batch_context_column_names,
        "prompt_detail": llm.prompt_detail,
        "description_verbosity": llm.description_verbosity,
        "logprob_high": llm.logprob_high,
        "logprob_medium": llm.logprob_medium,
        "force_logprobs": llm.force_logprobs,
        "thinking_budget": llm.thinking_budget,
    }


@dataclass
class EmbeddingConfig(_ObservableConfig):
    """Search-index embedding provider settings.

    Maps onto :mod:`amx.search.embeddings` providers; ``kind="minilm"``
    keeps the historical Chroma default. ``api_key`` is treated as a
    secret and stored in the OS keyring just like DB passwords.
    """

    kind: str = DEFAULT_EMBEDDING_KIND  # minilm | openai_compatible | sentence_transformers
    model: str = ""  # provider-specific (e.g. text-embedding-3-small, BAAI/bge-large-en-v1.5)
    api_key: str = ""  # only used by openai_compatible; secret-managed
    base_url: str = ""  # only used by openai_compatible; defaults to OpenAI proper

    def is_configured(self) -> bool:
        """True when the configured provider has the minimum fields to operate.

        MiniLM needs no setup; the other two need at least a model id."""
        normalised = (self.kind or "").lower().strip()
        if normalised in {"", "minilm", "default"}:
            return True
        return bool(self.model)


def _embedding_from_mapping(m: dict[str, Any]) -> EmbeddingConfig:
    return EmbeddingConfig(
        kind=str(m.get("kind", DEFAULT_EMBEDDING_KIND) or DEFAULT_EMBEDDING_KIND),
        model=str(m.get("model", "") or ""),
        api_key=str(m.get("api_key", "") or ""),
        base_url=str(m.get("base_url", "") or ""),
    )


def _embedding_to_mapping(emb: EmbeddingConfig) -> dict[str, Any]:
    return {
        "kind": emb.kind or DEFAULT_EMBEDDING_KIND,
        "model": emb.model,
        "api_key": emb.api_key,
        "base_url": emb.base_url,
    }


# Schema version stamped into every saved config.yml. Bump it whenever
# the on-disk shape changes in a way an OLDER AMX cannot understand
# (renamed key, removed key, semantic change). Additive changes (new
# optional key, new field with safe default) do NOT need a bump — old
# AMX silently ignores them via the dict.get(...) pattern.
#
# When ``load()`` finds ``schema_version`` higher than this constant it
# refuses with ``ConfigSchemaTooNewError`` so the user gets a clear
# upgrade message instead of having profiles silently mangled (the
# exact bug class that hit the 0.3.1 / 0.11.0 PATH skew on 2026-05-01).
CONFIG_SCHEMA_VERSION: int = 2


class ConfigSchemaTooNewError(RuntimeError):
    """Raised when ``config.yml`` was written by a newer AMX than the running one.

    The CLI top-level catches this and renders an actionable message
    (upgrade AMX or downgrade your config) instead of letting the user
    see a stack trace or — worse — a silent-overwrite-and-lose-profiles.
    """

    def __init__(self, *, file_version: int, supported_version: int, path: str) -> None:
        self.file_version = file_version
        self.supported_version = supported_version
        self.path = path
        super().__init__(
            f"Config at {path} was written by a newer AMX "
            f"(schema_version={file_version}). This AMX understands up to "
            f"schema_version={supported_version}. Upgrade AMX, or pin an older "
            f"AMX and re-run."
        )


@dataclass
class AMXConfig:
    db: DBConfig = field(default_factory=DBConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    embedding: EmbeddingConfig = field(default_factory=EmbeddingConfig)
    doc_paths: list[str] = field(default_factory=list)
    code_paths: list[str] = field(default_factory=list)
    selected_schemas: list[str] = field(default_factory=list)
    selected_tables: list[str] = field(default_factory=list)
    db_profiles: dict[str, DBConfig] = field(default_factory=dict)
    # Single-active mirror — kept for back-compat with the 99 call sites
    # that read ``cfg.active_db_profile`` directly. In 0.11.0 the source
    # of truth becomes ``active_db_profiles`` (the multi-pick scope set
    # by ``/use-db prod_pg analytics_bq``); this scalar mirrors the
    # **first** entry of that list. ``set_active_db_profile`` updates
    # both, ``set_active_db_profiles`` collapses the list to a single
    # entry when called with one name. Reads that need the full scope
    # use ``active_db_profiles`` (or ``ProfileScope.from_config(cfg)``).
    active_db_profile: str = "default"
    # 0.11.0 multi-DB execution scope. When non-empty this is the set of
    # profiles that ``/ask``, ``/run`` and ``/sync`` operate on by default.
    # Loaded with one-element fallback from the legacy ``active_db_profile``
    # when the YAML pre-dates this release. Saved on every write alongside
    # the legacy scalar so a 0.10.x reader can still round-trip without
    # losing the user's active profile.
    active_db_profiles: list[str] = field(default_factory=list)
    current_schema: str = ""
    current_table: str = ""
    llm_profiles: dict[str, LLMConfig] = field(default_factory=dict)
    active_llm_profile: str = "default"
    # Per-agent override: when non-empty AND present in ``llm_profiles``,
    # the RAG agent uses this profile instead of ``active_llm_profile``.
    # Lets the user pair, e.g., a small-and-cheap retrieval model with a
    # bigger reasoning model on the global profile (or vice versa).
    # Empty string ("") = no override, RAG uses the active profile.
    rag_llm_profile: str = ""
    doc_profiles: dict[str, list[str]] = field(default_factory=dict)
    active_doc_profile: str = ""
    code_profiles: dict[str, str] = field(default_factory=dict)
    active_code_profile: str = ""
    write_through_config: bool = True

    # ── Shared history store (v0.12.0) ───────────────────────────────────
    # When ``history_store_enabled`` is True, every run/result/event
    # write is dual-written to a team backend (named DBConfig profile)
    # under a dedicated schema (``history_store_schema``). Local SQLite
    # remains the read source for ``/history list`` so single-user
    # workflows keep their fast path. Configure via ``/history-store``.
    history_store_enabled: bool = False
    history_store_profile: str = ""
    history_store_schema: str = "AMX"
    # Overrides the profile's pinned database/catalog when building the
    # shared-history engine. A single DB profile (e.g. ``prod_pg``)
    # often points at multiple databases; the user picks where the AMX
    # schema lives at /history-store enable time. Interpreted per-backend
    # by ``apply_history_db_override`` — database for PG/MySQL/MSSQL/
    # Oracle/Redshift/Snowflake, catalog for Databricks, project for
    # BigQuery. Empty string means "use whatever the profile already
    # has pinned".
    history_store_database: str = ""

    # Ephemeral, never persisted to YAML. Tracks which chat session the
    # current REPL is appending to. Reset to None on every load.
    active_chat_session_id: int | None = field(default=None)

    CONFIG_DIR: str = field(default_factory=lambda: str(Path.home() / ".amx"), init=False)
    _config_path: str = field(default="", init=False, repr=False)
    _autosave_ready: bool = field(default=False, init=False, repr=False)
    _autosave_suspended: int = field(default=0, init=False, repr=False)
    _fresh_install: bool = field(default=False, init=False, repr=False)

    _PERSISTED_FIELDS: ClassVar[frozenset[str]] = frozenset(
        {
            "db",
            "llm",
            "embedding",
            "doc_paths",
            "code_paths",
            "selected_schemas",
            "selected_tables",
            "db_profiles",
            "active_db_profile",
            "active_db_profiles",
            "current_schema",
            "current_table",
            "llm_profiles",
            "active_llm_profile",
            "rag_llm_profile",
            "doc_profiles",
            "active_doc_profile",
            "code_profiles",
            "active_code_profile",
            "write_through_config",
            "history_store_enabled",
            "history_store_profile",
            "history_store_schema",
            "history_store_database",
        }
    )

    def __setattr__(self, name: str, value: Any) -> None:
        object.__setattr__(self, name, value)
        if name.startswith("_") or name == "CONFIG_DIR":
            return
        if name in {"db", "llm", "embedding", "db_profiles", "llm_profiles"}:
            self._attach_children()
        if name in self._PERSISTED_FIELDS:
            if name == "write_through_config" and getattr(self, "_autosave_ready", False):
                with suppress(Exception):
                    self.save()
                return
            self._autosave_nested()

    @classmethod
    def load(cls, path: str | None = None) -> AMXConfig:
        cfg = cls()
        p = Path(path) if path else Path(cfg.CONFIG_DIR) / "config.yml"
        object.__setattr__(cfg, "_config_path", str(p))
        object.__setattr__(cfg, "_autosave_suspended", 1)
        fresh_install = not p.exists()
        object.__setattr__(cfg, "_fresh_install", fresh_install)
        if p.exists():
            data: dict[str, Any] = yaml.safe_load(p.read_text()) or {}
            # Refuse forward — a config written by a newer AMX may have
            # keys/semantics this binary cannot interpret. Silent reads
            # would strip those keys on the next save() and lose the
            # user's data (the 0.3.1 vs 0.11.0 ghost-profile incident).
            file_version = int(data.get("schema_version") or 0)
            if file_version > CONFIG_SCHEMA_VERSION:
                raise ConfigSchemaTooNewError(
                    file_version=file_version,
                    supported_version=CONFIG_SCHEMA_VERSION,
                    path=str(p),
                )
            # Resolve any keyring references back to plaintext before populating
            # the in-memory dataclasses. Backwards-compatible: plaintext values
            # left untouched so legacy configs keep working.
            _resolve_secrets_in_data(data, get_default_store())
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
            # 0.11.0: multi-pick scope. When the YAML predates this
            # release we synthesise a one-element list from the legacy
            # scalar so the rest of the code can treat it uniformly.
            raw_scope = data.get("active_db_profiles")
            if isinstance(raw_scope, list) and raw_scope:
                # Dedupe while preserving user-specified order, drop empties.
                seen: set[str] = set()
                ordered: list[str] = []
                for item in raw_scope:
                    name = str(item or "").strip()
                    if not name or name in seen:
                        continue
                    seen.add(name)
                    ordered.append(name)
                cfg.active_db_profiles = ordered
                # Mirror first entry into the legacy scalar so old readers
                # still see a valid active profile.
                if ordered:
                    cfg.active_db_profile = ordered[0]
            else:
                cfg.active_db_profiles = [cfg.active_db_profile] if cfg.active_db_profile else []
            cfg.current_schema = str(data.get("current_schema") or "")
            cfg.current_table = str(data.get("current_table") or "")

            llm_prof_raw = data.get("llm_profiles") or {}
            if isinstance(llm_prof_raw, dict):
                for name, m in llm_prof_raw.items():
                    if isinstance(m, dict):
                        cfg.llm_profiles[str(name)] = _llm_from_mapping(m)

            cfg.active_llm_profile = str(data.get("active_llm_profile") or "default")
            cfg.rag_llm_profile = str(data.get("rag_llm_profile") or "")

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
            cfg.write_through_config = bool(data.get("write_through_config", True))

            # 0.12.0 — shared run-history store. Defaults preserve
            # local-only behaviour for users upgrading from 0.11.x.
            cfg.history_store_enabled = bool(data.get("history_store_enabled", False))
            cfg.history_store_profile = str(data.get("history_store_profile") or "")
            cfg.history_store_schema = str(data.get("history_store_schema") or "AMX")
            cfg.history_store_database = str(data.get("history_store_database") or "")

            embedding_raw = data.get("embedding")
            if isinstance(embedding_raw, dict):
                cfg.embedding = _embedding_from_mapping(embedding_raw)

        cfg.llm.api_key = cfg.llm.api_key or os.getenv("AMX_LLM_API_KEY", "")

        if not cfg.db_profiles:
            # No saved DB profiles — leave empty and clear the active pointer
            # so the CLI prompts setup instead of showing a phantom row built
            # from hardcoded defaults or the active mirror.
            cfg.active_db_profile = ""
            cfg.active_db_profiles = []
        else:
            # Drop scope entries that point at deleted profiles. Without this,
            # an outdated YAML carrying ``active_db_profiles: [foo]`` after
            # ``foo`` was removed would resurface as a ghost selection.
            cfg.active_db_profiles = [
                name for name in cfg.active_db_profiles if name in cfg.db_profiles
            ]
            try:
                cfg.apply_active_db_profile()
            except Exception:
                cfg.active_db_profile = next(iter(cfg.db_profiles.keys()))
                cfg.db = cfg.db_profiles[cfg.active_db_profile]
            # Re-anchor the multi-pick scope. apply_active_db_profile may
            # have changed active_db_profile (or kept it the same); keep
            # the list in sync as the canonical scope.
            if not cfg.active_db_profiles and cfg.active_db_profile:
                cfg.active_db_profiles = [cfg.active_db_profile]

        if not cfg.llm_profiles:
            cfg.active_llm_profile = ""
            cfg.rag_llm_profile = ""
        else:
            try:
                cfg.apply_active_llm_profile()
            except Exception:
                cfg.active_llm_profile = next(iter(cfg.llm_profiles.keys()))
                cfg.llm = replace(cfg.llm_profiles[cfg.active_llm_profile])
            # Drop the RAG override when it points at a deleted profile so
            # the orchestrator silently falls back to the active profile
            # instead of failing later with KeyError on rag_llm_profile.
            if cfg.rag_llm_profile and cfg.rag_llm_profile not in cfg.llm_profiles:
                cfg.rag_llm_profile = ""

        if not cfg.doc_profiles and cfg.doc_paths:
            cfg.doc_profiles["default"] = list(cfg.doc_paths)
            if not cfg.active_doc_profile:
                cfg.active_doc_profile = "default"

        if not cfg.code_profiles and cfg.code_paths:
            for idx, p in enumerate(cfg.code_paths):
                key = "default" if idx == 0 else f"repo{idx}"
                cfg.code_profiles[key] = p
            if not cfg.active_code_profile and cfg.code_profiles:
                cfg.active_code_profile = (
                    "default"
                    if "default" in cfg.code_profiles
                    else next(iter(cfg.code_profiles.keys()))
                )

        cfg.llm.api_key = cfg.llm.api_key or os.getenv("AMX_LLM_API_KEY", "")

        # Bridge the active chat session id across Click sub-invocations.
        # The interactive REPL dispatches each ``/ask <q>`` line via
        # ``main_command.main(args=...)`` which calls ``AMXConfig.load`` again
        # for every question. ``active_chat_session_id`` is intentionally
        # ephemeral (not in ``_PERSISTED_FIELDS``), so without this bridge
        # every question opens a fresh session and follow-up memory is lost.
        # We pick up the id from the environment variable that
        # ``cli_support.session._run_ask_repl`` sets on entry / updates after
        # each turn so the next ``main()`` call sees the same session.
        bridge_sid = os.getenv("AMX_CHAT_SESSION_ID", "").strip()
        if bridge_sid:
            with suppress(ValueError):
                cfg.active_chat_session_id = int(bridge_sid)

        object.__setattr__(cfg, "_autosave_suspended", 0)
        cfg._attach_children()
        object.__setattr__(cfg, "_autosave_ready", True)
        return cfg

    def save(self, path: str | None = None) -> Path:
        p = Path(path) if path else Path(self._config_path or Path(self.CONFIG_DIR) / "config.yml")
        p.parent.mkdir(parents=True, exist_ok=True)
        object.__setattr__(self, "_autosave_suspended", self._autosave_suspended + 1)
        try:
            if self.active_db_profile:
                self.db_profiles[self.active_db_profile] = self.db
            if self.active_llm_profile:
                self.llm_profiles[self.active_llm_profile] = replace(self.llm)

            doc_paths_yaml = self._doc_paths_for_yaml()
            code_paths_yaml = self._code_paths_for_yaml()

            # Always write both ``active_db_profile`` (legacy scalar) and
            # ``active_db_profiles`` (0.11.0 multi-pick). Round-trip
            # compatibility: a 0.10.x reader keeps working from the
            # scalar; 0.11+ readers prefer the list. The scalar is
            # always the first list entry so the two views never diverge.
            scope_list = (
                list(self.active_db_profiles)
                if self.active_db_profiles
                else ([self.active_db_profile] if self.active_db_profile else [])
            )
            # The top-level ``db:`` / ``llm:`` blocks mirror the active
            # profile's contents for backwards compatibility (pre-profile
            # configs only had these scalars). When no profiles exist
            # we skip writing them — otherwise a first-run save would
            # leak hardcoded dataclass defaults (host=localhost,
            # port=5432, etc.) into config.yml as if the user had
            # configured something. Older readers that DO have a real
            # active profile still get the mirror; brand-new installs
            # see a clean YAML with empty profile dicts.
            data: dict[str, Any] = {
                "schema_version": CONFIG_SCHEMA_VERSION,
            }
            if self.db_profiles:
                data["db"] = _db_to_mapping(self.db)
            data["db_profiles"] = {k: _db_to_mapping(v) for k, v in self.db_profiles.items()}
            data["active_db_profile"] = self.active_db_profile
            data["active_db_profiles"] = scope_list
            data["current_schema"] = self.current_schema
            data["current_table"] = self.current_table
            if self.llm_profiles:
                data["llm"] = _llm_to_mapping(self.llm)
            data["llm_profiles"] = {k: _llm_to_mapping(v) for k, v in self.llm_profiles.items()}
            data["active_llm_profile"] = self.active_llm_profile
            data["rag_llm_profile"] = str(self.rag_llm_profile or "")
            data["doc_paths"] = doc_paths_yaml
            data["doc_profiles"] = {k: list(v) for k, v in self.doc_profiles.items()}
            data["active_doc_profile"] = self.active_doc_profile
            data["code_paths"] = code_paths_yaml
            data["code_profiles"] = dict(self.code_profiles)
            data["active_code_profile"] = self.active_code_profile
            data["selected_schemas"] = self.selected_schemas
            data["selected_tables"] = self.selected_tables
            data["write_through_config"] = self.write_through_config
            data["history_store_enabled"] = bool(self.history_store_enabled)
            data["history_store_profile"] = str(self.history_store_profile or "")
            data["history_store_schema"] = str(self.history_store_schema or "AMX")
            data["history_store_database"] = str(self.history_store_database or "")
            data["embedding"] = _embedding_to_mapping(self.embedding)
            # Move plaintext secrets to the OS keyring; the YAML now stores only
            # opaque "keyring:..." references. No-op when keyring is unavailable.
            _externalise_secrets_in_data(
                data,
                active_db_profile=self.active_db_profile,
                active_llm_profile=self.active_llm_profile,
                store=get_default_store(),
            )
            payload = yaml.dump(data, default_flow_style=False, sort_keys=False)
            # Atomic write to reduce config corruption/state loss on interruptions.
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=str(p.parent),
                delete=False,
                prefix=f".{p.name}.",
                suffix=".tmp",
            ) as tmp:
                tmp.write(payload)
                tmp_path = Path(tmp.name)
            os.replace(tmp_path, p)
            # Restrict the config file to the current user — passwords and API
            # keys live here. Best-effort: chmod is a no-op on Windows.
            with suppress(OSError):
                os.chmod(p, 0o600)
            object.__setattr__(self, "_config_path", str(p))
            self._attach_children()
            object.__setattr__(self, "_autosave_ready", True)
        finally:
            object.__setattr__(self, "_autosave_suspended", max(0, self._autosave_suspended - 1))
        return p

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Defer write-through saves until the ``with`` block exits.

        Without this guard, every leaf-level mutation on ``cfg`` (or its
        nested ``cfg.db`` / ``cfg.llm`` dataclasses) triggers an immediate
        full YAML write. A bulk update like::

            cfg.db.host = "db.prod.example.com"
            cfg.db.user = "alice"
            cfg.db.password = "..."

        therefore performs three serialised writes, and a failure on
        the second silently loses the third change. Wrapping the block
        in ``with cfg.transaction():`` collapses that to a single
        atomic save at exit.

        If the block raises, the in-memory state remains as-is but the
        YAML is *not* updated — preventing a half-written profile from
        being persisted. The next successful mutation will re-converge
        disk and memory.

        Nested transactions are supported; only the outermost exit
        flushes.
        """
        object.__setattr__(self, "_autosave_suspended", self._autosave_suspended + 1)
        raised = False
        try:
            yield
        except BaseException:
            raised = True
            raise
        finally:
            new_level = max(0, self._autosave_suspended - 1)
            object.__setattr__(self, "_autosave_suspended", new_level)
            if (
                not raised
                and new_level == 0
                and getattr(self, "_autosave_ready", False)
                and self.write_through_config
            ):
                try:
                    self.save()
                except Exception:
                    # Same best-effort policy as _autosave_nested.
                    pass

    @property
    def is_first_run(self) -> bool:
        """True when ``load()`` did not find an existing config file on disk.

        Callers should use this to decide whether to launch the setup wizard
        or to skip auto-creating placeholder profiles.
        """
        return bool(getattr(self, "_fresh_install", False))

    @property
    def config_path(self) -> str:
        """Absolute path of the YAML config file backing this instance.

        Set by ``load()`` and updated by ``save()``. Useful for surfacing the
        exact on-disk location to the user — diagnosing a "my settings are
        not persisting" report requires knowing which file is actually being
        read and written.
        """
        return self._config_path or str(Path(self.CONFIG_DIR) / "config.yml")

    def apply_active_db_profile(self) -> None:
        name = self.active_db_profile or "default"
        if name not in self.db_profiles and self.db_profiles:
            name = next(iter(self.db_profiles.keys()))
            self.active_db_profile = name
        if name in self.db_profiles:
            self.db = self.db_profiles[name]

    def set_active_db_profile(self, name: str) -> None:
        """Set a single active DB profile (collapses the multi-pick scope).

        Equivalent to ``set_active_db_profiles([name])`` — kept as a
        thin shim because every existing call site (``cmd_use``,
        ``_maybe_modify_profiles_before_run``, etc.) speaks the
        single-name idiom and would be churn-y to migrate.
        """
        if name not in self.db_profiles:
            raise KeyError(f"Unknown DB profile: {name}")
        # The autosave triggered by ``active_db_profile = name`` runs save(),
        # which mirrors cfg.db back into db_profiles[active]. If we set the
        # active pointer before cfg.db, that mirror writes the OLD profile's
        # data over the newly-activated entry. Defer the autosave so both
        # fields converge before the YAML is written.
        with self.transaction():
            self.active_db_profile = name
            # Single-pick collapses the scope. Symmetric: any caller that
            # explicitly switches the active profile is opting back to a
            # single-DB workflow.
            self.active_db_profiles = [name]
            self.db = self.db_profiles[name]

    def set_active_db_profiles(self, names: list[str]) -> None:
        """0.11.0 multi-pick: set the persisted scope of active DB profiles.

        Validates every name, dedupes preserving order, and mirrors the
        first entry into the legacy scalar (``active_db_profile``) and
        the ``cfg.db`` shortcut so single-profile call sites still see
        the user's primary choice.

        An empty list is rejected — pass ``set_active_db_profile(name)``
        with a single profile if the user wants to narrow back down.
        """
        if not names:
            raise ValueError("At least one profile name is required")
        seen: set[str] = set()
        ordered: list[str] = []
        for raw in names:
            n = str(raw or "").strip()
            if not n or n in seen:
                continue
            if n not in self.db_profiles:
                raise KeyError(f"Unknown DB profile: {n}")
            seen.add(n)
            ordered.append(n)
        if not ordered:
            raise ValueError("At least one valid profile name is required")
        with self.transaction():
            self.active_db_profiles = ordered
            self.active_db_profile = ordered[0]
            self.db = self.db_profiles[ordered[0]]

    def effective_db_profiles(self) -> list[str]:
        """Resolved scope for the current process.

        Returns the persisted ``active_db_profiles`` list when populated,
        otherwise falls back to the legacy single-active scalar. Always
        returns a list — empty when no profile is configured. Use this
        instead of reading ``active_db_profiles`` directly so legacy
        configs (and tests) without a list field still work.
        """
        if self.active_db_profiles:
            return [n for n in self.active_db_profiles if n in self.db_profiles]
        if self.active_db_profile and self.active_db_profile in self.db_profiles:
            return [self.active_db_profile]
        return []

    def upsert_db_profile(self, name: str, db: DBConfig) -> None:
        self.db_profiles[name] = db
        if self.active_db_profile == name:
            self.db = db
        self._autosave()

    def remove_db_profile(self, name: str) -> None:
        """Remove a DB profile. The user can wipe the last one too —
        AMX surfaces the empty-config state via prompts and gates
        downstream features (``/ask``, browse) cleanly when there is
        no profile to operate on. Refusing the deletion forced a
        roundabout reset (add throwaway → activate → delete → delete
        throwaway) that's no easier than letting the user clear and
        re-add from scratch.
        """
        if name not in self.db_profiles:
            raise KeyError(f"Unknown DB profile: {name}")
        del self.db_profiles[name]
        # 0.11.0: also evict from the multi-pick scope to prevent ghost
        # selections after a profile is removed.
        if name in self.active_db_profiles:
            self.active_db_profiles = [n for n in self.active_db_profiles if n != name]
        if self.active_db_profile == name:
            # Promote the next available profile when there is one;
            # otherwise clear the active pointer + reset cfg.db to an
            # empty DBConfig so callers reading those fields don't see
            # stale data from the just-deleted profile.
            if self.db_profiles:
                self.active_db_profile = next(iter(self.db_profiles.keys()))
                self.db = self.db_profiles[self.active_db_profile]
                if not self.active_db_profiles:
                    self.active_db_profiles = [self.active_db_profile]
            else:
                self.active_db_profile = ""
                self.db = DBConfig()
                self.active_db_profiles = []
        self._autosave()

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
        # See set_active_db_profile — same race. ``active_llm_profile = name``
        # auto-saves before ``self.llm`` is refreshed, so save() mirrors the
        # OLD cfg.llm onto the freshly-activated profile and wipes its data
        # (the user-reported "/llm-profiles shows blank provider/model after
        # /add-llm-profile" bug). Use a transaction so save runs once at exit
        # with both fields consistent.
        with self.transaction():
            self.active_llm_profile = name
            self.llm = replace(self.llm_profiles[name])
            self.llm.api_key = self.llm.api_key or os.getenv("AMX_LLM_API_KEY", "")

    def upsert_llm_profile(self, name: str, llm: LLMConfig) -> None:
        normalized = replace(llm, model=normalize_llm_model(llm.provider, llm.model))
        self.llm_profiles[name] = normalized
        # Mirror the new data into cfg.llm when upserting the active profile —
        # otherwise the next save() rewrites llm_profiles[active] from the
        # stale mirror. Symmetric with upsert_db_profile.
        if self.active_llm_profile == name:
            self.llm = replace(normalized)
            self.llm.api_key = self.llm.api_key or os.getenv("AMX_LLM_API_KEY", "")
        self._autosave()

    def remove_llm_profile(self, name: str) -> None:
        """Remove an LLM profile. Symmetric with :meth:`remove_db_profile`
        — the user can wipe the last one. ``/ask`` already gates on
        :func:`SearchAgent._llm_available`; deleting the last LLM puts
        AMX in the same state as a fresh install (no LLM configured),
        and downstream surfaces (Studio /ask, CLI /search ask) show
        the "configure an LLM profile" prompt.
        """
        if name not in self.llm_profiles:
            raise KeyError(f"Unknown LLM profile: {name}")
        del self.llm_profiles[name]
        if self.active_llm_profile == name:
            if self.llm_profiles:
                self.active_llm_profile = next(iter(self.llm_profiles.keys()))
                self.llm = replace(self.llm_profiles[self.active_llm_profile])
            else:
                # No remaining profiles → clear active pointer and reset
                # cfg.llm to an empty LLMConfig. The /ask surfaces that
                # state via the configure-llm 412 (Studio) and the
                # `/search` discussion-requires-LLM message (CLI).
                self.active_llm_profile = ""
                self.llm = LLMConfig()
        if self.rag_llm_profile == name:
            self.rag_llm_profile = ""
        self._autosave()

    def effective_rag_llm(self) -> LLMConfig:
        """Return the LLMConfig the RAG agent should use right now.

        Falls back to ``self.llm`` (the active profile) when
        ``rag_llm_profile`` is empty or names a profile that no longer
        exists. The fallback is intentionally silent: a stale value is
        already cleaned up at load time, but a mid-session race (profile
        deleted while another module holds an old cfg) should still land
        on the active profile rather than raise.
        """
        name = (self.rag_llm_profile or "").strip()
        if name and name in self.llm_profiles:
            return self.llm_profiles[name]
        return self.llm

    def upsert_doc_profile(self, name: str, paths: list[str]) -> None:
        self.doc_profiles[name] = list(paths)
        self._autosave()

    def remove_doc_profile(self, name: str) -> None:
        if name not in self.doc_profiles:
            raise KeyError(f"Unknown document profile: {name}")
        del self.doc_profiles[name]
        if self.active_doc_profile == name:
            self.active_doc_profile = next(iter(self.doc_profiles.keys()), "")
        self._autosave()

    def upsert_code_profile(self, name: str, path: str) -> None:
        self.code_profiles[name] = path
        self._autosave()

    def remove_code_profile(self, name: str) -> None:
        if name not in self.code_profiles:
            raise KeyError(f"Unknown codebase profile: {name}")
        del self.code_profiles[name]
        if self.active_code_profile == name:
            self.active_code_profile = next(iter(self.code_profiles.keys()), "")
        self._autosave()

    def _autosave(self) -> None:
        if not self.write_through_config:
            return
        if getattr(self, "_autosave_suspended", 0) > 0:
            return
        try:
            self.save()
        except Exception:
            # Write-through persistence is best-effort and should never break runtime flow.
            pass

    def _autosave_nested(self) -> None:
        if not getattr(self, "_autosave_ready", False):
            return
        if getattr(self, "_autosave_suspended", 0) > 0:
            return
        self._autosave()

    def _attach_children(self) -> None:
        with suppress(Exception):
            object.__setattr__(self.db, "_amx_owner", self)
        with suppress(Exception):
            object.__setattr__(self.llm, "_amx_owner", self)
        with suppress(Exception):
            object.__setattr__(self.embedding, "_amx_owner", self)
        for profile in getattr(self, "db_profiles", {}).values():
            with suppress(Exception):
                object.__setattr__(profile, "_amx_owner", self)
        for profile in getattr(self, "llm_profiles", {}).values():
            with suppress(Exception):
                object.__setattr__(profile, "_amx_owner", self)

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
