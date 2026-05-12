"""Central configuration store shared across all AMX modules.

INTERNAL — not part of the public API. Programmatic users should go
through :meth:`amx.core.AMXApplication.load`, which returns a
configured application without exposing the ``AMXConfig`` dataclass
shape. The shape is **not** stable across
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

#: Environment variable that overrides the default ``~/.amx`` config
#: directory. Useful for running an isolated dev/test session in
#: parallel with a production AMX setup that lives at the default
#: path — set ``AMX_CONFIG_DIR=$HOME/.amx-dev amx`` to keep the two
#: from sharing config, history.db, uploads, or chroma_db. The
#: doctor command at ``amx/cli_support/commands/doctor.py`` already
#: documents this variable; the implementation just resolves it
#: here so every AMXConfig instance picks it up automatically.
_CONFIG_DIR_ENV_VAR = "AMX_CONFIG_DIR"


def _resolve_config_dir() -> str:
    """Return the path AMX should use as its config / state directory.

    Resolution order:
      1. ``$AMX_CONFIG_DIR`` if set + non-empty (expanded for ``~``).
      2. ``~/.amx`` (the historical default).
    """
    override = os.environ.get(_CONFIG_DIR_ENV_VAR, "").strip()
    if override:
        return str(Path(override).expanduser())
    return str(Path.home() / ".amx")


#: How many rotated config.yml backups to keep next to the live file.
#: Stored as ``config.yml.bak.1`` (newest) through ``config.yml.bak.N``
#: (oldest). Five generations matches the convention of editors with
#: built-in quick-undo and is enough headroom for a user to notice and
#: recover within a session even if multiple bad saves stack up before
#: they spot the regression.
BACKUP_ROTATION_KEEP = 5


def _detect_silent_truncation(cfg) -> list[str]:
    """Return integrity problems that should block a save.

    The PR #351 autosave race produced configs where ``active_*_profile``
    pointed at a name but the matching ``*_profiles`` dict was empty —
    the on-disk evidence of in-memory state being silently truncated
    before write. Any save in that shape would propagate the loss to
    every rotated backup over time. We catch the pattern at the
    serialization boundary and refuse to write.

    The placeholder ``"default"`` is ignored because the loader injects
    it on fresh installs where no real profile choice has ever been
    recorded — flagging it would block the very first legitimate save.
    """
    problems: list[str] = []
    pairs = (
        ("active_db_profile", "db_profiles"),
        ("active_llm_profile", "llm_profiles"),
        ("active_doc_profile", "doc_profiles"),
        ("active_code_profile", "code_profiles"),
    )
    for active_attr, dict_attr in pairs:
        active = getattr(cfg, active_attr, "") or ""
        bucket = getattr(cfg, dict_attr, {}) or {}
        if active and active != "default" and active not in bucket:
            problems.append(f"{active_attr}={active!r} but {dict_attr} has no such entry")
    return problems


def _rotate_config_backups(target: Path, keep: int = BACKUP_ROTATION_KEEP) -> None:
    """Rotate ``<target>.bak.1..N`` immediately before overwriting target.

    Sequence (called BEFORE the atomic write):
      * Drop the oldest backup (``.bak.N``).
      * Promote ``.bak.N-1`` → ``.bak.N``, ``.bak.N-2`` → ``.bak.N-1`` ...
      * Copy the current ``target`` (the about-to-be-overwritten file)
        to ``.bak.1``.

    Best-effort throughout: a failure here must NOT block the save.
    The live save is still atomic on its own, so the worst case from a
    rotation failure is missing a single backup generation, not a
    corrupted file.
    """
    if not target.exists():
        return  # first-ever save: nothing to back up yet
    try:
        # Drop the oldest first to make room.
        oldest = target.with_suffix(target.suffix + f".bak.{keep}")
        if oldest.exists():
            with suppress(OSError):
                oldest.unlink()
        # Promote each generation by one slot.
        for i in range(keep - 1, 0, -1):
            src = target.with_suffix(target.suffix + f".bak.{i}")
            dst = target.with_suffix(target.suffix + f".bak.{i + 1}")
            if src.exists():
                with suppress(OSError):
                    src.replace(dst)
        # Copy live → .bak.1 (copy, not rename, so the atomic save
        # below still has a target to overwrite).
        import shutil as _shutil

        dst = target.with_suffix(target.suffix + ".bak.1")
        with suppress(OSError):
            _shutil.copy2(target, dst)
            # Backup inherits the same owner-only permissions as the
            # source — credentials shouldn't be world-readable.
            with suppress(OSError):
                os.chmod(dst, 0o600)
    except Exception:
        # Rotation is purely opportunistic; never propagate a failure
        # that would block the user's save.
        return


def list_config_backups(config_path: Path | None = None) -> list[Path]:
    """Return the on-disk rotated backups, newest first.

    Used by ``amx restore-config`` to show the user which generations
    are available. Defaults to ``~/.amx/config.yml.bak.*`` (or the
    ``AMX_CONFIG_DIR`` override).
    """
    if config_path is None:
        config_path = Path(_resolve_config_dir()) / "config.yml"
    parent = config_path.parent
    if not parent.is_dir():
        return []
    out: list[tuple[int, Path]] = []
    for child in parent.iterdir():
        name = child.name
        prefix = config_path.name + ".bak."
        if not name.startswith(prefix):
            continue
        try:
            idx = int(name[len(prefix) :])
        except ValueError:
            continue
        out.append((idx, child))
    out.sort(key=lambda pair: pair[0])
    return [path for _, path in out]


def restore_config_from_backup(
    backup: Path,
    config_path: Path | None = None,
) -> Path:
    """Atomically copy ``backup`` over the live config.

    The current live file is rotated into ``.bak.1`` first (via the
    standard rotation helper) so a restore is itself undoable. Returns
    the path of the restored live file.
    """
    if config_path is None:
        config_path = Path(_resolve_config_dir()) / "config.yml"
    if not backup.is_file():
        raise FileNotFoundError(f"backup not found: {backup}")
    # Snapshot the backup's contents BEFORE rotation: rotating the live
    # file would shift .bak.N → .bak.N+1 and could move the user's
    # chosen backup out from under us (e.g. picking .bak.5 with the
    # chain already full).
    payload = backup.read_bytes()
    _rotate_config_backups(config_path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_bytes(payload)
    with suppress(OSError):
        os.chmod(config_path, 0o600)
    return config_path


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
    """Replace one ``keyring:...`` reference with its plaintext value.

    When the keyring backend is unavailable (eg. macOS denied access
    after a Python reinstall) or the entry no longer exists, the
    reference STAYS in the mapping unchanged. The previous behaviour
    overwrote the field with ``""`` which then became permanent on the
    next ``cfg.save()`` — externalise skips empty values, so the YAML
    lost the keyring pointer and the user had to re-enter the secret
    even after fixing the backend. Leaving the reference in place lets
    a subsequent process resolve it once the backend recovers.
    """
    value = mapping.get(field_name, "")
    if not is_secret_reference(value):
        return
    try:
        keyring_key = parse_reference(value)
        resolved = store.get(keyring_key)
    except Exception:
        resolved = None
    if resolved:
        mapping[field_name] = resolved
    # else: keep the reference intact so cfg.save() can still
    # round-trip it to YAML when the backend is healthy again.


def _resolve_secrets_in_data(data: dict[str, Any], store: SecretStore) -> dict[str, Any]:
    """Replace ``keyring:...`` references in ``data`` with plaintext values from the store.

    Mutates ``data`` in place. References that cannot be resolved
    (keyring unavailable, or key removed) are LEFT AS REFERENCES so a
    subsequent run can recover once the backend is healthy — overwriting
    them with ``""`` would silently destroy the YAML pointer on the
    next save.
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
    # PR γ: per-column code-RAG fan-out, parallel to ``rag_col_hits``.
    # ``0`` preserves the pre-PR-γ behaviour (one neutral
    # ``"<schema> <table>"`` semantic query only). Higher values let the
    # CodeAgent also issue one ``<table>.<col>`` query per column so
    # column-specific call-sites surface as separate citations. Kept
    # conservative by default because each query costs a Chroma round
    # trip and embedding tokens on metered providers.
    code_col_hits: int = 0


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
            code_col_hits=0,
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
            code_col_hits=1,
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
            code_col_hits=1,
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
        code_col_hits=0,
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

    # DuckDB extras — ``read_only`` lets multiple AMX processes attach
    # the same ``.duckdb`` file (the connector library otherwise grabs
    # an exclusive lock). ``motherduck_token`` carries the MotherDuck
    # PAT when the user pins a ``md:`` / ``md:<db>`` database path so
    # the cloud-attach flow doesn't fall back to the env var.
    read_only: bool = False
    motherduck_token: str = ""

    # BigQuery extras — ``location`` pins query jobs to a specific GCP
    # region (EU, US, asia-east1, …) so cross-region data isn't moved
    # invisibly. ``impersonate_service_account`` lets developers sign
    # queries as a workload identity without minting personal SA keys.
    location: str = ""
    impersonate_service_account: str = ""
    # ClickHouse TLS verification (visible only when ``secure=True``).
    # ``ca_cert`` lets corporate users point at a private CA bundle;
    # ``verify=False`` is the escape hatch when even the CA path can't be
    # configured (e.g., TLS-inspecting proxy with a non-distributable
    # root). Mirrors the Databricks / MSSQL TLS surface.
    ca_cert: str = ""
    verify: bool = True

    # PostgreSQL TLS — corporate / managed PG (RDS, CloudSQL) increasingly
    # requires verify-full with a private root. The default is libpq's
    # ``prefer`` which negotiates TLS but does not validate the server
    # cert, matching today's behaviour. Setting ``sslmode`` to
    # ``verify-ca`` / ``verify-full`` activates path validation against
    # ``sslrootcert`` (file path).
    sslmode: str = ""
    sslrootcert: str = ""

    # MySQL TLS — MySQL accepts ``ssl_disabled`` and ``ssl_ca`` as
    # connect kwargs (forwarded by SQLAlchemy via URL params). The
    # default matches the driver default (TLS preferred when the
    # server advertises it).
    ssl_disabled: bool = False
    ssl_ca: str = ""

    # Snowflake TLS / OCSP — corporate proxies that block OCSP
    # responder traffic time out the connect handshake. Setting
    # ``ocsp_fail_open=True`` lets the driver continue when the OCSP
    # check itself fails (still validates the cert path). ``insecure_mode``
    # is the last-resort bypass, matching the connector's
    # ``snowflake.connector.connect(..., insecure_mode=True)`` flag.
    insecure_mode: bool = False
    ocsp_fail_open: bool = False

    # Profiling guardrails
    profiling_mode: str = "full"  # full | sampled | metadata
    profiling_max_rows: int = 1_000_000  # skip full column scans above this row estimate (0=off)
    profiling_sample_size: int = 5
    # When True, metered backends (BigQuery / Snowflake / Databricks)
    # emit ``APPROX_COUNT_DISTINCT`` / ``approx_count_distinct`` instead
    # of ``COUNT(DISTINCT col)``. The exact aggregate scans every row in
    # the sampled slice (TABLESAMPLE / SAMPLE only narrows the input,
    # not the COUNT-DISTINCT hash), so wide tables with high-cardinality
    # columns can rack up credits even with the 1% sample. The
    # HyperLogLog approximation is within ~1-2% on realistic data and
    # bills a small fraction of the credits. Defaults to False so
    # existing behaviour is preserved.
    profiling_approximate: bool = False
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
            # Snowflake driver-level TLS / OCSP. Both arrive via
            # snowflake-sqlalchemy as URL query params, then surface as
            # ``snowflake.connector.connect(..., insecure_mode=..., ocsp_fail_open=...)``
            # kwargs. Default values (False) match the driver default so
            # opting out is a no-op until the user toggles them.
            if self.insecure_mode:
                params.append("insecure_mode=true")
            if self.ocsp_fail_open:
                params.append("ocsp_fail_open=true")
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
            params: list[str] = []
            if self.credentials_path:
                params.append(f"credentials_path={quote_plus(self.credentials_path)}")
            if self.location:
                # sqlalchemy-bigquery forwards ``location`` to the
                # BigQuery client so query jobs run in the pinned
                # region instead of the project's default.
                params.append(f"location={quote_plus(self.location)}")
            if self.impersonate_service_account:
                params.append(
                    f"impersonate_service_account={quote_plus(self.impersonate_service_account)}"
                )
            if params:
                url += "?" + "&".join(params)
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
            # MySQL TLS via PyMySQL connect kwargs surfaced as URL query
            # params. ``ssl_disabled=true`` explicitly opts out of TLS for
            # legacy intra-data-centre setups; ``ssl_ca`` activates path
            # validation against a private CA bundle. Neither is set by
            # default so the driver keeps negotiating opportunistically.
            params: list[str] = []
            if self.ssl_disabled:
                params.append("ssl_disabled=true")
            elif self.ssl_ca:
                params.append(f"ssl_ca={quote_plus(self.ssl_ca)}")
            if params:
                url += "?" + "&".join(params)
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
            # ClickHouse TLS knobs are only meaningful on HTTPS. The
            # ``verify=false`` toggle drops cert validation entirely
            # (escape hatch for TLS-inspecting proxies that present a
            # non-distributable root); ``ca_cert`` activates path
            # validation against a private bundle. Both arrive at the
            # urllib3 layer via clickhouse-connect's standard kwargs.
            if self.secure:
                params: list[str] = []
                if self.ca_cert:
                    params.append(f"ca_cert={quote_plus(self.ca_cert)}")
                if not self.verify:
                    params.append("verify=false")
                if params:
                    url += "?" + "&".join(params)
            return url

        if self.backend == "duckdb":
            # File-based or in-memory. ``database`` carries the path
            # (or ``:memory:``, or ``md:[<db>]`` for MotherDuck). No
            # host/port/user/password.
            target = self.database or ":memory:"
            url = f"duckdb:///{target}"
            params: list[str] = []
            if self.read_only:
                # duckdb-engine forwards ``read_only=true`` to
                # ``duckdb.connect`` so two AMX processes can share the
                # same file without fighting over the exclusive write
                # lock. The flag is ignored for ``:memory:`` and
                # MotherDuck URIs.
                params.append("read_only=true")
            if self.motherduck_token and (target.startswith("md:") or target == "md"):
                # MotherDuck attach: the token is passed via the
                # ``motherduck_token`` query parameter so it never
                # surfaces in process listings or shell history. The
                # MOTHERDUCK_TOKEN env var also works but explicit is
                # better when the profile pins the cloud DB.
                params.append(f"motherduck_token={quote_plus(self.motherduck_token)}")
            if params:
                url += "?" + "&".join(params)
            return url

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
        # PostgreSQL TLS: libpq honours ``sslmode`` and ``sslrootcert``
        # as URL query params, which is the SQLAlchemy convention for
        # psycopg2. ``sslmode=verify-full`` plus ``sslrootcert=<path>`` is
        # the corporate / managed-PG (RDS, CloudSQL, Azure Database for
        # PostgreSQL) idiom; the empty default keeps libpq's negotiation
        # behaviour unchanged for users who don't touch the field.
        pg_params: list[str] = []
        if self.sslmode:
            pg_params.append(f"sslmode={quote_plus(self.sslmode)}")
        if self.sslrootcert:
            pg_params.append(f"sslrootcert={quote_plus(self.sslrootcert)}")
        if pg_params:
            url += "?" + "&".join(pg_params)
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
        # TLS fields surfaced in PR 2 of the connector audit. The
        # defaults match the driver-native defaults so existing YAML
        # without these keys behaves identically.
        ca_cert=str(m.get("ca_cert", "")),
        verify=bool(m.get("verify", True)),
        sslmode=str(m.get("sslmode", "")),
        sslrootcert=str(m.get("sslrootcert", "")),
        ssl_disabled=bool(m.get("ssl_disabled", False)),
        ssl_ca=str(m.get("ssl_ca", "")),
        insecure_mode=bool(m.get("insecure_mode", False)),
        ocsp_fail_open=bool(m.get("ocsp_fail_open", False)),
        read_only=bool(m.get("read_only", False)),
        motherduck_token=str(m.get("motherduck_token", "")),
        location=str(m.get("location", "")),
        impersonate_service_account=str(m.get("impersonate_service_account", "")),
        profiling_mode=str(m.get("profiling_mode", "full")),
        profiling_max_rows=int(m.get("profiling_max_rows", 1_000_000)),
        profiling_sample_size=int(m.get("profiling_sample_size", 5)),
        profiling_stats_batch_size=int(m.get("profiling_stats_batch_size", 50)),
        profiling_approximate=bool(m.get("profiling_approximate", False)),
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
                "sslmode": db.sslmode,
                "sslrootcert": db.sslrootcert,
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
                "insecure_mode": db.insecure_mode,
                "ocsp_fail_open": db.ocsp_fail_open,
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
                "location": db.location,
                "impersonate_service_account": db.impersonate_service_account,
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
                "ssl_disabled": db.ssl_disabled,
                "ssl_ca": db.ssl_ca,
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
                "ca_cert": db.ca_cert,
                "verify": db.verify,
            }
        )
    elif db.backend == "duckdb":
        base.update(
            {
                "database": db.database,
                "read_only": db.read_only,
                "motherduck_token": db.motherduck_token,
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
            "profiling_approximate": bool(db.profiling_approximate),
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
    # Default output-token budget for a single LLM call. Bumped from 4096
    # in 2026-05 because modern frontier and budget models alike happily
    # produce 8–16k tokens of structured output, and the previous cap
    # silently truncated AMX runs against any model with a hidden reasoning
    # phase — most painfully on Kimi K2.x and similar OpenRouter routes
    # where the agent burned every token on internal thinking and emitted
    # nothing visible. Users who care about cost should lower this in their
    # profile; AMX defaults to "user knows what they're doing".
    max_tokens: int = 16_384
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
    # User-defined cost override in USD per 1M tokens. When BOTH are set,
    # they win over fetched LiteLLM / OpenRouter prices (resolution order
    # in ``amx/llm/pricing.py:lookup_price``). A half-override — only
    # input or only output — is treated as no override to avoid the
    # "set output, forgot input" footgun where AMX would silently bill
    # the user at "free input + market output" or vice versa.
    custom_input_cost_per_mtok: float | None = None
    custom_output_cost_per_mtok: float | None = None
    # Per-query wall-clock cap on Chroma similarity retrieval. When the
    # underlying ``RAGStore.query`` call exceeds this many seconds AMX
    # logs a structured warning, surfaces a diagnostic to the
    # orchestrator, and proceeds with an empty hit list so the run is
    # not blocked by a stalled vector store. Set to ``0`` (or a negative
    # value) to disable the timeout entirely.
    rag_query_timeout_sec: float = 5.0

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
        max_tokens=int(m.get("max_tokens", 16_384)),
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
        custom_input_cost_per_mtok=_optional_nonneg_float(m.get("custom_input_cost_per_mtok")),
        custom_output_cost_per_mtok=_optional_nonneg_float(m.get("custom_output_cost_per_mtok")),
        rag_query_timeout_sec=float(m.get("rag_query_timeout_sec", 5.0) or 5.0),
    )


def _optional_nonneg_float(value: Any) -> float | None:
    """Coerce a YAML scalar to ``float | None`` for cost overrides.

    Empty string -> None (Studio sends ``""`` when the user clears the
    field). Negative -> None (a negative rate would silently subtract
    from the aggregate cost — almost certainly a typo).
    """
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f < 0:
        return None
    return f


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
        "custom_input_cost_per_mtok": llm.custom_input_cost_per_mtok,
        "custom_output_cost_per_mtok": llm.custom_output_cost_per_mtok,
        "rag_query_timeout_sec": llm.rag_query_timeout_sec,
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
    # Multi-profile override for ``/run``. When non-empty, the
    # orchestrator unions every named profile's
    # :meth:`effective_doc_paths` into a single ``RAGStore`` source
    # filter for the run, so a single ``/run`` can pull context from
    # multiple doc collections at once. Empty list (the default) means
    # "use ``active_doc_profile`` exactly like before" — no migration
    # needed for existing configs.
    run_doc_profiles: list[str] = field(default_factory=list)
    # Per-doc-profile health telemetry updated at the end of every
    # ``RAGStore.ingest`` call. ``last_ingested_at`` is a Unix
    # timestamp (or ``0.0`` when the profile has never been ingested);
    # ``last_error`` carries a one-line reason from the most recent
    # failure (or ``""`` when the last run was clean). Surfaced by
    # ``GET /api/profiles/docs/{name}/health`` and the Studio Settings
    # page.
    doc_profiles_last_ingested_at: dict[str, float] = field(default_factory=dict)
    doc_profiles_last_error: dict[str, str] = field(default_factory=dict)
    code_profiles: dict[str, str] = field(default_factory=dict)
    active_code_profile: str = ""
    # Multi-profile override for ``/run`` (PR δ — mirrors
    # ``run_doc_profiles``). When non-empty the orchestrator unions
    # every named profile's ``effective_code_paths`` into a single
    # ``source_filters`` list for ``query_code_snippets``. Empty list
    # (the default) → fall back to the single active profile, matching
    # the pre-PR-δ single-profile behaviour byte-for-byte.
    run_code_profiles: list[str] = field(default_factory=list)
    # Per-code-profile health telemetry stamped at the end of every
    # ``index_codebase_tree`` call (PR δ — mirrors
    # ``doc_profiles_last_ingested_at`` / ``doc_profiles_last_error``).
    # Surfaced by ``GET /api/profiles/code/{name}/health`` and the
    # Studio Settings → Code panel so users can tell at a glance which
    # code profiles are wired up and indexed.
    code_profile_last_indexed_at: dict[str, float] = field(default_factory=dict)
    code_profile_last_error: dict[str, str] = field(default_factory=dict)
    # Map a doc/code profile to the DB profiles it documents. Empty list
    # (or absent key) = global, matches every DB scope. When `/ask` runs
    # against scope_profiles=[X], only doc/code profiles whose linked
    # list is empty OR contains X are pulled into the LLM tool window.
    doc_profile_linked_dbs: dict[str, list[str]] = field(default_factory=dict)
    code_profile_linked_dbs: dict[str, list[str]] = field(default_factory=dict)
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

    CONFIG_DIR: str = field(default_factory=lambda: _resolve_config_dir(), init=False)
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
            "run_doc_profiles",
            "doc_profiles_last_ingested_at",
            "doc_profiles_last_error",
            "code_profiles",
            "active_code_profile",
            "run_code_profiles",
            "code_profile_last_indexed_at",
            "code_profile_last_error",
            "doc_profile_linked_dbs",
            "code_profile_linked_dbs",
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

            run_doc_raw = data.get("run_doc_profiles") or []
            if isinstance(run_doc_raw, list):
                cfg.run_doc_profiles = [
                    str(name).strip()
                    for name in run_doc_raw
                    if isinstance(name, str) and str(name).strip()
                ]

            doc_last_ingested_raw = data.get("doc_profiles_last_ingested_at") or {}
            if isinstance(doc_last_ingested_raw, dict):
                for name, ts in doc_last_ingested_raw.items():
                    try:
                        cfg.doc_profiles_last_ingested_at[str(name)] = float(ts)
                    except (TypeError, ValueError):
                        continue
            doc_last_error_raw = data.get("doc_profiles_last_error") or {}
            if isinstance(doc_last_error_raw, dict):
                for name, err in doc_last_error_raw.items():
                    cfg.doc_profiles_last_error[str(name)] = str(err or "")

            code_prof_raw = data.get("code_profiles") or {}
            if isinstance(code_prof_raw, dict):
                for name, path in code_prof_raw.items():
                    if isinstance(path, str):
                        cfg.code_profiles[str(name)] = path

            cfg.active_code_profile = str(data.get("active_code_profile") or "")

            # PR δ: multi-profile code scope + per-profile health
            # telemetry. Missing keys preserve pre-PR-δ defaults so the
            # YAML round-trip stays backwards-compatible.
            run_code_raw = data.get("run_code_profiles") or []
            if isinstance(run_code_raw, list):
                cfg.run_code_profiles = [
                    str(name).strip()
                    for name in run_code_raw
                    if isinstance(name, str) and str(name).strip()
                ]

            code_last_indexed_raw = data.get("code_profile_last_indexed_at") or {}
            if isinstance(code_last_indexed_raw, dict):
                for name, ts in code_last_indexed_raw.items():
                    try:
                        cfg.code_profile_last_indexed_at[str(name)] = float(ts)
                    except (TypeError, ValueError):
                        continue
            code_last_error_raw = data.get("code_profile_last_error") or {}
            if isinstance(code_last_error_raw, dict):
                for name, err in code_last_error_raw.items():
                    cfg.code_profile_last_error[str(name)] = str(err or "")

            doc_link_raw = data.get("doc_profile_linked_dbs") or {}
            if isinstance(doc_link_raw, dict):
                for name, dbs in doc_link_raw.items():
                    if isinstance(dbs, list):
                        cfg.doc_profile_linked_dbs[str(name)] = [
                            str(x) for x in dbs if isinstance(x, str) and x
                        ]
            code_link_raw = data.get("code_profile_linked_dbs") or {}
            if isinstance(code_link_raw, dict):
                for name, dbs in code_link_raw.items():
                    if isinstance(dbs, list):
                        cfg.code_profile_linked_dbs[str(name)] = [
                            str(x) for x in dbs if isinstance(x, str) and x
                        ]

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
            # Three distinct cases collapse into the same observable
            # state here:
            #   1) Genuinely no saved DB profiles (first run / clean
            #      install). YAML's ``active_db_profile`` is the
            #      loader-injected placeholder ``"default"`` from the
            #      ``or "default"`` fallback at the assignment above.
            #   2) Truly empty YAML state — ``active_db_profile`` is
            #      the empty string. Same as (1) for our purposes.
            #   3) The YAML carried a USER-CHOSEN ``active_db_profile``
            #      (anything other than the ``"default"`` placeholder)
            #      but ``db_profiles`` parsed as empty — almost always
            #      a sign of upstream data corruption (the dict body
            #      was wiped while the scalar reference survived, as
            #      seen during the PR #351 reload race investigation).
            # Cases 1 + 2 we clear normally (the CLI prompts setup).
            # Case 3 we preserve the active name so the user has a
            # breadcrumb back to recovery — ``amx doctor`` /
            # ``amx restore-config`` (PR β) can use the surviving
            # name to pick the right backup to restore. Clobbering
            # the name would erase that breadcrumb and turn a
            # recoverable situation into a silent data loss.
            is_real_user_choice = cfg.active_db_profile and cfg.active_db_profile != "default"
            if is_real_user_choice:
                import logging as _logging

                _logging.getLogger("amx.config").warning(
                    "db_profiles is empty but active_db_profile=%r is set; "
                    "preserving the reference so recovery tooling can find "
                    "the right backup. Run `amx doctor` for diagnosis.",
                    cfg.active_db_profile,
                )
                # Keep ``active_db_profile`` as-is; clear the scope
                # list only if it points at non-existent profiles.
                cfg.active_db_profiles = []
            else:
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

        # Prune ghost references in linked-DB maps:
        #   - keys pointing at doc/code profiles that no longer exist
        #   - DB profile names inside the lists that were deleted
        # Without this, an outdated YAML carrying ``doc_profile_linked_dbs:
        # {contracts: [removed_pg]}`` would still scope ``/ask`` against a
        # deleted DB and silently shrink retrieval results.
        cfg.doc_profile_linked_dbs = {
            name: [db for db in dbs if db in cfg.db_profiles]
            for name, dbs in cfg.doc_profile_linked_dbs.items()
            if name in cfg.doc_profiles
        }
        cfg.code_profile_linked_dbs = {
            name: [db for db in dbs if db in cfg.db_profiles]
            for name, dbs in cfg.code_profile_linked_dbs.items()
            if name in cfg.code_profiles
        }

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
        try:
            object.__setattr__(cfg, "_loaded_mtime", p.stat().st_mtime if p.exists() else 0.0)
        except OSError:
            object.__setattr__(cfg, "_loaded_mtime", 0.0)
        return cfg

    def reload_if_stale(self) -> bool:
        """Re-read the YAML and copy its values onto self when disk is newer.

        Studio and the CLI share the same ``~/.amx/config.yml``. When the
        user edits a doc/code/LLM profile in Studio while a CLI session
        is open, the CLI's in-memory :class:`AMXConfig` would otherwise
        keep showing the old values until restart. Called once per
        prompt input by :func:`run_interactive_session` so the dispatch
        cycle always sees fresh data without any background watcher.

        Returns ``True`` when a reload happened, ``False`` otherwise.
        Best-effort: a missing or unreadable file is treated as "no
        change" so a transient disk hiccup doesn't blow up the prompt.
        """
        path_str = getattr(self, "_config_path", "") or ""
        if not path_str:
            return False
        try:
            disk_mtime = Path(path_str).stat().st_mtime
        except OSError:
            return False
        last = float(getattr(self, "_loaded_mtime", 0.0) or 0.0)
        # Use a small slop to avoid reloading on our own save() — the
        # save() path bumps ``_loaded_mtime`` to the post-write value,
        # but a same-second write from elsewhere should still trigger.
        if disk_mtime <= last:
            return False
        try:
            fresh = AMXConfig.load(path_str)
        except Exception:
            return False
        # CRITICAL: suspend autosave for the entire reload window.
        # Without this, the scalar ``setattr`` loop below would
        # trigger ``_autosave_nested`` on every assignment — and the
        # FIRST scalar set would persist a partially-merged state
        # (fresh scalars + stale dicts that haven't been swapped
        # yet) back to disk. Real-world symptom: user's
        # ``db_profiles`` / ``llm_profiles`` got progressively
        # truncated to empty maps while their ``active_*_profile``
        # name scalars survived, producing the "History store isn't
        # initialized yet — activate a DB profile" error and the
        # appearance that profile data had been lost. The data was
        # intact in memory at every individual moment; the intermediate
        # save just kept writing the half-state to YAML.
        object.__setattr__(self, "_autosave_suspended", self._autosave_suspended + 1)
        try:
            # Mutate self in place: callers (history_store, embedding
            # provider, slash-command handlers) hold references to this
            # specific instance and must continue to see the same object
            # with updated fields. Touch every mutable surface that
            # ``load()`` populates from YAML. Anything missing here is a
            # known bug — keep this list in sync when adding new YAML
            # keys (also keep it in sync with ``save()``).

            # DICTS FIRST. Profile dicts and nested telemetry dicts —
            # swap contents in-place so existing references keep
            # pointing at the right collection object. Doing this
            # BEFORE the scalar loop is defensive: even if the
            # autosave suspension above were ever removed by mistake,
            # the dicts would already be fresh by the time a save
            # could fire from a scalar setattr.
            for dict_attr in (
                "db_profiles",
                "llm_profiles",
                "doc_profiles",
                "code_profiles",
                "doc_profiles_last_ingested_at",
                "doc_profiles_last_error",
                "code_profile_last_indexed_at",
                "code_profile_last_error",
                "doc_profile_linked_dbs",
                "code_profile_linked_dbs",
            ):
                target = getattr(self, dict_attr, None)
                source = getattr(fresh, dict_attr, None)
                if isinstance(target, dict) and isinstance(source, dict):
                    target.clear()
                    target.update(source)

            # Nested dataclasses (``cfg.db``, ``cfg.llm``,
            # ``cfg.embedding``) before scalars too, same reason.
            for nested_attr in ("db", "llm", "embedding"):
                fresh_nested = getattr(fresh, nested_attr, None)
                if fresh_nested is None:
                    continue
                target_nested = getattr(self, nested_attr, None)
                if target_nested is None:
                    continue
                for fld in fresh_nested.__dataclass_fields__:
                    try:
                        setattr(target_nested, fld, getattr(fresh_nested, fld))
                    except Exception:
                        continue

            # Scalars LAST. Now safe even if autosave were to fire —
            # all dicts are already aligned with disk.
            for attr in (
                "doc_paths",
                "code_paths",
                "selected_schemas",
                "selected_tables",
                "active_db_profile",
                "active_db_profiles",
                "current_schema",
                "current_table",
                "active_llm_profile",
                "rag_llm_profile",
                "active_doc_profile",
                "run_doc_profiles",
                "active_code_profile",
                "run_code_profiles",
                "history_store_enabled",
                "history_store_profile",
                "history_store_database",
                "history_store_schema",
            ):
                if hasattr(fresh, attr):
                    try:
                        setattr(self, attr, getattr(fresh, attr))
                    except Exception:
                        continue
        finally:
            object.__setattr__(
                self,
                "_autosave_suspended",
                max(0, self._autosave_suspended - 1),
            )
        object.__setattr__(self, "_loaded_mtime", disk_mtime)
        return True

    def save(self, path: str | None = None) -> Path:
        p = Path(path) if path else Path(self._config_path or Path(self.CONFIG_DIR) / "config.yml")
        p.parent.mkdir(parents=True, exist_ok=True)
        object.__setattr__(self, "_autosave_suspended", self._autosave_suspended + 1)
        try:
            if self.active_db_profile:
                self.db_profiles[self.active_db_profile] = self.db
            if self.active_llm_profile:
                self.llm_profiles[self.active_llm_profile] = replace(self.llm)

            # Shadow integrity check: ``db_profiles`` and ``llm_profiles``
            # have just been auto-repaired above (the historical save
            # contract). Anything still inconsistent at this point —
            # ``active_doc_profile`` / ``active_code_profile`` pointing
            # at an absent dict entry — matches the PR #351 autosave
            # race signature with no built-in recovery. Refuse the
            # write so the truncation doesn't propagate into the
            # rotated backups.
            problems = _detect_silent_truncation(self)
            if problems:
                import logging as _logging

                _logging.getLogger("amx.config").error(
                    "AMX config save refused — in-memory state is "
                    "internally inconsistent:\n  - %s\nDisk file %s left "
                    "untouched. Run /restore-config to recover from the "
                    "most recent backup if needed.",
                    "\n  - ".join(problems),
                    p,
                )
                return p

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
            data["run_doc_profiles"] = list(self.run_doc_profiles)
            data["doc_profiles_last_ingested_at"] = {
                k: float(v) for k, v in self.doc_profiles_last_ingested_at.items()
            }
            data["doc_profiles_last_error"] = {
                k: str(v) for k, v in self.doc_profiles_last_error.items() if v
            }
            data["code_paths"] = code_paths_yaml
            data["code_profiles"] = dict(self.code_profiles)
            data["active_code_profile"] = self.active_code_profile
            data["run_code_profiles"] = list(self.run_code_profiles)
            data["code_profile_last_indexed_at"] = {
                k: float(v) for k, v in self.code_profile_last_indexed_at.items()
            }
            data["code_profile_last_error"] = {
                k: str(v) for k, v in self.code_profile_last_error.items() if v
            }
            data["doc_profile_linked_dbs"] = {
                k: list(v) for k, v in self.doc_profile_linked_dbs.items() if v
            }
            data["code_profile_linked_dbs"] = {
                k: list(v) for k, v in self.code_profile_linked_dbs.items() if v
            }
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
            # Rotating backup BEFORE the atomic write: if the new
            # YAML turns out to be corrupted or wipes profile dicts
            # via some future bug, the user can restore from one of
            # the previous N saves. Five generations is the same
            # ballpark Vim, JetBrains, and macOS Finder use for
            # quick undo. Best-effort: a backup failure must never
            # block the save itself (the new write is still atomic
            # and the user's in-memory state is the source of truth
            # at this instant).
            with suppress(Exception):
                _rotate_config_backups(p, keep=BACKUP_ROTATION_KEEP)
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
            # Track the post-write mtime so :meth:`reload_if_stale`
            # doesn't mistake our own save for an external change and
            # ping-pong the user's in-memory edits with a stale reload.
            try:
                object.__setattr__(self, "_loaded_mtime", p.stat().st_mtime)
            except OSError:
                pass
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
        """Reconcile the persisted DB pointer at config-load time.

        ``active_db_profile`` is now an *internal* default-fallback
        pointer -- the Studio UI no longer exposes activation, and the
        SPA picks a profile per-action (Run, Ask, Browse). The pointer
        only matters for CLI commands invoked without ``--profile``
        (the legacy ``amx run`` flow), where some deterministic
        default has to win. We keep accepting an explicit value
        because the CLI's ``/use-db`` still sets it and existing
        configs round-trip; invalid / missing values snap back to the
        first defined profile so a stale name in YAML can't crash the
        bootstrap.
        """
        name = self.active_db_profile or "default"
        if name not in self.db_profiles and self.db_profiles:
            name = next(iter(self.db_profiles.keys()))
            self.active_db_profile = name
        if name in self.db_profiles:
            self.db = self.db_profiles[name]

    def set_active_db_profile(self, name: str) -> None:
        """Set the internal default-fallback DB profile pointer.

        Studio no longer exposes "Activate" anywhere; the only public
        caller for this method is the CLI's ``/use-db <name>`` (which
        documents itself as "set the default fallback profile") and
        the add/remove reconcile path in this module. Equivalent to
        ``set_active_db_profiles([name])`` -- kept as a thin shim so
        every existing call site (``cmd_use``,
        ``_maybe_modify_profiles_before_run``, etc.) keeps speaking
        the single-name idiom.
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
        # Drop the deleted DB from every doc/code link list so /ask scope
        # resolution doesn't reference a phantom profile.
        for prof, dbs in list(self.doc_profile_linked_dbs.items()):
            self.doc_profile_linked_dbs[prof] = [d for d in dbs if d != name]
        for prof, dbs in list(self.code_profile_linked_dbs.items()):
            self.code_profile_linked_dbs[prof] = [d for d in dbs if d != name]
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
        self.doc_profile_linked_dbs.pop(name, None)
        if self.active_doc_profile == name:
            self.active_doc_profile = next(iter(self.doc_profiles.keys()), "")
        self._autosave()

    def set_doc_profile_linked_dbs(self, name: str, db_profiles: list[str]) -> None:
        """Associate a doc profile with one or more DB profiles. Empty
        list = global (the doc profile is in scope for every /ask). Caller
        gets a clean error if either side doesn't exist — UI and CLI both
        rely on the exception to surface the typo instead of silently
        writing a ghost link that the load-time pruner would just drop.
        """
        if name not in self.doc_profiles:
            raise KeyError(f"Unknown document profile: {name}")
        cleaned: list[str] = []
        for raw in db_profiles or []:
            db = (raw or "").strip()
            if not db:
                continue
            if db not in self.db_profiles:
                raise KeyError(f"Unknown DB profile: {db}")
            if db not in cleaned:
                cleaned.append(db)
        if cleaned:
            self.doc_profile_linked_dbs[name] = cleaned
        else:
            self.doc_profile_linked_dbs.pop(name, None)
        self._autosave()

    def upsert_code_profile(self, name: str, path: str) -> None:
        self.code_profiles[name] = path
        self._autosave()

    def remove_code_profile(self, name: str) -> None:
        if name not in self.code_profiles:
            raise KeyError(f"Unknown codebase profile: {name}")
        del self.code_profiles[name]
        self.code_profile_linked_dbs.pop(name, None)
        if self.active_code_profile == name:
            self.active_code_profile = next(iter(self.code_profiles.keys()), "")
        self._autosave()

    def set_code_profile_linked_dbs(self, name: str, db_profiles: list[str]) -> None:
        """Symmetric with :meth:`set_doc_profile_linked_dbs`."""
        if name not in self.code_profiles:
            raise KeyError(f"Unknown codebase profile: {name}")
        cleaned: list[str] = []
        for raw in db_profiles or []:
            db = (raw or "").strip()
            if not db:
                continue
            if db not in self.db_profiles:
                raise KeyError(f"Unknown DB profile: {db}")
            if db not in cleaned:
                cleaned.append(db)
        if cleaned:
            self.code_profile_linked_dbs[name] = cleaned
        else:
            self.code_profile_linked_dbs.pop(name, None)
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

    def record_doc_profile_ingest(self, profile_name: str, *, error: str | None = None) -> None:
        """Stamp the doc profile's health telemetry after an ingest run.

        Always updates ``last_ingested_at`` so even a failed ingest is
        reflected in the Studio Settings "Last indexed" line — users
        need to see that something happened, not that the profile is
        untouched. ``last_error`` records a one-line reason on
        failure or clears to ``""`` on success.
        """
        name = (profile_name or "").strip()
        if not name:
            return
        import time as _time

        self.doc_profiles_last_ingested_at[name] = _time.time()
        self.doc_profiles_last_error[name] = str(error or "")
        with suppress(Exception):
            self._autosave_nested()

    def effective_run_doc_paths(self) -> list[str]:
        """Doc paths the orchestrator should pass to ``RAGStore`` for ``/run``.

        When ``run_doc_profiles`` is non-empty (multi-profile override
        set by ``/run --doc foo --doc bar`` or the Studio Run dialog),
        return the **union** of every named profile's paths so a single
        run can pull retrieval context from multiple doc collections.
        Empty list → fall back to the single active profile, matching
        the pre-PR-D single-profile behaviour byte-for-byte.
        """
        names = [n for n in (self.run_doc_profiles or []) if n]
        if not names:
            return self.effective_doc_paths()
        seen: set[str] = set()
        out: list[str] = []
        for name in names:
            if name == DISABLED_PROFILE:
                continue
            paths = self.doc_profiles.get(name) or []
            for p in paths:
                if p and p not in seen:
                    seen.add(p)
                    out.append(p)
        return out

    def effective_doc_paths(self, name: str | None = None) -> list[str]:
        if name is not None:
            if name == DISABLED_PROFILE:
                return []
            if name in self.doc_profiles:
                return list(self.doc_profiles[name])
            return []
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

    def effective_code_paths(self, name: str | None = None) -> list[str]:
        """Code paths for a named profile (PR δ) or the active default.

        ``name`` lets the orchestrator's multi-profile fan-out
        (``effective_run_code_paths``) ask for a *specific* profile's
        path without flipping the active pointer. The historical
        no-argument behaviour is unchanged so legacy callers keep
        working byte-for-byte.
        """
        if name is not None:
            if name == DISABLED_PROFILE:
                return []
            if name in self.code_profiles:
                return [self.code_profiles[name]]
            return []
        if self.code_profiles:
            active = self.active_code_profile
            if active == DISABLED_PROFILE:
                return []
            if active and active in self.code_profiles:
                return [self.code_profiles[active]]
            if "default" in self.code_profiles:
                return [self.code_profiles["default"]]
            key = sorted(self.code_profiles.keys())[0]
            return [self.code_profiles[key]]
        return list(self.code_paths)

    def effective_run_code_paths(self) -> list[str]:
        """Code paths the orchestrator should pass to ``query_code_snippets``.

        When ``run_code_profiles`` is non-empty (multi-profile override
        set by ``/run --code foo --code bar`` or the Studio Run dialog
        chip multi-select), return the **union** of every named
        profile's paths so a single run can pull retrieval context from
        multiple code collections. Empty list → fall back to the single
        active profile, matching the pre-PR-δ single-profile behaviour
        byte-for-byte.
        """
        names = [n for n in (self.run_code_profiles or []) if n]
        if not names:
            return self.effective_code_paths()
        seen: set[str] = set()
        out: list[str] = []
        for name in names:
            if name == DISABLED_PROFILE:
                continue
            for p in self.effective_code_paths(name):
                if p and p not in seen:
                    seen.add(p)
                    out.append(p)
        return out

    def record_code_profile_ingest(self, profile_name: str, *, error: str | None = None) -> None:
        """Stamp the code profile's health telemetry after an index run.

        Mirrors :meth:`record_doc_profile_ingest`. Always updates
        ``last_indexed_at`` so even a failed index is reflected in the
        Studio Settings "Last indexed" line — users need to see that
        something happened, not that the profile is untouched.
        ``last_error`` records a one-line reason on failure or clears
        to ``""`` on success.
        """
        name = (profile_name or "").strip()
        if not name:
            return
        import time as _time

        self.code_profile_last_indexed_at[name] = _time.time()
        self.code_profile_last_error[name] = str(error or "")
        with suppress(Exception):
            self._autosave_nested()

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
