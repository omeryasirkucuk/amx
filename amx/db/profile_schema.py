"""Single source of truth for which fields each backend exposes.

Before this module the per-backend field list lived in three places —
:class:`amx.config.DBConfig` (dataclass), the ``_DB_BACKENDS`` catalog in
``amx.web.routers.profiles`` (Studio form), and the hard-coded prompt
blocks in ``amx.cli_support.commands.db`` (CLI wizard). Whenever one
copy drifted, fields that the URL builder *did* read were silently
hidden from one of the surfaces — that is exactly how the Databricks
TLS bug (PR #303) survived a release: ``DBConfig.url`` honoured
``tls_no_verify`` / ``tls_trusted_ca_file`` but Studio never offered
them.

Each spec entry tags fields with their UI kind, whether they're
secrets, whether they belong in the "advanced" collapse, and crucially
whether they are read by :meth:`DBConfig.url`. The unit test
``tests/test_profile_schema_url_coverage.py`` enforces the last bit:
every ``applies_to_url=True`` field must appear in the URL string for
at least one realistic configuration of its backend. That test would
have caught the Databricks-TLS gap before #303 ever shipped.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

FieldKind = Literal["text", "password", "int", "bool", "select"]
FieldGroup = Literal["basic", "advanced"]


@dataclass(frozen=True)
class FieldSpec:
    """One field on a backend's profile form.

    Attributes mirror what the Studio form needs to render the input,
    what the CLI wizard needs to prompt for it, and what the validation
    layer needs to keep the three call sites in sync.
    """

    name: str
    """``DBConfig`` attribute name. Must exist on the dataclass."""

    kind: FieldKind = "text"
    """How Studio renders the input. CLI maps to ask / ask_password / etc."""

    label: str = ""
    """Human-readable label for the form / wizard prompt."""

    help: str = ""
    """Short explanation shown alongside the field (Studio tooltip / wizard
    parenthetical)."""

    secret: bool = False
    """When True the value is masked on read and the placeholder is
    accepted as ``no change`` on PUT."""

    required: bool = False
    """When True the wizard re-prompts until the user provides a value."""

    group: FieldGroup = "basic"
    """``advanced`` fields render inside the form's collapse block."""

    applies_to_url: bool = True
    """When True the URL builder must consume this field (defensive
    coverage test). Set False for purely-stored metadata like
    ``profiling_*`` knobs that the connector reads via getattr."""

    options: tuple[str, ...] = ()
    """For ``kind == 'select'`` — list of valid choices."""

    default_visible: bool = True
    """When False the field is hidden from listings (used for backends
    where a field exists on the dataclass for compatibility but never
    surfaced)."""


def _common_host_port(default_port: int) -> list[FieldSpec]:
    return [
        FieldSpec(name="host", kind="text", label="Host", required=True),
        FieldSpec(
            name="port", kind="int", label="Port", required=True, help=f"Default {default_port}"
        ),
        FieldSpec(name="user", kind="text", label="User", required=True),
        FieldSpec(name="password", kind="password", label="Password", required=True, secret=True),
        FieldSpec(
            name="database",
            kind="text",
            label="Database",
            help="Leave blank to pick at command time.",
        ),
    ]


_SCHEMA: dict[str, tuple[FieldSpec, ...]] = {
    "postgresql": (
        *_common_host_port(5432),
        FieldSpec(
            name="sslmode",
            kind="select",
            label="SSL mode",
            help="libpq sslmode. verify-full is the corporate / managed-PG idiom.",
            group="advanced",
            options=("", "disable", "allow", "prefer", "require", "verify-ca", "verify-full"),
        ),
        FieldSpec(
            name="sslrootcert",
            kind="text",
            label="SSL root cert",
            help="Path to a private CA bundle. Required when sslmode is verify-ca / verify-full and the cert is not in the OS trust store.",
            group="advanced",
        ),
    ),
    "mysql": (
        *_common_host_port(3306),
        FieldSpec(
            name="ssl_disabled",
            kind="bool",
            label="Disable TLS",
            help="Opt out of TLS for legacy intra-data-centre setups.",
            group="advanced",
        ),
        FieldSpec(
            name="ssl_ca",
            kind="text",
            label="SSL CA bundle path",
            help="Activates path validation against a private CA. Ignored when 'Disable TLS' is set.",
            group="advanced",
        ),
    ),
    "snowflake": (
        FieldSpec(
            name="account",
            kind="text",
            label="Account",
            required=True,
            help="Snowflake account identifier (e.g. xy12345.eu-central-1).",
        ),
        FieldSpec(name="user", kind="text", label="User", required=True),
        FieldSpec(name="password", kind="password", label="Password", required=True, secret=True),
        FieldSpec(
            name="database",
            kind="text",
            label="Database",
            help="Leave blank to pick at command time.",
        ),
        FieldSpec(name="warehouse", kind="text", label="Warehouse"),
        FieldSpec(name="role", kind="text", label="Role"),
        FieldSpec(
            name="insecure_mode",
            kind="bool",
            label="Insecure mode",
            help="Last-resort TLS bypass (snowflake.connector.connect insecure_mode flag).",
            group="advanced",
        ),
        FieldSpec(
            name="ocsp_fail_open",
            kind="bool",
            label="OCSP fail-open",
            help="Allow connect when the OCSP responder is blocked by a corporate proxy.",
            group="advanced",
        ),
    ),
    "databricks": (
        FieldSpec(
            name="host",
            kind="text",
            label="Workspace host",
            required=True,
            help="adb-XXXXXXXXXXXX.azuredatabricks.net or workspace URL",
        ),
        FieldSpec(name="http_path", kind="text", label="SQL warehouse HTTP path", required=True),
        FieldSpec(
            name="access_token", kind="password", label="Access token", required=True, secret=True
        ),
        FieldSpec(
            name="catalog",
            kind="text",
            label="Unity Catalog",
            required=True,
            help="Use 'hive_metastore' for legacy workspaces.",
        ),
        FieldSpec(name="database", kind="text", label="Schema (optional)"),
        FieldSpec(
            name="tls_trusted_ca_file",
            kind="text",
            label="Trusted CA bundle path",
            help="Corporate / self-signed proxy CA path.",
            group="advanced",
        ),
        FieldSpec(
            name="tls_no_verify",
            kind="bool",
            label="Skip TLS verification",
            help="Pick this OR the CA bundle path based on IT policy.",
            group="advanced",
        ),
    ),
    "bigquery": (
        FieldSpec(name="project", kind="text", label="GCP project", required=True),
        FieldSpec(name="dataset", kind="text", label="Default dataset"),
        FieldSpec(
            name="location",
            kind="text",
            label="Query location",
            help="GCP region (EU / US / europe-west3 / …). Empty = project default.",
        ),
        FieldSpec(
            name="credentials_path",
            kind="text",
            label="Service-account JSON path",
            help="Leave blank to use Application Default Credentials.",
        ),
        FieldSpec(
            name="impersonate_service_account",
            kind="text",
            label="Impersonate service account",
            help="Workload-identity email; signs queries without a personal SA key.",
            group="advanced",
        ),
    ),
    "oracle": (
        FieldSpec(name="host", kind="text", label="Host", required=True),
        FieldSpec(name="port", kind="int", label="Port", required=True, help="Default 1521."),
        FieldSpec(name="user", kind="text", label="User", required=True),
        FieldSpec(name="password", kind="password", label="Password", required=True, secret=True),
        FieldSpec(
            name="database",
            kind="text",
            label="SID (fallback)",
            help="Used when service_name is blank.",
        ),
        FieldSpec(
            name="service_name",
            kind="text",
            label="Service name",
            help="Preferred for Oracle Cloud / RAC.",
        ),
    ),
    "mssql": (
        FieldSpec(name="host", kind="text", label="Host", required=True),
        FieldSpec(name="port", kind="int", label="Port", required=True, help="Default 1433."),
        FieldSpec(name="user", kind="text", label="User", required=True),
        FieldSpec(name="password", kind="password", label="Password", required=True, secret=True),
        FieldSpec(
            name="database",
            kind="text",
            label="Database",
            help="Leave blank to pick at command time.",
        ),
        FieldSpec(
            name="driver",
            kind="text",
            label="ODBC driver",
            help="Defaults to 'ODBC Driver 18 for SQL Server'.",
        ),
        FieldSpec(
            name="encrypt",
            kind="bool",
            label="Encrypt connection",
            help="Driver default; switch off only for legacy unencrypted setups.",
            group="advanced",
        ),
        FieldSpec(
            name="trust_server_certificate",
            kind="bool",
            label="Trust server certificate",
            help="Skip cert chain validation. Required for self-signed dev / corporate servers.",
            group="advanced",
        ),
    ),
    "redshift": (
        *_common_host_port(5439),
        FieldSpec(
            name="cluster_identifier",
            kind="text",
            label="Cluster identifier",
            help="Only required for IAM auth.",
        ),
    ),
    "clickhouse": (
        FieldSpec(name="host", kind="text", label="Host", required=True),
        FieldSpec(name="port", kind="int", label="Port", help="8123 for HTTP / 8443 for HTTPS."),
        FieldSpec(name="user", kind="text", label="User"),
        FieldSpec(name="password", kind="password", label="Password", secret=True),
        FieldSpec(name="database", kind="text", label="Database"),
        FieldSpec(name="secure", kind="bool", label="Use HTTPS"),
        FieldSpec(
            name="ca_cert",
            kind="text",
            label="CA bundle path",
            help="Private CA path (HTTPS only).",
            group="advanced",
        ),
        FieldSpec(
            name="verify",
            kind="bool",
            label="Verify TLS certificate",
            help="Off only for TLS-inspecting proxies that present a non-distributable root.",
            group="advanced",
        ),
    ),
    "duckdb": (
        FieldSpec(
            name="database",
            kind="text",
            label="Database path",
            required=True,
            help="Path to a .duckdb file, ':memory:', or 'md:<db>' for MotherDuck.",
        ),
        FieldSpec(
            name="read_only",
            kind="bool",
            label="Read-only",
            help="Let multiple AMX processes attach the same file. Ignored for ':memory:' / MotherDuck.",
            group="advanced",
        ),
        FieldSpec(
            name="motherduck_token",
            kind="password",
            label="MotherDuck token",
            help="Required when database starts with 'md:'. Stored as a secret.",
            secret=True,
            group="advanced",
        ),
    ),
}


def supported_backends() -> tuple[str, ...]:
    """Backends with a registered spec — exposed to the Studio API and
    the CLI wizard's picker so neither has to hard-code the list."""
    return tuple(_SCHEMA.keys())


def spec_for(backend: str) -> tuple[FieldSpec, ...]:
    """Return the field spec for *backend* or an empty tuple."""
    return _SCHEMA.get(backend, ())


def field_names_for(backend: str) -> list[str]:
    """Backwards-compatible field-name list for Studio's ``_DB_BACKENDS``.

    The Studio wizard's pre-spec API returned ``{"id", "label", "fields"}``
    per backend; surface keeps the ``fields`` array so existing clients
    keep working while richer metadata is layered on via the spec.
    """
    return [f.name for f in _SCHEMA.get(backend, ())]


def applies_to_url_fields(backend: str) -> list[str]:
    """Fields the URL builder is expected to consume for *backend*."""
    return [f.name for f in _SCHEMA.get(backend, ()) if f.applies_to_url]


class ProfileValidationError(ValueError):
    """Raised when a DB profile payload fails the spec's required-field check.

    ``missing`` lists the ``FieldSpec.name`` values that were empty / null.
    Surfaced as a 400 by the Studio upsert route and printed by the CLI
    wizard so the user sees which field needs filling instead of getting
    a downstream runtime error (e.g. Databricks' historical
    ``Catalog 'None' was not found`` SQL crash when the catalog was
    accepted as an empty string at save time).
    """

    def __init__(self, backend: str, missing: list[str]) -> None:
        self.backend = backend
        self.missing = list(missing)
        labels = ", ".join(missing)
        super().__init__(
            f"Profile for backend {backend!r} is missing required field(s): {labels}."
        )


def validate_required_fields(backend: str, payload: dict) -> None:
    """Reject a profile payload when any ``required=True`` field is empty.

    Treats Python ``None``, empty string, and whitespace-only strings as
    missing. Numeric / boolean fields are validated by presence: ``None``
    is missing, ``0`` / ``False`` is acceptable. Unknown backends are a
    no-op so a forwards-compatible Studio build adding a new backend
    won't fail the existing release.
    """
    spec = _SCHEMA.get(backend, ())
    if not spec:
        return
    missing: list[str] = []
    for field in spec:
        if not field.required:
            continue
        value = payload.get(field.name) if isinstance(payload, dict) else None
        if value is None:
            missing.append(field.name)
            continue
        if isinstance(value, str) and not value.strip():
            missing.append(field.name)
    if missing:
        raise ProfileValidationError(backend, missing)
