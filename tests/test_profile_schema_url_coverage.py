"""Profile schema URL-coverage regression net.

For every backend, every field marked ``applies_to_url=True`` in
:mod:`amx.db.profile_schema` MUST be consumed by
:meth:`amx.config.DBConfig.url`. The test sets each field to a
recognisable sentinel and asserts the sentinel (or its booleanised
encoding) shows up in the rendered URL.

Because the URL builder uses conditional branches (Oracle picks
``service_name`` over ``database`` SID; ClickHouse only emits TLS
fields when ``secure=True``; MySQL picks ``ssl_disabled`` over
``ssl_ca``), the test runs each backend through one or more scenarios
that together cover the full spec'd surface area.

This is the regression that would have caught the Databricks-TLS gap
before PR #303 ever shipped: ``tls_no_verify`` / ``tls_trusted_ca_file``
were on DBConfig but never flowed into the URL — the user only
discovered the silent drop when the connect attempt blew up on
``SSLCertVerificationError``. Since AMX's TLS handling for those two
fields lives in the adapter (``DatabricksAdapter.create_engine``)
rather than the URL string, this test does NOT cover them; the spec
marks them ``applies_to_url=True`` because the connector layer reads
them. For URL-encoded TLS fields (PG sslmode, MySQL ssl_ca, ClickHouse
ca_cert / verify, Snowflake insecure_mode / ocsp_fail_open) the test
asserts the URL string contains the field name as a query param.
"""

from __future__ import annotations

import unittest

from amx.config import DBConfig
from amx.db.profile_schema import spec_for, supported_backends

# Fields the connector reads but that do NOT round-trip through the URL
# string (they are passed to the driver via create_engine kwargs). The
# coverage check skips them.
_NOT_IN_URL: dict[str, set[str]] = {
    "databricks": {"tls_no_verify", "tls_trusted_ca_file"},
    # Trino's TLS bundle path travels via ``connect_args`` (verify=<path>)
    # on the SQLAlchemy engine, not via a URL query param — same
    # rationale as Databricks. ``jwt_token`` similarly travels via
    # ``connect_args["auth"]``.
    "trino": {"tls_trusted_ca_file", "jwt_token"},
}


# Per-backend scenarios. Each entry is a (label, kwargs, expected_substrings)
# tuple. Together the scenarios for one backend must cover every
# applies_to_url field declared in its spec.
_SCENARIOS: dict[str, list[tuple[str, dict[str, object], list[str]]]] = {
    "postgresql": [
        (
            "with-tls",
            {
                "host": "pgh",
                "port": 5601,
                "user": "pgu",
                "password": "pgp",
                "database": "pgdb",
                "sslmode": "verify-full",
                "sslrootcert": "/etc/ssl/ca.pem",
            },
            ["pgh", "5601", "pgu", "pgdb", "sslmode", "verify-full", "sslrootcert", "ca.pem"],
        ),
    ],
    "mysql": [
        (
            "with-ssl-ca",
            {
                "host": "myh",
                "port": 3307,
                "user": "myu",
                "password": "myp",
                "database": "mydb",
                "ssl_disabled": False,
                "ssl_ca": "/etc/ca.pem",
            },
            ["myh", "3307", "myu", "mydb", "ssl_ca"],
        ),
        (
            "ssl-disabled",
            {
                "host": "myh",
                "port": 3307,
                "user": "myu",
                "password": "myp",
                "database": "mydb",
                "ssl_disabled": True,
            },
            ["ssl_disabled"],
        ),
    ],
    "snowflake": [
        (
            "with-tls",
            {
                "account": "acct",
                "user": "snu",
                "password": "snp",
                "database": "db",
                "warehouse": "wh",
                "role": "rl",
                "insecure_mode": True,
                "ocsp_fail_open": True,
            },
            [
                "acct",
                "snu",
                "db",
                "warehouse",
                "wh",
                "role",
                "rl",
                "insecure_mode",
                "ocsp_fail_open",
            ],
        ),
    ],
    "databricks": [
        (
            "uc-catalog",
            {
                "host": "dbx-host",
                "http_path": "/sql/x",
                "access_token": "t",
                "catalog": "main",
                "database": "schemax",
            },
            # ``http_path`` is URL-encoded as ``%2Fsql%2Fx`` so check
            # the percent-encoded form, not the raw slash form.
            ["dbx-host", "http_path", "%2Fsql%2Fx", "catalog", "main", "schemax"],
        ),
    ],
    "bigquery": [
        (
            "with-creds",
            {
                "project": "pj",
                "dataset": "ds",
                "credentials_path": "/run/sa.json",
                "location": "EU",
                "impersonate_service_account": "svc@pj.iam",
            },
            [
                "pj",
                "ds",
                "credentials_path",
                "sa.json",
                "location=EU",
                "impersonate_service_account",
                "svc%40pj.iam",
            ],
        ),
    ],
    "oracle": [
        (
            "service-name",
            {
                "host": "orh",
                "port": 1522,
                "user": "oru",
                "password": "orp",
                "service_name": "XEPDB1",
            },
            ["orh", "1522", "oru", "service_name", "XEPDB1"],
        ),
        (
            "sid",
            {"host": "orh", "port": 1522, "user": "oru", "password": "orp", "database": "XE"},
            ["XE"],
        ),
    ],
    "mssql": [
        (
            "encrypt-yes-trust-yes",
            {
                "host": "mssh",
                "port": 1434,
                "user": "msu",
                "password": "msp",
                "database": "msdb",
                "driver": "ODBC Driver 18 for SQL Server",
                "encrypt": True,
                "trust_server_certificate": True,
            },
            ["mssh", "1434", "msu", "msdb", "driver", "Encrypt=yes", "TrustServerCertificate=yes"],
        ),
        (
            "encrypt-no",
            {
                "host": "mssh",
                "port": 1434,
                "user": "msu",
                "password": "msp",
                "database": "msdb",
                "driver": "ODBC Driver 18 for SQL Server",
                "encrypt": False,
            },
            ["Encrypt=no"],
        ),
    ],
    "redshift": [
        (
            "iam-cluster",
            {
                "host": "rsh",
                "port": 5440,
                "user": "rsu",
                "password": "rsp",
                "database": "rsdb",
                "cluster_identifier": "my-cluster",
            },
            ["rsh", "5440", "rsu", "rsdb", "cluster_identifier", "my-cluster"],
        ),
    ],
    "clickhouse": [
        (
            "https-with-ca",
            {
                "host": "chh",
                "port": 8444,
                "user": "chu",
                "password": "chp",
                "database": "chdb",
                "secure": True,
                "ca_cert": "/etc/ch.pem",
                "verify": False,
            },
            ["chh", "8444", "chu", "chdb", "clickhouse+https", "ca_cert", "verify=false"],
        ),
        (
            "http-plain",
            {
                "host": "chh",
                "port": 8123,
                "user": "chu",
                "password": "chp",
                "database": "chdb",
                "secure": False,
            },
            ["clickhouse+http"],
        ),
    ],
    "duckdb": [
        (
            "file-path-readonly",
            {"database": "/tmp/local.duckdb", "read_only": True},
            ["/tmp/local.duckdb", "read_only=true"],
        ),
        (
            "in-memory",
            {"database": ""},
            [":memory:"],
        ),
        (
            "motherduck",
            {"database": "md:warehouse", "motherduck_token": "tok-1"},
            ["md:warehouse", "motherduck_token=tok-1"],
        ),
    ],
    "trino": [
        (
            "https-basic-with-catalog",
            {
                "host": "trino.example.com",
                "port": 443,
                "user": "alice",
                "password": "secret",
                "catalog": "hive",
                "database": "default",
                "http_scheme": "https",
                "verify": False,
            },
            [
                "trino.example.com",
                "443",
                "alice",
                "hive",
                "default",
                "http_scheme=https",
                "verify=false",
            ],
        ),
        (
            "http-no-catalog",
            {
                "host": "localhost",
                "port": 8080,
                "user": "amx",
                "password": "",
                "http_scheme": "http",
            },
            ["localhost", "8080", "http_scheme=http"],
        ),
    ],
    "hive": [
        (
            "plain-auth-with-db",
            {
                "host": "hive.example.com",
                "port": 10000,
                "user": "alice",
                "password": "secret",
                "database": "warehouse",
                "auth_mode": "PLAIN",
            },
            ["hive.example.com", "10000", "alice", "warehouse", "auth=PLAIN"],
        ),
    ],
}


class ProfileSchemaUrlCoverageTests(unittest.TestCase):
    def test_scenarios_cover_every_url_field(self) -> None:
        """Each scenario's per-field value must appear in the rendered URL.

        Combined with the per-backend coverage check below this is what
        would have caught the Databricks-TLS class of drift: any spec
        field tagged ``applies_to_url=True`` that the URL builder
        silently drops makes one of the scenarios fail.
        """
        misses: list[str] = []
        # Track which fields each backend's scenarios actually exercise —
        # the second loop asserts the union covers the whole spec.
        exercised: dict[str, set[str]] = {}

        for backend in supported_backends():
            scenarios = _SCENARIOS.get(backend, [])
            self.assertTrue(
                scenarios,
                f"No coverage scenarios defined for backend {backend!r}.",
            )
            exercised[backend] = set()
            for label, kwargs, expectations in scenarios:
                cfg = DBConfig(backend=backend, **kwargs)  # type: ignore[arg-type]
                url = cfg.url
                for expected in expectations:
                    if expected not in url:
                        misses.append(f"{backend}/{label}: expected {expected!r} in URL {url!r}")
                exercised[backend].update(kwargs.keys())

        # Every applies_to_url spec field must be touched by at least
        # one scenario's kwargs — adding a new spec field WITHOUT a
        # matching scenario fails the test, forcing the test author to
        # prove the URL builder reads it.
        for backend in supported_backends():
            for s in spec_for(backend):
                if not s.applies_to_url:
                    continue
                if s.name in _NOT_IN_URL.get(backend, set()):
                    continue
                if s.name not in exercised.get(backend, set()):
                    misses.append(
                        f"{backend}.{s.name}: spec field is not exercised by "
                        f"any URL coverage scenario — add a scenario in "
                        f"_SCENARIOS that sets it and asserts the value in "
                        f"the rendered URL."
                    )

        self.assertFalse(
            misses,
            "URL builder coverage gaps:\n  " + "\n  ".join(misses),
        )

    def test_supported_backends_match_dbconfig_legal_values(self) -> None:
        for backend in supported_backends():
            cfg = DBConfig(backend=backend)
            self.assertEqual(cfg.backend, backend)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
