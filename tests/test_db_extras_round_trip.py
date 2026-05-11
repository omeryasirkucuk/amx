"""PR 4 — DuckDB / BigQuery extras + shared-history banner.

Each field added in this PR must:

* exist on :class:`DBConfig`,
* round-trip through YAML save/load,
* surface in :attr:`DBConfig.url` where applicable,
* appear in :func:`profile_schema.spec_for` so Studio renders it.

Plus a sanity check that the Studio backends endpoint flags
DuckDB / ClickHouse as ``supports_shared_history=False``.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from amx.config import AMXConfig, DBConfig
from amx.db.profile_schema import spec_for


def _round_trip(profile: DBConfig) -> DBConfig:
    tmp = Path(tempfile.mkdtemp())
    cfg = AMXConfig()
    cfg.db_profiles["test"] = profile
    cfg.save(path=str(tmp / "config.yml"))
    fresh = AMXConfig.load(path=str(tmp / "config.yml"))
    return fresh.db_profiles["test"]


class DuckdbExtrasTests(unittest.TestCase):
    def test_read_only_round_trip(self) -> None:
        profile = DBConfig(backend="duckdb", database="/tmp/x.duckdb", read_only=True)
        loaded = _round_trip(profile)
        self.assertTrue(loaded.read_only)
        self.assertIn("read_only=true", profile.url)

    def test_motherduck_token_round_trip(self) -> None:
        profile = DBConfig(backend="duckdb", database="md:warehouse", motherduck_token="tok-42")
        loaded = _round_trip(profile)
        self.assertEqual(loaded.motherduck_token, "tok-42")
        self.assertIn("motherduck_token=tok-42", profile.url)

    def test_token_ignored_for_local_path(self) -> None:
        # When ``database`` is a local file the token has no business in
        # the URL; the only path that should embed it is ``md:`` /
        # ``md``. Otherwise the user could accidentally splash their PAT
        # into a log line that doesn't expect it.
        profile = DBConfig(
            backend="duckdb",
            database="/tmp/local.duckdb",
            motherduck_token="should-not-leak",
        )
        self.assertNotIn("should-not-leak", profile.url)

    def test_spec_lists_extras_as_advanced(self) -> None:
        names = {s.name: s.group for s in spec_for("duckdb")}
        self.assertEqual(names.get("read_only"), "advanced")
        self.assertEqual(names.get("motherduck_token"), "advanced")


class BigqueryExtrasTests(unittest.TestCase):
    def test_location_round_trip(self) -> None:
        profile = DBConfig(backend="bigquery", project="p", location="EU")
        loaded = _round_trip(profile)
        self.assertEqual(loaded.location, "EU")
        self.assertIn("location=EU", profile.url)

    def test_impersonate_round_trip(self) -> None:
        profile = DBConfig(
            backend="bigquery",
            project="p",
            impersonate_service_account="svc@p.iam.gserviceaccount.com",
        )
        loaded = _round_trip(profile)
        self.assertEqual(
            loaded.impersonate_service_account,
            "svc@p.iam.gserviceaccount.com",
        )
        # The URL builder URL-encodes the ``@`` so we match on the
        # encoded form.
        self.assertIn("impersonate_service_account=", profile.url)
        self.assertIn("svc%40p.iam.gserviceaccount.com", profile.url)


class SharedHistoryBackendsApiTests(unittest.TestCase):
    def test_duckdb_and_clickhouse_flag_false(self) -> None:
        from amx.web.routers.profiles import _DB_BACKENDS

        by_id = {b["id"]: b for b in _DB_BACKENDS}
        self.assertFalse(by_id["duckdb"]["supports_shared_history"])
        self.assertFalse(by_id["clickhouse"]["supports_shared_history"])

    def test_supporting_backends_flag_true(self) -> None:
        from amx.web.routers.profiles import _DB_BACKENDS

        by_id = {b["id"]: b for b in _DB_BACKENDS}
        for backend in (
            "postgresql",
            "mysql",
            "snowflake",
            "databricks",
            "bigquery",
            "oracle",
            "mssql",
            "redshift",
        ):
            self.assertTrue(
                by_id[backend]["supports_shared_history"],
                f"{backend} should support shared history",
            )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
