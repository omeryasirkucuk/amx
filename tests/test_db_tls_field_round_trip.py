"""Per-backend TLS field round-trip: set, save, reload, URL.

Each backend's new TLS fields must:
* exist on :class:`DBConfig`,
* survive a YAML save/load cycle, and
* emit the expected token in :attr:`DBConfig.url`.

This is the symmetry test the Databricks-TLS bug exposed: PR #303 added
the fields to the dataclass and the form, but no test asserted that a
round-trip produced a connect-able URL. Keeping this guard for every
new TLS field stops the same drift from recurring.
"""

from __future__ import annotations

import unittest
from dataclasses import asdict
from pathlib import Path

from amx.config import AMXConfig, DBConfig


def _round_trip_yaml(profile: DBConfig, tmp_dir: Path) -> DBConfig:
    """Save *profile* under a fresh AMXConfig + reload from disk."""
    cfg = AMXConfig()
    cfg.CONFIG_DIR = str(tmp_dir)  # type: ignore[misc]
    cfg.db_profiles["test"] = profile
    cfg.save(path=str(tmp_dir / "config.yml"))
    fresh = AMXConfig.load(path=str(tmp_dir / "config.yml"))
    return fresh.db_profiles["test"]


class TlsFieldRoundTripTests(unittest.TestCase):
    def test_postgresql_sslmode_and_root_cert(self) -> None:
        profile = DBConfig(
            backend="postgresql",
            host="pg.example",
            port=5432,
            user="app",
            password="pw",
            database="prod",
            sslmode="verify-full",
            sslrootcert="/etc/ssl/private-ca.pem",
        )
        # URL tokens
        self.assertIn("sslmode=verify-full", profile.url)
        self.assertIn("sslrootcert", profile.url)
        # Round-trip
        loaded = _round_trip_yaml(profile, _make_tmp(self))
        self.assertEqual(loaded.sslmode, "verify-full")
        self.assertEqual(loaded.sslrootcert, "/etc/ssl/private-ca.pem")

    def test_mysql_ssl_ca_and_disabled(self) -> None:
        profile = DBConfig(
            backend="mysql",
            host="mysql.example",
            port=3306,
            user="app",
            password="pw",
            database="prod",
            ssl_ca="/etc/mysql/ca.pem",
        )
        self.assertIn("ssl_ca=", profile.url)
        loaded = _round_trip_yaml(profile, _make_tmp(self))
        self.assertEqual(loaded.ssl_ca, "/etc/mysql/ca.pem")

        opted_out = DBConfig(
            backend="mysql",
            host="mysql.example",
            port=3306,
            user="app",
            password="pw",
            database="prod",
            ssl_disabled=True,
        )
        self.assertIn("ssl_disabled=true", opted_out.url)

    def test_clickhouse_ca_cert_and_verify(self) -> None:
        profile = DBConfig(
            backend="clickhouse",
            host="ch.example",
            port=8443,
            user="default",
            password="",
            database="default",
            secure=True,
            ca_cert="/etc/ch/ca.pem",
verify=True,
        )
        self.assertIn("ca_cert=", profile.url)
        self.assertIn("verify=false", profile.url)
        # When secure is False the TLS knobs must NOT leak into the URL
        # — they're only meaningful on HTTPS.
        plain = DBConfig(
            backend="clickhouse",
            host="ch.example",
            port=8123,
            user="default",
            password="",
            database="default",
            secure=False,
            ca_cert="/etc/ch/ca.pem",
verify=True,
        )
        self.assertNotIn("ca_cert", plain.url)
        self.assertNotIn("verify", plain.url)

    def test_snowflake_insecure_mode_and_ocsp(self) -> None:
        profile = DBConfig(
            backend="snowflake",
            account="acct",
            user="snu",
            password="pw",
            database="DW",
            insecure_mode=True,
            ocsp_fail_open=True,
        )
        self.assertIn("insecure_mode=true", profile.url)
        self.assertIn("ocsp_fail_open=true", profile.url)
        loaded = _round_trip_yaml(profile, _make_tmp(self))
        self.assertTrue(loaded.insecure_mode)
        self.assertTrue(loaded.ocsp_fail_open)

    def test_mssql_studio_now_exposes_encrypt_and_trust(self) -> None:
        # The Studio backends catalog must now list ``encrypt`` and
        # ``trust_server_certificate`` as fields (they were already on
        # DBConfig and in the URL, but the Studio form hid them — the
        # exact pre-#303 Databricks-TLS pattern reproduced for MSSQL).
        from amx.web.routers.profiles import _DB_BACKENDS

        mssql = next(b for b in _DB_BACKENDS if b["id"] == "mssql")
        self.assertIn("encrypt", mssql["fields"])
        self.assertIn("trust_server_certificate", mssql["fields"])
        # And the spec metadata flags them as advanced so they collapse
        # under the Studio form's "Advanced" section.
        spec_groups = {f["name"]: f["group"] for f in mssql["field_specs"]}
        self.assertEqual(spec_groups["encrypt"], "advanced")
        self.assertEqual(spec_groups["trust_server_certificate"], "advanced")


def _make_tmp(case: unittest.TestCase) -> Path:
    import tempfile

    d = Path(tempfile.mkdtemp())
    case.addCleanup(lambda: _rm_tree(d))
    return d


def _rm_tree(p: Path) -> None:
    import shutil

    shutil.rmtree(p, ignore_errors=True)


# Sanity: every newly added TLS field has a default that lets a fresh
# DBConfig() construct without errors.
class TlsFieldDefaultsTests(unittest.TestCase):
    def test_defaults_are_safe(self) -> None:
        cfg = DBConfig()
        for name in (
            "sslmode",
            "sslrootcert",
            "ssl_disabled",
            "ssl_ca",
            "ca_cert",
            "verify",
            "insecure_mode",
            "ocsp_fail_open",
        ):
            self.assertIn(name, asdict(cfg))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
