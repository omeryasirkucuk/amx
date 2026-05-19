"""``/api/live/*`` endpoints expose the wizard-driven scope envelope.

The wizard captures different scope fields per backend:

* Databricks: ``catalog`` (top) + ``database`` (= schema, third level)
* BigQuery:   ``project`` (= catalog) + ``dataset`` (= schema)
* 2-level:    ``database``

The SPA needs ONE consistent envelope per backend so the sidebar +
RunNew can filter their renders uniformly. ``_active_scope_for_profile``
in ``amx/web/routers/live_db.py`` returns that envelope; these tests
pin the per-backend wiring so a future adapter never silently leaks
the wrong field into the wrong slot.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.config import DBConfig
from amx.web.routers import live_db

PROFILE = "test-profile"


@pytest.fixture()
def stub_db(monkeypatch, cfg):
    instance = MagicMock()
    instance.supports_catalogs.return_value = False
    instance.list_catalogs.return_value = ["main", "archive"]
    instance.list_databases.return_value = ["prod", "staging"]
    instance.list_schemas.return_value = ["sales", "marketing"]
    monkeypatch.setattr(live_db, "DatabaseConnector", lambda *_a, **_kw: instance)
    return instance


@pytest.fixture(autouse=True)
def _wipe_connector_cache():
    live_db._CONNECTOR_CACHE.clear()
    yield
    live_db._CONNECTOR_CACHE.clear()


def test_databricks_pins_catalog_and_schema(cfg, stub_db, client, auth_headers):
    """Databricks ``cfg.database`` is the SCHEMA pin (wizard prompt
    literally reads "Schema / database (optional)"). It must land in
    ``active_schema``, NEVER in ``active_database`` — that would lie
    about the field's role for the 2-level backends below."""
    cfg.db_profiles[PROFILE] = DBConfig(
        backend="databricks",
        host="db.test",
        access_token="token",
        catalog="main",
        database="sales",
        http_path="/sql/1.0/warehouses/abc",
    )
    stub_db.supports_catalogs.return_value = True

    r = client.get(f"/api/live/catalogs?profile={PROFILE}", headers=auth_headers)
    assert r.status_code == 200
    j = r.json()
    assert j["active_catalog"] == "main"
    assert j["active_project"] is None

    r = client.get(
        f"/api/live/schemas?profile={PROFILE}&catalog=main",
        headers=auth_headers,
    )
    assert r.status_code == 200
    j = r.json()
    assert j["active_schema"] == "sales"
    assert j["active_dataset"] is None


def test_bigquery_pins_project_and_dataset(cfg, stub_db, client, auth_headers):
    """BigQuery uses ``project`` / ``dataset`` instead of catalog /
    database. The envelope's BigQuery-specific slots carry them."""
    cfg.db_profiles[PROFILE] = DBConfig(
        backend="bigquery",
        project="my-gcp-proj",
        dataset="analytics",
    )
    stub_db.supports_catalogs.return_value = True

    r = client.get(f"/api/live/catalogs?profile={PROFILE}", headers=auth_headers)
    assert r.status_code == 200
    j = r.json()
    assert j["active_project"] == "my-gcp-proj"
    assert j["active_catalog"] is None

    r = client.get(
        f"/api/live/schemas?profile={PROFILE}",
        headers=auth_headers,
    )
    assert r.status_code == 200
    j = r.json()
    assert j["active_dataset"] == "analytics"
    assert j["active_schema"] is None


@pytest.mark.parametrize(
    "backend",
    # 2-level backends only — Trino is 3-level (catalog/schema/table)
    # and is exercised separately by the catalog-scope tests. Hive joins
    # here because its ``database`` field IS the schema in HiveQL.
    ["postgresql", "snowflake", "mysql", "oracle", "mssql", "redshift", "clickhouse", "hive"],
)
def test_two_level_backends_only_pin_database(backend: str, cfg, stub_db, client, auth_headers):
    """For every 2-level backend the wizard captures only ``database``.
    Neither ``active_schema`` nor ``active_dataset`` should ever
    surface a value here — those are schema-pin concepts that only
    apply to Databricks / BigQuery respectively."""
    cfg.db_profiles[PROFILE] = DBConfig(
        backend=backend,
        host="h",
        user="amx",
        database="warehouse",
    )

    r = client.get(f"/api/live/databases?profile={PROFILE}", headers=auth_headers)
    assert r.status_code == 200
    j = r.json()
    assert j["active_database"] == "warehouse"

    r = client.get(
        f"/api/live/schemas?profile={PROFILE}",
        headers=auth_headers,
    )
    assert r.status_code == 200
    j = r.json()
    assert j["active_schema"] is None
    assert j["active_dataset"] is None


def test_unpinned_profile_returns_all_nones(cfg, stub_db, client, auth_headers):
    """When the user left every scope field blank the envelope is
    fully empty — every surface should then show the connector's full
    enumeration. Rule: optional is optional."""
    cfg.db_profiles[PROFILE] = DBConfig(backend="postgresql", host="h", user="amx")

    r = client.get(f"/api/live/databases?profile={PROFILE}", headers=auth_headers)
    j = r.json()
    assert j["active_database"] is None

    r = client.get(
        f"/api/live/schemas?profile={PROFILE}",
        headers=auth_headers,
    )
    j = r.json()
    assert j["active_schema"] is None
    assert j["active_dataset"] is None
