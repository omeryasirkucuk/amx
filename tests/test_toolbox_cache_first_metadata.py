"""Cache-first metadata lookup for /ask agent tools.

The user reported that the /ask agent paid for a live ``profile_table``
on every describe_table and a cross-DB sweep on every find_table_by_name
even when AMX already had the answer in either ``catalog_entities``
(from /search sync) or ``run_context_cache`` (from a prior /run-apply
or a recent /ask write-back).

This module pins the three-layer resolver:

1. ``catalog_entities`` — persistent, populated by /search sync. Read
   via ``SearchCatalog.fetch_table_metadata``. No TTL; the response
   carries ``age_seconds`` so the LLM can hedge.
2. ``run_context_cache`` — 24h TTL, populated by /run-apply OR by a
   prior /ask write-back. Same key shape, so /run-apply primes /ask
   and vice-versa.
3. Live ``db.profile_table`` — the expensive fallback. On success we
   write back to ``run_context_cache`` so the next call is free.

``force_fresh=True`` bypasses both caches and forces a live read.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from amx.search.agent_tools import ToolBox


def _bare_toolbox(*, db_database: str = "", db_profile: str = "p"):
    cfg_db = SimpleNamespace(database=db_database, catalog="", backend="postgresql", project="")
    cfg = SimpleNamespace(
        db=cfg_db,
        db_profiles={db_profile: cfg_db},
        doc_profiles={},
        code_profiles={},
        doc_profile_linked_dbs={},
        code_profile_linked_dbs={},
    )
    catalog = MagicMock()
    # By default the catalog returns nothing — individual tests
    # override ``catalog.fetch_table_metadata`` to seed hits.
    catalog.fetch_table_metadata.return_value = None
    catalog.find_tables_by_exact_name.return_value = []
    catalog._connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []
    catalog._connect.return_value.__exit__.return_value = False
    catalog.fetch_distinct_schemas.return_value = []
    catalog.fetch_distinct_tables_in_schema.return_value = []

    tb = ToolBox.__new__(ToolBox)
    tb.cfg = cfg
    tb.catalog = catalog
    tb.db_profile = db_profile
    tb.db_profiles = [db_profile]
    tb._connectors = {}
    tb._owned_connectors = set()
    tb._db = None
    tb._db_factory = lambda: MagicMock()
    # These bare ToolBox fixtures exercise the cache-miss -> live
    # fallback path that PR #501 gated behind ``allow_live_refresh``.
    # Opt the bare fixture into live mode so the existing assertions
    # (`source == "live"`, write-back triggers, etc.) keep firing.
    tb._allow_live_refresh = True
    return tb


class _NullCtx:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_describe_table_serves_from_catalog_when_synced(monkeypatch):
    """Regression: when /search sync has covered (profile, schema, table),
    ``describe_table`` returns the catalog snapshot WITHOUT calling
    ``db.profile_table``. Response carries ``source="catalog"`` and
    ``age_seconds`` so the LLM can hedge."""
    tb = _bare_toolbox()
    last_synced = 1_700_000_000.0
    tb.catalog.fetch_table_metadata.return_value = {
        "table_comment": "billing header",
        "row_count": 1234,
        "last_synced_at": last_synced,
        "columns": [
            {"name": "vbeln", "dtype": "varchar", "nullable": False, "comment": "doc"},
            {"name": "fkdat", "dtype": "date", "nullable": True, "comment": "date"},
            {"name": "waerk", "dtype": "char", "nullable": True, "comment": ""},
        ],
    }

    # Profile_table must never be called when the catalog has the data.
    def _boom(*_a, **_kw):
        raise AssertionError("profile_table called despite catalog hit")

    fake_db = MagicMock()
    fake_db.profile_table.side_effect = _boom
    fake_db.cfg = SimpleNamespace(database="", catalog="", project="")
    tb._db = fake_db
    monkeypatch.setattr(tb, "_resolve_catalog_or_autopick", lambda *_a, **_kw: ("", [], []))
    monkeypatch.setattr(tb, "_scoped_catalog", lambda *_a, **_kw: _NullCtx())
    # Make _now deterministic for age computation.
    monkeypatch.setattr(tb, "_now", lambda: last_synced + 86400.0)

    out = tb._tool_describe_table(schema="sap_s6p", table="vbrk")

    assert out["found"] is True
    assert out["source"] == "catalog"
    assert abs(out["age_seconds"] - 86400.0) < 1.0
    assert out["column_count"] == 3
    assert out["row_count"] == 1234
    assert "last_synced_at" in out  # ISO timestamp
    fake_db.profile_table.assert_not_called()


def test_describe_table_writes_back_on_live_miss(monkeypatch):
    """When neither catalog nor 24h cache has the table, a live
    ``profile_table`` runs and the result is written back to the 24h
    cache so the NEXT call is free."""
    tb = _bare_toolbox()
    # Stub the catalog AND the history-store cache to miss.
    tb.catalog.fetch_table_metadata.return_value = None

    saved: dict = {}
    lookup_state: dict = {"hits": 0}

    class _Store:
        def lookup_run_context_cache(self, *, db_profile, database, schema, table):
            lookup_state["hits"] += 1
            payload = saved.get((db_profile, database, schema, table))
            if not payload:
                return None
            return {"payload": payload, "source_run_id": None, "created_at": 0.0}

        def save_run_context_cache(
            self, *, db_profile, database, schema, table, payload, source_run_id, ttl_seconds
        ):
            saved[(db_profile, database, schema, table)] = payload

    monkeypatch.setattr(tb, "_history_store", lambda: _Store())

    fake_profile = SimpleNamespace(
        columns=[
            SimpleNamespace(name="c1", dtype="int", nullable=False, existing_comment=""),
            SimpleNamespace(name="c2", dtype="varchar", nullable=True, existing_comment="x"),
            SimpleNamespace(name="c3", dtype="date", nullable=True, existing_comment=""),
        ],
        row_count=42,
        existing_comment="t-comment",
        analytics=None,
    )
    call_count = {"n": 0}

    def _profile_table(_schema, _table, sample_size=0):
        call_count["n"] += 1
        return fake_profile

    fake_db = MagicMock()
    fake_db.profile_table.side_effect = _profile_table
    fake_db.cfg = SimpleNamespace(database="warehouse", catalog="", project="")
    tb._db = fake_db
    monkeypatch.setattr(tb, "_databases_to_sweep", lambda: [None])
    monkeypatch.setattr(tb, "_connector_for_database", lambda _d: fake_db)
    monkeypatch.setattr(tb, "_resolve_catalog_or_autopick", lambda *_a, **_kw: ("", [], []))
    monkeypatch.setattr(tb, "_scoped_catalog", lambda *_a, **_kw: _NullCtx())
    monkeypatch.setattr(tb, "_now", lambda: 1_700_000_000.0)

    first = tb._tool_describe_table(schema="sales", table="orders")
    assert first["source"] == "live"
    assert call_count["n"] == 1
    # Write-back ran.
    assert ("p", "warehouse", "sales", "orders") in saved

    second = tb._tool_describe_table(schema="sales", table="orders")
    assert second["source"] == "live_cache"
    # No second live round-trip.
    assert call_count["n"] == 1


def test_describe_table_force_fresh_bypasses_caches(monkeypatch):
    """``force_fresh=True`` skips both the catalog and the 24h cache
    and forces a fresh live ``profile_table`` even when a strong
    catalog hit exists."""
    tb = _bare_toolbox()
    tb.catalog.fetch_table_metadata.return_value = {
        "table_comment": "stale",
        "row_count": 0,
        "last_synced_at": 1.0,
        "columns": [
            {"name": "c", "dtype": "int", "nullable": False, "comment": ""},
        ],
    }

    saved: dict = {}

    class _Store:
        def lookup_run_context_cache(self, **_kw):
            return None

        def save_run_context_cache(
            self, *, db_profile, database, schema, table, payload, source_run_id, ttl_seconds
        ):
            saved[(db_profile, database, schema, table)] = payload

    monkeypatch.setattr(tb, "_history_store", lambda: _Store())

    fake_profile = SimpleNamespace(
        columns=[
            SimpleNamespace(name="fresh_col", dtype="text", nullable=True, existing_comment="")
        ],
        row_count=999,
        existing_comment="fresh",
        analytics=None,
    )
    fake_db = MagicMock()
    fake_db.profile_table.return_value = fake_profile
    fake_db.cfg = SimpleNamespace(database="w", catalog="", project="")
    tb._db = fake_db
    monkeypatch.setattr(tb, "_databases_to_sweep", lambda: [None])
    monkeypatch.setattr(tb, "_connector_for_database", lambda _d: fake_db)
    monkeypatch.setattr(tb, "_resolve_catalog_or_autopick", lambda *_a, **_kw: ("", [], []))
    monkeypatch.setattr(tb, "_scoped_catalog", lambda *_a, **_kw: _NullCtx())
    monkeypatch.setattr(tb, "_now", lambda: 1_700_000_000.0)

    out = tb._tool_describe_table(schema="s", table="t", force_fresh=True)

    assert out["source"] == "live"
    assert out["row_count"] == 999
    fake_db.profile_table.assert_called_once()
    # Write-back still runs so the next non-force call is cheap.
    assert ("p", "w", "s", "t") in saved


def test_find_table_by_name_writes_discovery_rows_for_describe_table_reuse(monkeypatch):
    """After a successful live sweep, ``find_table_by_name`` writes a
    discovery row into the 24h cache so a subsequent ``describe_table``
    can resolve the database without paying for the sweep again."""
    tb = _bare_toolbox()
    # Empty catalog — force live sweep.
    tb.catalog.find_tables_by_exact_name.return_value = []

    public = MagicMock()
    public.cfg = SimpleNamespace(database="public", catalog="", project="")
    public.list_databases.return_value = ["public", "SAP"]
    public.list_schemas.return_value = ["public"]
    public.list_assets.side_effect = lambda sch: []
    public.list_assets_bulk = None
    public.supports_catalogs.return_value = False

    sap = MagicMock()
    sap.cfg = SimpleNamespace(database="SAP", catalog="", project="")
    sap.list_schemas.return_value = ["sap_s6p"]
    sap.list_assets.side_effect = lambda sch: [("vbrk", "table")] if sch == "sap_s6p" else []
    sap.list_assets_bulk = None
    sap.supports_catalogs.return_value = False

    def _connector_for_database(name):
        if not name or name == "public":
            return public
        if name == "SAP":
            return sap
        raise AssertionError(f"unexpected db: {name}")

    tb._db = public
    # Direct attribute assignment — monkeypatch.setattr proxies through
    # the test's cleanup hook which seems to behave inconsistently for
    # instance attributes on classes without __slots__.
    tb._connector_for_database = _connector_for_database  # type: ignore[method-assign]

    saved: dict = {}

    class _Store:
        def lookup_run_context_cache(self, **_kw):
            return None

        def save_run_context_cache(
            self, *, db_profile, database, schema, table, payload, source_run_id, ttl_seconds
        ):
            saved[(db_profile, database, schema, table)] = payload

    monkeypatch.setattr(tb, "_history_store", lambda: _Store())

    out = tb._tool_find_table_by_name("vbrk")

    assert "sap_s6p.vbrk" in out["matches"]
    assert out["resolved_databases"].get("sap_s6p.vbrk") == "SAP"
    # A discovery row was persisted for the matched (profile, db, schema, table).
    assert ("p", "SAP", "sap_s6p", "vbrk") in saved
    assert saved[("p", "SAP", "sap_s6p", "vbrk")].get("kind") == "discovery"


def test_databases_to_sweep_memoises_per_profile(monkeypatch):
    """The cross-DB fanout's ``list_databases`` call is cached for 24h
    so a second /ask question doesn't pay the live round-trip again."""
    tb = _bare_toolbox(db_database="")

    cache_state: dict = {}

    class _Store:
        def lookup_run_context_cache(self, *, db_profile, database, schema, table):
            payload = cache_state.get((db_profile, database, schema, table))
            if payload is None:
                return None
            return {"payload": payload, "source_run_id": None, "created_at": 0.0}

        def save_run_context_cache(
            self, *, db_profile, database, schema, table, payload, source_run_id, ttl_seconds
        ):
            cache_state[(db_profile, database, schema, table)] = payload

    monkeypatch.setattr(tb, "_history_store", lambda: _Store())

    live_calls = {"n": 0}

    fake_db = MagicMock()
    fake_db.supports_catalogs.return_value = False

    def _list_databases():
        live_calls["n"] += 1
        return ["one", "two"]

    fake_db.list_databases.side_effect = _list_databases
    tb._db = fake_db

    first = tb._databases_to_sweep()
    second = tb._databases_to_sweep()

    assert first == ["one", "two"]
    assert second == ["one", "two"]
    # ``list_databases`` was called exactly once across the two
    # invocations — the second read served from the 24h cache slot.
    assert live_calls["n"] == 1


def test_describe_table_zero_column_catalog_hit_not_reported_as_empty(monkeypatch):
    """Regression: a catalog entry that carries the TABLE but ZERO
    columns (column entities never synced) must NOT be reported as an
    empty table.

    Before the fix, ``describe_table`` trusted the zero-column cache
    payload and returned ``column_count=0``, leading the assistant to
    tell the user their populated table was an empty stub. In
    cache-only mode the tool now returns ``columns_not_cached=True``
    with a hint that explicitly forbids claiming emptiness.
    """
    tb = _bare_toolbox()
    tb._allow_live_refresh = False  # cache-only mode — the /ask default

    # Resolver returns a payload (table exists) but with NO columns.
    monkeypatch.setattr(
        tb,
        "_resolve_table_metadata",
        lambda **_kw: ({"columns": [], "table_comment": "", "row_count": 0}, "catalog", 100.0),
    )
    # profile_table must never run in cache-only mode.
    fake_db = MagicMock()
    fake_db.profile_table.side_effect = AssertionError("live probe in cache-only mode")
    fake_db.cfg = SimpleNamespace(database="", catalog="", project="")
    tb._db = fake_db

    out = tb._tool_describe_table(schema="airline", table="Airports")

    assert out["columns_not_cached"] is True
    # The table itself is known to exist — only its columns are missing.
    assert out["found"] is True
    assert out["cache_only"] is True
    # The hint must steer the assistant away from claiming emptiness.
    hint = out["hint"].lower()
    assert "not cached" in hint
    assert "empty" in hint  # "do NOT tell the user the table is empty"
    fake_db.profile_table.assert_not_called()


def test_describe_table_zero_column_catalog_hit_falls_through_to_live(monkeypatch):
    """When live refresh is allowed, a zero-column catalog hit is
    demoted to a miss so ``describe_table`` probes the live DB and
    returns the real columns instead of trusting the empty catalog
    entry.
    """
    tb = _bare_toolbox()  # fixture sets _allow_live_refresh = True

    monkeypatch.setattr(
        tb,
        "_resolve_table_metadata",
        lambda **_kw: ({"columns": [], "table_comment": "", "row_count": 0}, "catalog", 100.0),
    )

    fake_profile = SimpleNamespace(
        columns=[
            SimpleNamespace(name="Code", dtype="varchar", nullable=False, existing_comment=""),
            SimpleNamespace(
                name="Description", dtype="varchar", nullable=True, existing_comment=""
            ),
        ],
        row_count=100,
        existing_comment="",
        analytics=None,
    )
    fake_db = MagicMock()
    fake_db.profile_table.return_value = fake_profile
    fake_db.cfg = SimpleNamespace(database="", catalog="", project="")
    tb._db = fake_db

    class _Store:
        def lookup_run_context_cache(self, **_kw):
            return None

        def save_run_context_cache(self, **_kw):
            return None

    monkeypatch.setattr(tb, "_history_store", lambda: _Store())
    monkeypatch.setattr(tb, "_databases_to_sweep", lambda: [None])
    monkeypatch.setattr(tb, "_connector_for_database", lambda _d: fake_db)
    monkeypatch.setattr(tb, "_resolve_catalog_or_autopick", lambda *_a, **_kw: ("", [], []))
    monkeypatch.setattr(tb, "_scoped_catalog", lambda *_a, **_kw: _NullCtx())
    monkeypatch.setattr(tb, "_now", lambda: 1_700_000_000.0)

    out = tb._tool_describe_table(schema="airline", table="Airports")

    assert out["source"] == "live"
    assert out["column_count"] == 2
    fake_db.profile_table.assert_called()


def test_describe_table_cached_zero_row_count_reported_as_unknown(monkeypatch):
    """Regression: /search sync never captures row counts (every
    catalog row stores row_count=0), so a cached row_count of 0 means
    "unknown", NOT "empty table". describe_table must surface it as
    ``row_count=None`` + ``row_count_known=False`` so the assistant
    doesn't tell the user a populated table has zero rows.
    """
    tb = _bare_toolbox()
    tb._allow_live_refresh = False  # cache-only mode — the /ask default

    # Catalog hit WITH columns (so we stay on the cache branch) but
    # row_count 0 — exactly what /search sync produces.
    monkeypatch.setattr(
        tb,
        "_resolve_table_metadata",
        lambda **_kw: (
            {
                "columns": [
                    {"name": "id", "dtype": "int", "nullable": False, "comment": "pk"},
                    {"name": "name", "dtype": "varchar", "nullable": True, "comment": ""},
                ],
                "table_comment": "customer rows",
                "row_count": 0,
            },
            "catalog",
            100.0,
        ),
    )
    fake_db = MagicMock()
    fake_db.cfg = SimpleNamespace(database="", catalog="", project="")
    tb._db = fake_db

    out = tb._tool_describe_table(schema="beer_factory", table="customers")

    # The table + its columns ARE known; only the row count is not.
    assert out["found"] is True
    assert out["column_count"] == 2
    assert out["row_count_known"] is False
    assert out["row_count"] is None
    assert out["stats"]["row_count"] is None
    assert out["stats"]["row_count_known"] is False


def test_describe_table_cached_positive_row_count_is_trusted(monkeypatch):
    """A cached row_count > 0 is real data worth surfacing — it must
    stay an int and be marked known, so the guard above doesn't blank
    out legitimately-captured counts."""
    tb = _bare_toolbox()
    tb._allow_live_refresh = False

    monkeypatch.setattr(
        tb,
        "_resolve_table_metadata",
        lambda **_kw: (
            {
                "columns": [{"name": "id", "dtype": "int", "nullable": False, "comment": ""}],
                "table_comment": "",
                "row_count": 4242,
            },
            "catalog",
            100.0,
        ),
    )
    fake_db = MagicMock()
    fake_db.cfg = SimpleNamespace(database="", catalog="", project="")
    tb._db = fake_db

    out = tb._tool_describe_table(schema="s", table="t")

    assert out["row_count"] == 4242
    assert out["row_count_known"] is True


def test_list_schemas_serves_from_catalog_when_synced(monkeypatch):
    """``list_schemas`` reads from ``catalog_entities`` first; only
    falls back to a live ``db.list_schemas`` call when the catalog is
    empty for this profile."""
    tb = _bare_toolbox()
    tb.catalog.fetch_distinct_schemas.return_value = [
        {"name": "sap_s6p", "last_synced_at": 1_699_000_000.0},
        {"name": "public", "last_synced_at": 1_700_000_000.0},
    ]
    fake_db = MagicMock()

    def _boom(*_a, **_kw):
        raise AssertionError("db.list_schemas called despite catalog hit")

    fake_db.list_schemas.side_effect = _boom
    fake_db.cfg = SimpleNamespace(database="w", catalog="", project="")
    tb._db = fake_db
    monkeypatch.setattr(tb, "_now", lambda: 1_700_000_500.0)

    out = tb._tool_list_schemas()

    assert out["source"] == "catalog"
    assert out["count"] == 2
    assert "sap_s6p" in out["schemas"]
    fake_db.list_schemas.assert_not_called()
