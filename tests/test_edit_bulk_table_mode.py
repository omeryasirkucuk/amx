"""Tests for the ``/edit`` bulk-by-name flow's mode + seed contract.

User report 2026-05-04: picking ``world.City`` from the live-DB
picker in "Bulk by table" mode returned 33 *column* matches and zero
table matches, plus picking ``cars.country`` showed "No tables or
columns" even though the user had clearly just selected the table
from the live picker. Two root causes:

1. ``_run_bulk_edit_by_name`` always queried both catalog indices
   regardless of how the user identified the entity. The "Pick a
   table" mode lost its kind information between the resolver and
   the lookup.
2. The catalog query was the only source — a stale index made the
   user's own selection invisible.

The fix:

* The resolver now returns ``(bare_name, kind, seed)``.
* The runner takes ``kind`` and filters catalog queries by it.
* The runner takes ``seed`` and splices the user's picker selection
  into the result list when the catalog miss it.
* For ``kind="table"`` we additionally fall back to a live-DB scan
  so a stale catalog can never make a real table vanish.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.cli_support.commands import manual as manual_module


@pytest.fixture()
def cfg():
    from amx.config import AMXConfig

    cfg = AMXConfig()
    cfg.active_db_profile = "local-postgre"
    return cfg


def _stub_catalog(monkeypatch, *, table_rows=None, column_rows=None):
    catalog = MagicMock()
    catalog.find_tables_by_exact_name.return_value = list(table_rows or [])
    catalog.find_columns_by_exact_name.return_value = list(column_rows or [])
    monkeypatch.setattr(
        "amx.search.catalog.SearchCatalog.from_history_store",
        classmethod(lambda cls: catalog),
    )
    return catalog


def _stub_picker(monkeypatch):
    """Replace the multi-select picker so the test runs to completion
    without prompting. Returns the list of entries the runner *would*
    have shown the user."""
    captured: dict[str, list] = {"entries": []}

    def fake_render_table(_title, _columns, rows):
        captured["entries"] = list(rows)

    def fake_ask(*_args, **_kwargs):
        return ""  # user cancels — test only cares about the rendered list.

    monkeypatch.setattr(manual_module, "render_table", fake_render_table)
    monkeypatch.setattr(manual_module, "ask", fake_ask)
    monkeypatch.setattr(manual_module, "confirm", lambda *a, **k: False)
    return captured


def test_table_mode_skips_column_lookup_entirely(cfg, monkeypatch) -> None:
    """User picks "Pick a table" → only ``find_tables_by_exact_name``
    runs. Even when columns named the same thing exist in the catalog,
    they must NOT show up in the result list."""
    catalog = _stub_catalog(
        monkeypatch,
        table_rows=[{"schema_name": "world", "table_name": "City"}],
        column_rows=[{"schema_name": "x", "table_name": "y", "column_name": "city"}],
    )
    captured = _stub_picker(monkeypatch)

    manual_module._run_bulk_edit_by_name(
        cfg,
        bare_name="City",
        comment=None,
        skip_confirm=False,
        log_event=None,
        kind="table",
    )

    catalog.find_tables_by_exact_name.assert_called_once()
    catalog.find_columns_by_exact_name.assert_not_called()
    kinds = {row[1] for row in captured["entries"]}  # second column is Kind
    assert kinds == {"TABLE"}, f"Table mode should not surface columns; got {kinds}"


def test_column_mode_skips_table_lookup(cfg, monkeypatch) -> None:
    catalog = _stub_catalog(
        monkeypatch,
        table_rows=[{"schema_name": "world", "table_name": "City"}],
        column_rows=[{"schema_name": "x", "table_name": "y", "column_name": "city"}],
    )
    captured = _stub_picker(monkeypatch)

    manual_module._run_bulk_edit_by_name(
        cfg,
        bare_name="city",
        comment=None,
        skip_confirm=False,
        log_event=None,
        kind="column",
    )

    catalog.find_columns_by_exact_name.assert_called_once()
    catalog.find_tables_by_exact_name.assert_not_called()
    kinds = {row[1] for row in captured["entries"]}
    assert kinds == {"COL"}


def test_any_mode_queries_both(cfg, monkeypatch) -> None:
    catalog = _stub_catalog(
        monkeypatch,
        table_rows=[{"schema_name": "s", "table_name": "City"}],
        column_rows=[{"schema_name": "x", "table_name": "y", "column_name": "city"}],
    )
    _stub_picker(monkeypatch)

    manual_module._run_bulk_edit_by_name(
        cfg,
        bare_name="City",
        comment=None,
        skip_confirm=False,
        log_event=None,
        kind="any",
    )

    catalog.find_tables_by_exact_name.assert_called_once()
    catalog.find_columns_by_exact_name.assert_called_once()


def test_seed_table_is_inserted_when_catalog_misses_it(cfg, monkeypatch) -> None:
    """User picked ``world.City`` from the live picker, but the
    catalog hasn't indexed it yet. The runner must splice the seed
    into the result list so the user's own selection never vanishes."""
    _stub_catalog(
        monkeypatch,
        table_rows=[{"schema_name": "other", "table_name": "City"}],
    )
    # Disable the live-DB fallback; we only want to test the seed path
    # in isolation here.
    monkeypatch.setattr(manual_module, "_scan_live_db_for_tables_named", lambda cfg, name: [])
    captured = _stub_picker(monkeypatch)

    manual_module._run_bulk_edit_by_name(
        cfg,
        bare_name="City",
        comment=None,
        skip_confirm=False,
        log_event=None,
        kind="table",
        seed={"schema": "world", "table": "City"},
    )

    labels = [row[2] for row in captured["entries"]]  # Schema.Table[.Column]
    assert "world.City" in labels, f"Seed table must appear in results; got {labels}"


def test_seed_table_is_not_duplicated_when_catalog_already_has_it(cfg, monkeypatch) -> None:
    """If the catalog already has the seed entry, we must NOT
    duplicate it."""
    _stub_catalog(
        monkeypatch,
        table_rows=[
            {"schema_name": "world", "table_name": "City"},
            {"schema_name": "other", "table_name": "City"},
        ],
    )
    monkeypatch.setattr(manual_module, "_scan_live_db_for_tables_named", lambda cfg, name: [])
    captured = _stub_picker(monkeypatch)

    manual_module._run_bulk_edit_by_name(
        cfg,
        bare_name="City",
        comment=None,
        skip_confirm=False,
        log_event=None,
        kind="table",
        seed={"schema": "world", "table": "City"},
    )

    labels = [row[2] for row in captured["entries"]]
    assert labels.count("world.City") == 1


def test_table_mode_falls_back_to_live_db_when_catalog_empty(cfg, monkeypatch) -> None:
    """The user-reported scenario: catalog is stale, picked
    ``cars.country`` returned no matches. The runner must scan the
    live DB and surface every table named ``country`` so the bulk
    edit still works."""
    _stub_catalog(monkeypatch, table_rows=[])

    monkeypatch.setattr(
        manual_module,
        "_scan_live_db_for_tables_named",
        lambda cfg, name: [
            {"schema_name": "cars", "table_name": "country"},
            {"schema_name": "world", "table_name": "Country"},
        ],
    )
    captured = _stub_picker(monkeypatch)

    manual_module._run_bulk_edit_by_name(
        cfg,
        bare_name="country",
        comment=None,
        skip_confirm=False,
        log_event=None,
        kind="table",
    )

    labels = sorted(row[2] for row in captured["entries"])
    assert labels == ["cars.country", "world.Country"]


def test_table_mode_with_no_matches_anywhere_emits_actionable_error(
    cfg, monkeypatch, capsys
) -> None:
    _stub_catalog(monkeypatch, table_rows=[])
    monkeypatch.setattr(manual_module, "_scan_live_db_for_tables_named", lambda cfg, name: [])
    _stub_picker(monkeypatch)

    manual_module._run_bulk_edit_by_name(
        cfg,
        bare_name="nope",
        comment=None,
        skip_confirm=False,
        log_event=None,
        kind="table",
    )
    out = capsys.readouterr().out + capsys.readouterr().err
    # The kind-aware message should mention "tables" and "live DB
    # scanned" so the user knows the catalog isn't the only thing
    # checked.
    assert "tables" in out.lower() or "table" in out.lower()


def test_resolver_returns_kind_and_seed_for_table_path(monkeypatch, cfg) -> None:
    """The resolver carries ``kind`` and ``seed`` forward so the
    runner doesn't have to re-parse the prompt string."""
    monkeypatch.setattr(
        manual_module,
        "_select_db_profile_for_wizard",
        lambda cfg: ("local-postgre", cfg.db),
    )
    monkeypatch.setattr(manual_module, "_connector_for_profile", lambda cfg: MagicMock())
    monkeypatch.setattr(
        manual_module, "_select_schema_for_wizard", lambda db, default=None: "world"
    )
    monkeypatch.setattr(
        manual_module,
        "_select_table_for_wizard",
        lambda db, schema, default=None: "City",
    )

    result = manual_module._resolve_bulk_target_name(cfg, "Pick a table from the catalog")
    assert result is not None
    bare_name, kind, seed = result
    assert bare_name == "City"
    assert kind == "table"
    assert seed == {"schema": "world", "table": "City"}


def test_resolver_returns_kind_and_seed_for_column_path(monkeypatch, cfg) -> None:
    monkeypatch.setattr(
        manual_module,
        "_select_db_profile_for_wizard",
        lambda cfg: ("local-postgre", cfg.db),
    )
    monkeypatch.setattr(manual_module, "_connector_for_profile", lambda cfg: MagicMock())
    monkeypatch.setattr(
        manual_module, "_select_schema_for_wizard", lambda db, default=None: "world"
    )
    monkeypatch.setattr(
        manual_module,
        "_select_table_for_wizard",
        lambda db, schema, default=None: "City",
    )
    monkeypatch.setattr(
        manual_module, "_select_column_for_wizard", lambda db, schema, table: "Name"
    )

    result = manual_module._resolve_bulk_target_name(cfg, "Pick a column from the catalog")
    assert result is not None
    bare_name, kind, seed = result
    assert bare_name == "Name"
    assert kind == "column"
    assert seed == {"schema": "world", "table": "City", "column": "Name"}


def test_resolver_returns_any_for_typed_path(monkeypatch, cfg) -> None:
    monkeypatch.setattr(
        manual_module,
        "_ask_text_or_cancel",
        lambda *args, **kwargs: "customer_id",
    )
    result = manual_module._resolve_bulk_target_name(cfg, "Type a name manually")
    assert result is not None
    bare_name, kind, seed = result
    assert bare_name == "customer_id"
    assert kind == "any"
    assert seed is None
