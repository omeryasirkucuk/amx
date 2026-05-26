"""/lineage fetch — CLI wiring, backend gate, and a mocked happy path."""

from __future__ import annotations

from pathlib import Path

import click
from click.testing import CliRunner

from amx.config import AMXConfig, DBConfig
from amx.storage.sqlite_store import SQLiteHistoryStore


def _make_root(cfg: AMXConfig):
    from amx.cli_support.commands.lineage import register_lineage_commands

    pass_config = click.make_pass_decorator(AMXConfig, ensure=False)

    @click.group()
    @click.pass_context
    def root(ctx):
        ctx.obj = cfg

    register_lineage_commands(root, pass_config=pass_config, log_event=lambda **_: None)
    return root


def _invoke(cfg: AMXConfig, args):
    runner = CliRunner()
    return runner.invoke(_make_root(cfg), args, catch_exceptions=False, env={"COLUMNS": "240"})


def test_lineage_fetch_help_resolves():
    cfg = AMXConfig()
    result = _invoke(cfg, ["lineage", "fetch", "--help"])
    assert result.exit_code == 0, result.output
    assert "--with-columns" in result.output
    assert "--profile" in result.output


def test_lineage_fetch_rejects_unsupported_backend(tmp_path: Path, monkeypatch):
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    monkeypatch.setattr("amx.cli_support.commands.lineage.history_store", lambda: hs)

    cfg = AMXConfig()
    cfg.db_profiles = {"pg": DBConfig(backend="postgresql", database="")}
    cfg.active_db_profile = "pg"

    result = _invoke(cfg, ["lineage", "fetch", "public.orders", "--profile", "pg"])
    assert result.exit_code == 0, result.output
    assert "not available for backend" in result.output


def test_lineage_fetch_happy_path_with_mocked_provider(tmp_path: Path, monkeypatch):
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    # Seed the anchor table so the picker / materializer can resolve it.
    with hs._connect() as conn:
        conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, entity_kind, asset_kind) "
            "VALUES ('dbr','databricks','workspace','new_schema','dummy_table','table','table')",
        )
    monkeypatch.setattr("amx.cli_support.commands.lineage.history_store", lambda: hs)

    from amx.lineage.native import provider as P

    class _StubProvider:
        backend = "databricks"

        def fetch_table_lineage(self, fqn, *, with_columns, anchor_columns=()):
            r = P.NativeLineageResult(
                anchor=P.NativeLineageNode(kind=P.TABLE, name="dummy_table", fqn=fqn)
            )
            r.edges.append(
                P.NativeLineageEdge(
                    source=P.NativeLineageNode(kind=P.NOTEBOOK, name="ETL", external_id="n1"),
                    target=r.anchor,
                    direction=P.UPSTREAM,
                )
            )
            return r

    monkeypatch.setattr(P, "provider_for_profile", lambda profile, backend: _StubProvider())

    cfg = AMXConfig()
    cfg.db_profiles = {"dbr": DBConfig(backend="databricks", database="workspace")}
    cfg.active_db_profile = "dbr"

    result = _invoke(
        cfg, ["lineage", "fetch", "workspace.new_schema.dummy_table", "--profile", "dbr"]
    )
    assert result.exit_code == 0, result.output
    assert "Fetched native lineage" in result.output
