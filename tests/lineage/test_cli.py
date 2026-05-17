"""CLI coverage by invoking the registered Click command callbacks directly.

We bypass Click's ``CliRunner`` / ``pass_config`` plumbing so the tests
stay focused on lineage behaviour rather than wiring. Each subcommand's
``.callback`` is a plain function whose first parameter is the
``AMXConfig`` — call it directly and verify side-effects on the seeded
history store.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from amx.cli import main as cli_main
from amx.config import AMXConfig, DBConfig
from amx.lineage import store as lineage_store
from amx.storage.sqlite_store import SQLiteHistoryStore

from .conftest import (
    seed_column_comments_cache_for_table,
    seed_foreign_key_relationship,
    seed_table_entity,
)


@pytest.fixture
def cli_env(tmp_path):
    """Set up an AMXConfig with one DuckDB profile and a seeded history store."""
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()

    profile_cfg = DBConfig(backend="duckdb", database="", host="localhost", port=0)
    cfg = AMXConfig()
    cfg.db_profiles = {"local": profile_cfg}
    cfg.active_db_profile = "local"

    orders_id = seed_table_entity(hs, profile="local", schema="public", table="orders")
    customers_id = seed_table_entity(hs, profile="local", schema="public", table="customers")
    seed_foreign_key_relationship(
        hs,
        from_table_id=orders_id,
        to_table_id=customers_id,
        constrained_columns=["customer_id"],
        referred_columns=["id"],
        referred_table="customers",
    )
    seed_column_comments_cache_for_table(
        hs,
        profile="local",
        schema="public",
        table="orders",
        columns={"customer_id": {"type": "integer"}},
    )
    seed_column_comments_cache_for_table(
        hs,
        profile="local",
        schema="public",
        table="customers",
        columns={"id": {"type": "integer"}},
    )

    return cfg, hs


def _invoke(name: str, cfg: AMXConfig, /, **kwargs):
    """Invoke a /lineage subcommand callback under a synthetic Click context.

    The commands are wrapped with ``@pass_config`` (a Click
    ``make_pass_decorator``) which pulls ``AMXConfig`` from the current
    Click context. Tests that call the callback directly therefore need
    a real ``click.Context`` set up with ``obj=cfg``.
    """
    import click

    cmd = cli_main.commands["lineage"].commands[name]
    with click.Context(cmd, obj=cfg) as ctx:
        return ctx.invoke(cmd, **kwargs)


def test_cli_create_cache_only_writes_artifact(cli_env, tmp_path):
    cfg, hs = cli_env
    out_path = tmp_path / "lineage.svg"

    with (
        patch("amx.cli_support.commands.lineage.history_store", return_value=hs),
        patch("amx.lineage.service.render_lineage_image", return_value=out_path),
    ):
        _invoke(
            "create",
            cfg,
            anchor="public.orders",
            column=None,
            out=str(out_path),
            fmt="svg",
            depth_up=None,
            depth_down=None,
            name="orders-cli",
            profile_flag=None,
            no_cache=False,
            cache_only=True,
            prefetch=False,
            force=False,
        )

    artifacts = lineage_store.list_lineage_artifacts(hs)
    assert len(artifacts) == 1
    assert artifacts[0]["name"] == "orders-cli"
    # cache-only with no view-DDL cache => partial render expected.
    assert artifacts[0]["extractors_partial"] is True


def test_cli_cache_only_never_calls_connector_factory(cli_env, tmp_path):
    cfg, hs = cli_env
    out_path = tmp_path / "lineage.svg"
    factory_calls: list[str] = []

    def boom_factory(profile):
        factory_calls.append(profile)
        raise AssertionError("cache-only must not open a connector")

    with (
        patch("amx.cli_support.commands.lineage.history_store", return_value=hs),
        patch(
            "amx.cli_support.commands.lineage._build_connector_factory", return_value=boom_factory
        ),
        patch("amx.lineage.service.render_lineage_image", return_value=out_path),
    ):
        _invoke(
            "create",
            cfg,
            anchor="public.orders",
            column=None,
            out=str(out_path),
            fmt="svg",
            depth_up=None,
            depth_down=None,
            name="no-net",
            profile_flag=None,
            no_cache=False,
            cache_only=True,
            prefetch=False,
            force=False,
        )
    assert factory_calls == []


def test_cli_list_shows_seeded_artifact(cli_env, capsys):
    cfg, hs = cli_env
    lineage_store.insert_lineage_artifact(
        hs,
        name="x",
        db_profile="local",
        anchor_entity_id=1,
        depth_up=1,
        depth_down=1,
        fmt="svg",
        output_path="/tmp/x.svg",
        edge_set_hash="h",
        node_count=2,
        edge_count=1,
        extractors_used=["fk"],
        extractors_partial=False,
    )
    with patch("amx.cli_support.commands.lineage.history_store", return_value=hs):
        _invoke("list", cfg, profile_flag=None)
    out = capsys.readouterr().out
    assert "x" in out


def test_cli_delete_removes_artifact_and_file(cli_env, tmp_path):
    cfg, hs = cli_env
    out_path = tmp_path / "lineage.svg"
    out_path.write_text("<svg/>")
    lineage_store.insert_lineage_artifact(
        hs,
        name="bye",
        db_profile="local",
        anchor_entity_id=1,
        depth_up=1,
        depth_down=1,
        fmt="svg",
        output_path=str(out_path),
        edge_set_hash="h",
        node_count=1,
        edge_count=0,
        extractors_used=["fk"],
        extractors_partial=False,
    )
    with patch("amx.cli_support.commands.lineage.history_store", return_value=hs):
        _invoke("delete", cfg, name_or_id="bye", yes=True)
    assert lineage_store.lookup_lineage_artifact(hs, name_or_id="bye") is None
    assert not out_path.exists()


def test_cli_show_renders_text_tree(cli_env, capsys):
    cfg, hs = cli_env
    with patch("amx.cli_support.commands.lineage.history_store", return_value=hs):
        _invoke(
            "show",
            cfg,
            anchor="public.orders",
            column=None,
            depth_up=1,
            depth_down=1,
            profile_flag=None,
        )
    out = capsys.readouterr().out
    assert "public.orders" in out


def test_cli_open_reports_missing_file(cli_env, tmp_path, capsys):
    cfg, hs = cli_env
    out_path = tmp_path / "lineage.svg"  # NOT created — should trigger warning
    lineage_store.insert_lineage_artifact(
        hs,
        name="ghost",
        db_profile="local",
        anchor_entity_id=1,
        depth_up=1,
        depth_down=1,
        fmt="svg",
        output_path=str(out_path),
        edge_set_hash="h",
        node_count=1,
        edge_count=0,
        extractors_used=["fk"],
        extractors_partial=False,
    )
    with patch("amx.cli_support.commands.lineage.history_store", return_value=hs):
        _invoke("open", cfg, name_or_id="ghost")
    out = capsys.readouterr().out
    assert "File missing" in out
