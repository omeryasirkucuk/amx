import click
from click.testing import CliRunner


def _make_root():
    """Build a minimal Click root that hosts /db assets and /db ingest-assets.

    Bypasses the AMX interactive-mode guard by constructing the group
    directly rather than going through ``amx.cli.main``. Mirrors the
    pattern used in ``tests/cli/test_schedule_cli.py``.
    """
    from amx.cli_support.commands.db_assets import register_db_assets_commands

    pass_config = click.make_pass_decorator(object, ensure=True)

    @click.group()
    @click.pass_context
    def root(ctx):
        if ctx.obj is None:
            # Inject a minimal AMXConfig-like object so pass_config works.
            from amx.config import AMXConfig

            ctx.obj = AMXConfig()

    @root.group()
    def db():
        """Database commands."""

    register_db_assets_commands(db, pass_config=pass_config)
    return root


def _invoke(args):
    """Invoke the minimal Click root with the given args."""
    runner = CliRunner()
    return runner.invoke(_make_root(), args, catch_exceptions=False)


def test_db_assets_help_resolves():
    result = _invoke(["db", "assets", "--help"])
    assert result.exit_code == 0, result.output
    for cmd in ("list", "show", "search", "refresh", "prune"):
        assert cmd in result.output, f"`/db assets {cmd}` missing from help"


def test_db_ingest_assets_help_resolves():
    result = _invoke(["db", "ingest-assets", "--help"])
    assert result.exit_code == 0, result.output
    assert "--types" in result.output
    assert "--profile" in result.output
    assert "--history-days" in result.output
    assert "--runs-per-job" in result.output


def test_ingest_assets_with_flags_skips_wizard(monkeypatch, tmp_path):
    """When --types is provided, the wizard skips the picker and runs the service."""
    from amx.services.ingest_assets import IngestResult

    captured = {}

    def fake_open_connector(cfg, profile_name):
        captured["profile"] = profile_name
        return object()

    def fake_open_catalog(cfg):
        return object()

    def fake_run(self, req, *, progress=None):
        captured["request"] = req
        return IngestResult(counts={"notebooks": 3, "lineage": 5}, failures={})

    import amx.cli_support.commands.db_assets_impl as impl

    monkeypatch.setattr(impl, "_open_connector", fake_open_connector)
    monkeypatch.setattr(impl, "_open_catalog", fake_open_catalog)
    monkeypatch.setattr(impl, "_resolve_profile", lambda cfg, name: "prod")
    monkeypatch.setattr("amx.services.ingest_assets.IngestAssetsService.run", fake_run)

    result = _invoke(
        [
            "db",
            "ingest-assets",
            "--profile",
            "prod",
            "--types",
            "notebooks",
            "--history-days",
            "14",
        ]
    )
    assert result.exit_code == 0, result.output
    assert captured["profile"] == "prod"
    assert captured["request"].types == ["notebooks"]
    assert captured["request"].history_days == 14
    assert "notebooks=3" in result.output


def test_ingest_assets_rejects_unknown_type():
    result = _invoke(
        [
            "db",
            "ingest-assets",
            "--profile",
            "prod",
            "--types",
            "definitely_not_real,notebooks",
        ]
    )
    assert result.exit_code != 0
    assert "Unknown asset type" in result.output or "definitely_not_real" in result.output


# ── Task 33: run_list ─────────────────────────────────────────────────────────


def test_db_assets_list_notebooks_renders_rows(monkeypatch, tmp_path):
    """Seed a remote_notebooks row, invoke /db assets list --type notebooks,
    confirm the row appears in the output."""
    import sqlite3

    from amx.storage.sqlite_store import SQLiteHistoryStore

    db_path = tmp_path / "amx.db"
    store = SQLiteHistoryStore(db_path)
    store.init()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO remote_notebooks
                   (profile_name, platform, external_id, name, workspace_path,
                    qualified_name, language, source_text, source_hash,
                    last_modified_at, last_modified_by, owner, cell_count, ingested_at)
               VALUES ('prod', 'databricks', 'ext-1', 'my_notebook',
                       '/Users/alice/my_notebook', NULL, 'python', '{}', 'h1',
                       NULL, NULL, NULL, 5, '2026-05-21T00:00:00')"""
        )
        conn.commit()

    import amx.cli_support.commands.db_assets_impl as impl

    monkeypatch.setattr(impl, "_resolve_profile", lambda cfg, name: "prod")
    monkeypatch.setattr(impl, "_history_db_path", lambda cfg: db_path, raising=False)

    result = _invoke(["db", "assets", "list", "--profile", "prod", "--type", "notebooks"])
    assert result.exit_code == 0, result.output
    assert "my_notebook" in result.output


def test_db_assets_list_jobs_renders_rows(monkeypatch, tmp_path):
    import sqlite3

    from amx.storage.sqlite_store import SQLiteHistoryStore

    db_path = tmp_path / "amx.db"
    SQLiteHistoryStore(db_path).init()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO remote_jobs
                   (profile_name, job_id, name, schedule_cron, schedule_pause_status,
                    success_rate_30d, ingested_at)
               VALUES ('prod', 42, 'nightly_etl', '0 2 * * *', 'UNPAUSED', 0.95,
                       '2026-05-21T00:00:00')"""
        )
        conn.commit()

    import amx.cli_support.commands.db_assets_impl as impl

    monkeypatch.setattr(impl, "_resolve_profile", lambda cfg, name: "prod")
    monkeypatch.setattr(impl, "_history_db_path", lambda cfg: db_path, raising=False)

    result = _invoke(["db", "assets", "list", "--profile", "prod", "--type", "jobs"])
    assert result.exit_code == 0, result.output
    assert "nightly_etl" in result.output


# ── Task 34: run_show ─────────────────────────────────────────────────────────


def test_db_assets_show_notebook(monkeypatch, tmp_path):
    import json
    import sqlite3

    from amx.storage.sqlite_store import SQLiteHistoryStore

    db_path = tmp_path / "amx.db"
    SQLiteHistoryStore(db_path).init()
    nb_src = json.dumps(
        {
            "cells": [
                {
                    "cell_type": "code",
                    "source": ["print(1)"],
                    "metadata": {"language": "python"},
                    "outputs": [],
                    "execution_count": None,
                }
            ],
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {},
        }
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO remote_notebooks
                   (profile_name, platform, external_id, name, workspace_path,
                    qualified_name, language, source_text, source_hash,
                    last_modified_at, last_modified_by, owner, cell_count, ingested_at)
               VALUES ('prod', 'databricks', 'ext-1', 'mynb',
                       '/n', NULL, 'python', ?, 'h', NULL, NULL, NULL, 1,
                       '2026-05-21')""",
            (nb_src,),
        )
        conn.commit()
        nb_id = conn.execute("SELECT id FROM remote_notebooks").fetchone()[0]

    import amx.cli_support.commands.db_assets_impl as impl

    monkeypatch.setattr(impl, "_resolve_profile", lambda cfg, name: "prod")
    monkeypatch.setattr(impl, "_history_db_path", lambda cfg: db_path, raising=False)

    result = _invoke(
        ["db", "assets", "show", str(nb_id), "--profile", "prod", "--type", "notebooks"]
    )
    assert result.exit_code == 0, result.output
    assert "mynb" in result.output
    assert "print(1)" in result.output


# ── Task 35: run_search ───────────────────────────────────────────────────────


def test_db_assets_search_finds_term_in_notebook_source(monkeypatch, tmp_path):
    import sqlite3

    from amx.storage.sqlite_store import SQLiteHistoryStore

    db_path = tmp_path / "amx.db"
    SQLiteHistoryStore(db_path).init()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO remote_notebooks
                   (profile_name, platform, external_id, name, workspace_path,
                    qualified_name, language, source_text, source_hash,
                    last_modified_at, last_modified_by, owner, cell_count, ingested_at)
               VALUES ('prod', 'databricks', 'ext-1', 'kpi_nb',
                       '/kpi', NULL, 'sql',
                       'SELECT * FROM monthly_revenue WHERE active', 'h',
                       NULL, NULL, NULL, 1, '2026-05-21')"""
        )
        conn.commit()

    import amx.cli_support.commands.db_assets_impl as impl

    monkeypatch.setattr(impl, "_resolve_profile", lambda cfg, name: "prod")
    monkeypatch.setattr(impl, "_history_db_path", lambda cfg: db_path, raising=False)

    result = _invoke(["db", "assets", "search", "monthly_revenue", "--profile", "prod"])
    assert result.exit_code == 0, result.output
    assert "kpi_nb" in result.output


# ── Task 36: run_prune + run_refresh ─────────────────────────────────────────


def test_db_assets_prune_drops_old_rows(monkeypatch, tmp_path):
    import sqlite3

    from amx.storage.sqlite_store import SQLiteHistoryStore

    db_path = tmp_path / "amx.db"
    SQLiteHistoryStore(db_path).init()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO remote_notebooks
                   (profile_name, platform, external_id, name, workspace_path,
                    qualified_name, language, source_text, source_hash,
                    last_modified_at, last_modified_by, owner, cell_count, ingested_at)
               VALUES ('prod', 'databricks', 'ext-old', 'old_nb', '/old', NULL,
                       'python', '{}', 'h', NULL, NULL, NULL, 1,
                       '2020-01-01T00:00:00')"""
        )
        conn.execute(
            """INSERT INTO remote_notebooks
                   (profile_name, platform, external_id, name, workspace_path,
                    qualified_name, language, source_text, source_hash,
                    last_modified_at, last_modified_by, owner, cell_count, ingested_at)
               VALUES ('prod', 'databricks', 'ext-new', 'new_nb', '/new', NULL,
                       'python', '{}', 'h', NULL, NULL, NULL, 1, ?)""",
            (__import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),),
        )
        conn.commit()

    import amx.cli_support.commands.db_assets_impl as impl

    monkeypatch.setattr(impl, "_resolve_profile", lambda cfg, name: "prod")
    monkeypatch.setattr(impl, "_history_db_path", lambda cfg: db_path, raising=False)

    result = _invoke(["db", "assets", "prune", "--older-than", "30d", "--profile", "prod", "-y"])
    assert result.exit_code == 0, result.output
    with sqlite3.connect(db_path) as conn:
        names = [r[0] for r in conn.execute("SELECT name FROM remote_notebooks").fetchall()]
    assert names == ["new_nb"]


def test_db_assets_prune_validates_window():
    result = _invoke(
        ["db", "assets", "prune", "--older-than", "garbage", "--profile", "prod", "-y"]
    )
    assert result.exit_code != 0


def test_db_assets_refresh_confirmation_no(monkeypatch, tmp_path):
    """User declines the confirmation — nothing happens, no exception."""
    import amx.cli_support.commands.db_assets_impl as impl

    monkeypatch.setattr(impl, "_resolve_profile", lambda cfg, name: "prod")
    monkeypatch.setattr(click, "confirm", lambda *a, **kw: False)
    result = _invoke(["db", "assets", "refresh", "--profile", "prod"])
    assert result.exit_code == 0
    assert "Cancelled" in result.output or "cancelled" in result.output.lower()
