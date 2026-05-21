import click
import pytest
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
    monkeypatch.setattr(
        "amx.services.ingest_assets.IngestAssetsService.run", fake_run
    )

    result = _invoke([
        "db", "ingest-assets",
        "--profile", "prod",
        "--types", "notebooks",
        "--history-days", "14",
    ])
    assert result.exit_code == 0, result.output
    assert captured["profile"] == "prod"
    assert captured["request"].types == ["notebooks"]
    assert captured["request"].history_days == 14
    assert "notebooks=3" in result.output


def test_ingest_assets_rejects_unknown_type():
    result = _invoke([
        "db", "ingest-assets",
        "--profile", "prod",
        "--types", "definitely_not_real,notebooks",
    ])
    assert result.exit_code != 0
    assert "Unknown asset type" in result.output or "definitely_not_real" in result.output
