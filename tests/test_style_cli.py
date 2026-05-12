"""Tests for /style slash-command handlers."""

import json


def _make_cfg(tmp_path, monkeypatch):
    """Build a minimal AMXConfig instance pointing CONFIG_DIR at tmp_path."""
    from amx.config import AMXConfig, DBConfig, LLMConfig

    monkeypatch.setattr(AMXConfig, "CONFIG_DIR", str(tmp_path), raising=False)
    cfg = AMXConfig()
    # CONFIG_DIR is init=False with default_factory; set the instance attribute
    # directly so _db_path() resolves to tmp_path/history.db in tests.
    object.__setattr__(cfg, "CONFIG_DIR", str(tmp_path))
    cfg.active_db_profile = "warehouse"
    cfg.active_llm_profile = "default"
    cfg.db_profiles = {"warehouse": DBConfig()}
    cfg.llm_profiles = {"default": LLMConfig()}
    return cfg


def test_show_when_missing(tmp_path, monkeypatch, capsys):
    from amx.cli_support.commands.style import cmd_style
    from amx.storage.sqlite_store import SQLiteHistoryStore

    SQLiteHistoryStore(tmp_path / "history.db").init()
    cfg = _make_cfg(tmp_path, monkeypatch)

    cmd_style(cfg, ["show"])
    out = capsys.readouterr().out
    assert "no style reference" in out.lower() or "No style reference" in out


def test_clear_removes_entry(tmp_path, monkeypatch):
    from amx.cli_support.commands.style import cmd_style
    from amx.llm.style.profile import StyleProfile
    from amx.storage.sqlite_store import SQLiteHistoryStore
    from amx.storage.style_store import StyleStore

    SQLiteHistoryStore(tmp_path / "history.db").init()
    cfg = _make_cfg(tmp_path, monkeypatch)

    store = StyleStore(tmp_path / "history.db")
    store.upsert(
        "default",
        "a.b.c",
        "duckdb",
        StyleProfile(
            language="en-US",
            tone="x",
            avg_length_words=1,
            length_range=(1, 1),
            person="x",
            capitalization="x",
            ends_with_period=True,
            structural_patterns=[],
            vocabulary_register="x",
            redacted_examples=[],
        ),
        sample_count=3,
    )
    cmd_style(cfg, ["clear"])
    assert store.get("default") is None


def test_on_off_toggles_enabled(tmp_path, monkeypatch):
    from amx.cli_support.commands.style import cmd_style
    from amx.llm.style.profile import StyleProfile
    from amx.storage.sqlite_store import SQLiteHistoryStore
    from amx.storage.style_store import StyleStore

    SQLiteHistoryStore(tmp_path / "history.db").init()
    cfg = _make_cfg(tmp_path, monkeypatch)
    store = StyleStore(tmp_path / "history.db")
    store.upsert(
        "default",
        "a.b.c",
        "duckdb",
        StyleProfile(
            language="en-US",
            tone="x",
            avg_length_words=1,
            length_range=(1, 1),
            person="x",
            capitalization="x",
            ends_with_period=True,
            structural_patterns=[],
            vocabulary_register="x",
            redacted_examples=[],
        ),
        sample_count=3,
    )
    cmd_style(cfg, ["off"])
    assert store.get("default").enabled is False
    cmd_style(cfg, ["on"])
    assert store.get("default").enabled is True


def test_set_persists_via_mocked_connector_and_llm(tmp_path, monkeypatch):
    from amx.cli_support.commands import style as style_mod
    from amx.cli_support.commands.style import cmd_style
    from amx.storage.sqlite_store import SQLiteHistoryStore
    from amx.storage.style_store import StyleStore

    SQLiteHistoryStore(tmp_path / "history.db").init()
    cfg = _make_cfg(tmp_path, monkeypatch)

    fake_comments = {f"col_{i}": f"Unique id of order {i}." for i in range(5)}
    fake_llm_resp = json.dumps(
        {
            "language": "en-US",
            "tone": "formal",
            "avg_length_words": 5,
            "length_range": [3, 7],
            "person": "impersonal",
            "capitalization": "sentence-case",
            "ends_with_period": True,
            "structural_patterns": ["noun + role"],
            "vocabulary_register": "business",
            "redacted_examples": ["Unique id of the <ENTITY>."],
        }
    )

    class FakeConn:
        cfg = type("c", (), {"backend": "snowflake"})()
        backend = "snowflake"

        def use(self, db):
            pass

        def get_column_comments(self, schema, table):
            return fake_comments

    monkeypatch.setattr(style_mod, "_open_connector", lambda c, p: FakeConn())
    monkeypatch.setattr(style_mod, "_make_llm_caller", lambda c, p: lambda s, u: fake_llm_resp)

    cmd_style(cfg, ["set", "warehouse.sales.orders"])
    row = StyleStore(tmp_path / "history.db").get("default")
    assert row is not None
    assert row.source_ref == "warehouse.sales.orders"
    assert row.source_db_kind == "snowflake"
    assert row.profile.language == "en-US"
