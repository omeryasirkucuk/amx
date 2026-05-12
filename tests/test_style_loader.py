from unittest.mock import patch


def _write_style_row(tmp_path, profile_name="default", enabled=True):
    from amx.llm.style.profile import StyleProfile
    from amx.storage.sqlite_store import SQLiteHistoryStore
    from amx.storage.style_store import StyleStore

    SQLiteHistoryStore(tmp_path / "history.db").init()
    store = StyleStore(tmp_path / "history.db")
    store.upsert(
        profile_name,
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
            redacted_examples=["Unique id of the <ENTITY>."],
        ),
        sample_count=3,
    )
    if not enabled:
        store.set_enabled(profile_name, False)


class _FakeCfg:
    def __init__(self, config_dir, active="default"):
        self.CONFIG_DIR = str(config_dir)
        self.active_llm_profile = active


def test_loader_returns_profile_when_present(tmp_path):
    from amx.config import AMXConfig
    from amx.llm.style import loader

    _write_style_row(tmp_path)
    with patch.object(AMXConfig, "load", return_value=_FakeCfg(tmp_path)):
        sp = loader.load_active_style_profile()
        assert sp is not None
        assert sp.language == "en-US"


def test_loader_returns_none_when_disabled(tmp_path):
    from amx.config import AMXConfig
    from amx.llm.style import loader

    _write_style_row(tmp_path, enabled=False)
    with patch.object(AMXConfig, "load", return_value=_FakeCfg(tmp_path)):
        assert loader.load_active_style_profile() is None


def test_loader_returns_none_when_db_missing(tmp_path):
    from amx.config import AMXConfig
    from amx.llm.style import loader

    # No history.db file at tmp_path; loader must NOT crash.
    with patch.object(AMXConfig, "load", return_value=_FakeCfg(tmp_path)):
        assert loader.load_active_style_profile() is None


def test_loader_returns_none_when_active_profile_empty(tmp_path):
    from amx.config import AMXConfig
    from amx.llm.style import loader

    with patch.object(AMXConfig, "load", return_value=_FakeCfg(tmp_path, active="")):
        assert loader.load_active_style_profile() is None


def test_profile_agent_attaches_style_on_init(tmp_path):
    from amx.config import AMXConfig

    _write_style_row(tmp_path)

    # Build a minimal fake LLMProvider to satisfy ProfileAgent.__init__.
    class _FakeLLMCfg:
        column_batch_size = 10
        n_alternatives = 3
        description_verbosity = "brief"
        prompt_detail_cfg = None

    class _StubLLM:
        cfg = _FakeLLMCfg()

    with patch.object(AMXConfig, "load", return_value=_FakeCfg(tmp_path)):
        from amx.agents.profile_agent import ProfileAgent

        agent = ProfileAgent(_StubLLM())
        assert agent._style_profile is not None
        assert agent._style_profile.language == "en-US"
