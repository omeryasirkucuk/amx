from amx.llm.style.profile import StyleProfile
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.storage.style_store import StyleStore


def _example_profile() -> StyleProfile:
    return StyleProfile(
        language="en-US",
        tone="formal",
        avg_length_words=10,
        length_range=(5, 18),
        person="impersonal",
        capitalization="sentence-case",
        ends_with_period=True,
        structural_patterns=["noun + role"],
        vocabulary_register="business",
        redacted_examples=["Unique id of the <ENTITY>."],
    )


def test_upsert_and_fetch(tmp_path):
    db = tmp_path / "amx.sqlite"
    SQLiteHistoryStore(db).init()
    store = StyleStore(db)

    store.upsert(
        llm_profile="default",
        source_ref="warehouse.sales.orders",
        source_db_kind="snowflake",
        profile=_example_profile(),
        sample_count=12,
    )
    row = store.get("default")
    assert row is not None
    assert row.llm_profile == "default"
    assert row.source_ref == "warehouse.sales.orders"
    assert row.profile.language == "en-US"
    assert row.enabled is True
    assert row.sample_count == 12


def test_upsert_overwrites_same_profile(tmp_path):
    db = tmp_path / "amx.sqlite"
    SQLiteHistoryStore(db).init()
    store = StyleStore(db)
    store.upsert("default", "a.b.c", "duckdb", _example_profile(), 3)
    first = store.get("default")
    store.upsert("default", "x.y.z", "duckdb", _example_profile(), 7)
    second = store.get("default")
    assert second is not None
    assert second.source_ref == "x.y.z"
    assert second.sample_count == 7
    assert second.created_at == first.created_at  # preserved on overwrite
    assert second.updated_at >= first.updated_at


def test_set_enabled_and_clear(tmp_path):
    db = tmp_path / "amx.sqlite"
    SQLiteHistoryStore(db).init()
    store = StyleStore(db)
    store.upsert("default", "a.b.c", "duckdb", _example_profile(), 3)
    store.set_enabled("default", False)
    row = store.get("default")
    assert row is not None and row.enabled is False
    store.clear("default")
    assert store.get("default") is None
