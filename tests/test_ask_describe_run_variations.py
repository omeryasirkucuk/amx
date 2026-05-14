"""Ask's ``describe_run`` tool must surface Variations descendants.

The Ask page asks free-form questions about runs. The agent's
catalog tool ``describe_run`` in
``amx.search.agent_tools.ToolBox._tool_describe_run`` is the only
path the agent has into a run's metadata. Without descendants on
the response, the agent cannot answer "what variations did we try
for column X?" or "were those semantic or lexical?" -- the user's
explicit ask.

These tests pin the contract:

* v1 result rows carry ``alternatives_mode`` at the top level.
* When ``include_variations=True`` (default) each result row has a
  ``variations`` list whose entries carry ``mode``,
  ``seed_alternative_text``, ``version_label``, ``descendant_run_id``.
* When ``include_variations=False`` the ``variations`` key is
  absent so long Ask sessions can save tokens.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from amx.config import AMXConfig
from amx.search.agent_tools import ToolBox
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _build_toolbox() -> ToolBox:
    cfg = AMXConfig()
    catalog = MagicMock()
    return ToolBox(cfg, catalog, db_factory=lambda: MagicMock())


def _seed_parent_run(s: SQLiteHistoryStore) -> tuple[int, int]:
    parent_id = s.create_run(
        command="analyze.run",
        mode="batch",
        db_backend="postgresql",
        db_profile="local",
        llm_provider="openai",
        llm_model="gpt-x",
        scope={"public": ["country"]},
        settings={"alternatives_mode": "semantic"},
    )
    [rid] = s.save_run_results(
        parent_id,
        [
            {
                "schema": "public",
                "table": "country",
                "column": "abbreviation",
                "asset_kind": "column",
                "source": "llm",
                "confidence": "medium",
                "alternatives": [
                    "Two-letter ISO code identifying the country.",
                    "Short alpha code for the country (e.g. US, TR).",
                ],
                "alternatives_mode": "semantic",
                "model": "gpt-x",
                "provider": "openai",
            }
        ],
    )
    return parent_id, rid


def _seed_variations_descendant(
    s: SQLiteHistoryStore,
    *,
    parent_run_id: int,
    parent_result_id: int,
    seed_text: str,
    new_alts: list[str],
    mode: str = "lexical",
) -> int:
    child_id = s.create_run(
        command="rerun",
        mode="single",
        db_backend="postgresql",
        db_profile="local",
        llm_provider="openai",
        llm_model="gpt-x",
        scope={"public": ["country"]},
        settings={
            "trigger": "variations",
            "parent_run_id": parent_run_id,
            "alternatives_mode": mode,
        },
    )
    s.save_run_results(
        child_id,
        [
            {
                "schema": "public",
                "table": "country",
                "column": "abbreviation",
                "asset_kind": "column",
                "source": "rerun",
                "confidence": "medium",
                "alternatives": new_alts,
                "alternatives_mode": mode,
                "model": "gpt-x",
                "provider": "openai",
                "parent_result_id": parent_result_id,
                "rerun_seq": s.next_rerun_seq(parent_result_id),
                "seed_alternative_id": f"{parent_result_id}:0",
                "seed_alternative_text": seed_text,
                "parent_run_id": parent_run_id,
            }
        ],
    )
    return child_id


class TestDescribeRunVariations:
    def test_describe_run_surfaces_variations(self, store: SQLiteHistoryStore) -> None:
        parent_id, parent_rid = _seed_parent_run(store)
        seed = "Two-letter ISO code identifying the country."
        child = _seed_variations_descendant(
            store,
            parent_run_id=parent_id,
            parent_result_id=parent_rid,
            seed_text=seed,
            new_alts=[
                "Three-letter alpha-3 code identifying the country.",
                "Numeric ISO 3166-1 country code.",
            ],
            mode="lexical",
        )

        toolbox = _build_toolbox()
        with patch("amx.storage.sqlite_store.history_store", return_value=store):
            out = toolbox._tool_describe_run(parent_id)

        # Sanity: results present, descendants_warning absent.
        assert "results" in out
        assert out.get("descendants_warning") is None
        assert len(out["results"]) == 1
        row = out["results"][0]
        assert row["alternatives_mode"] == "semantic"
        # Variations block has the v2 entry.
        variations = row["variations"]
        assert len(variations) == 1
        v2 = variations[0]
        assert v2["version_label"] == "v2"
        assert v2["mode"] == "lexical"
        assert v2["seed_alternative_text"] == seed
        assert v2["descendant_run_id"] == child
        assert v2["kind"] == "variations"
        # The variation's alternatives list survives.
        assert v2["alternatives"] == [
            "Three-letter alpha-3 code identifying the country.",
            "Numeric ISO 3166-1 country code.",
        ]

    def test_describe_run_no_variations_returns_empty_list(self, store: SQLiteHistoryStore) -> None:
        parent_id, _ = _seed_parent_run(store)
        toolbox = _build_toolbox()
        with patch("amx.storage.sqlite_store.history_store", return_value=store):
            out = toolbox._tool_describe_run(parent_id)

        assert len(out["results"]) == 1
        # Default include_variations=True with no descendants ->
        # variations is an empty list, NOT missing.
        assert out["results"][0]["variations"] == []

    def test_describe_run_include_variations_false_omits_block(
        self, store: SQLiteHistoryStore
    ) -> None:
        parent_id, parent_rid = _seed_parent_run(store)
        _seed_variations_descendant(
            store,
            parent_run_id=parent_id,
            parent_result_id=parent_rid,
            seed_text="seed",
            new_alts=["alt"],
        )
        toolbox = _build_toolbox()
        with patch("amx.storage.sqlite_store.history_store", return_value=store):
            out = toolbox._tool_describe_run(parent_id, include_variations=False)

        assert len(out["results"]) == 1
        # variations key not present -> token-saver path.
        assert "variations" not in out["results"][0]
        # alternatives_mode is still present because the v1 itself
        # carries it; it is cheap and the LLM needs it even in the
        # token-saver path.
        assert "alternatives_mode" in out["results"][0]

    def test_multiple_variations_ordered_chronologically(self, store: SQLiteHistoryStore) -> None:
        parent_id, parent_rid = _seed_parent_run(store)
        first = _seed_variations_descendant(
            store,
            parent_run_id=parent_id,
            parent_result_id=parent_rid,
            seed_text="seed",
            new_alts=["v2 alt"],
            mode="lexical",
        )
        second = _seed_variations_descendant(
            store,
            parent_run_id=parent_id,
            parent_result_id=parent_rid,
            seed_text="seed",
            new_alts=["v3 alt"],
            mode="semantic",
        )

        toolbox = _build_toolbox()
        with patch("amx.storage.sqlite_store.history_store", return_value=store):
            out = toolbox._tool_describe_run(parent_id)

        variations = out["results"][0]["variations"]
        assert [v["version_label"] for v in variations] == ["v2", "v3"]
        assert variations[0]["descendant_run_id"] == first
        assert variations[1]["descendant_run_id"] == second
        assert variations[0]["mode"] == "lexical"
        assert variations[1]["mode"] == "semantic"
