"""End-to-end tests for the Variations executor.

Mocks the LLM provider so the test exercises the seed-injection +
audit-column-write path without making a network call. Verifies:

* the new row carries ``seed_alternative_id``, ``seed_alternative_text``,
  and ``parent_run_id`` populated correctly;
* the user's top-level mode pick is in effect on the new row;
* the seed text is filtered back out of the alternatives list.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.agents._orchestrator import rerun as rerun_mod
from amx.agents._orchestrator import variations as var_mod
from amx.agents._orchestrator.rerun import RerunOutcome
from amx.agents._orchestrator.variations import (
    _filter_seed_out,
    _seed_directive,
    variations_one_item,
)
from amx.config import AMXConfig, LLMConfig
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    # Point both modules at the test store so the orchestrator + the
    # post-run column patch hit the same DB.
    monkeypatch.setattr(rerun_mod, "history_store", lambda: s)
    monkeypatch.setattr(var_mod, "history_store", lambda: s)
    return s


def _seed_original_run(s: SQLiteHistoryStore) -> tuple[int, int]:
    run_id = s.create_run(
        command="analyze.run",
        mode="chat",
        db_backend="postgresql",
        db_profile="local",
        llm_provider="openai",
        llm_model="gpt-x",
        scope={"public": ["orders"]},
        settings={"alternatives_mode": "semantic"},
    )
    [rid] = s.save_run_results(
        run_id,
        [
            {
                "schema": "public",
                "table": "orders",
                "column": "status",
                "asset_kind": "column",
                "source": "llm",
                "confidence": "medium",
                "alternatives": [
                    "Order lifecycle state — open, fulfilled, cancelled.",
                    "Geographic reference table storing coordinates.",
                    "Spatial coordinate table recording lat/long pairs.",
                ],
                "alternatives_mode": "semantic",
                "model": "gpt-x",
                "provider": "openai",
            }
        ],
    )
    return run_id, rid


class TestSeedDirective:
    def test_semantic_block(self) -> None:
        out = _seed_directive(seed_text="a seed", mode="semantic", user_addendum=None)
        assert "SEED_DESCRIPTION" in out
        assert "semantic mode" in out
        assert "paraphrase" in out

    def test_lexical_block(self) -> None:
        out = _seed_directive(seed_text="a seed", mode="lexical", user_addendum=None)
        assert "SEED_DESCRIPTION" in out
        assert "lexical mode" in out
        assert "vocabulary" in out

    def test_addendum_appended(self) -> None:
        out = _seed_directive(seed_text="seed", mode="semantic", user_addendum="extra guidance")
        assert "extra guidance" in out


class TestFilterSeedOut:
    def test_filters_exact_match(self) -> None:
        result = _filter_seed_out(["seed", "v1", "v2"], seed_text="seed")
        assert result == ["v1", "v2"]

    def test_case_and_whitespace_insensitive(self) -> None:
        result = _filter_seed_out(["  SEED  ", "v1"], seed_text="seed")
        assert result == ["v1"]

    def test_no_match_passes_through(self) -> None:
        result = _filter_seed_out(["v1", "v2"], seed_text="something else")
        assert result == ["v1", "v2"]


class TestVariationsOneItem:
    def test_empty_seed_raises(self, store: SQLiteHistoryStore) -> None:
        from amx.agents.rerun_context import RerunContextError

        cfg = AMXConfig(llm=LLMConfig(provider="openai", model="gpt-x", api_key="k"))
        with pytest.raises(RerunContextError):
            variations_one_item(
                cfg,
                original_run_id=1,
                result_id=1,
                alternative_index=0,
                seed_text="   ",
            )

    def test_mode_override_wins(
        self, store: SQLiteHistoryStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The user's top-level mode pick must override any nested
        ``llm_overrides.alternatives_mode``."""
        run_id, rid = _seed_original_run(store)
        seed = "Geographic reference table storing coordinates."
        captured: dict[str, object] = {}

        def fake_rerun_items(*args, **kwargs):
            captured["overrides"] = kwargs.get("llm_overrides")
            # Spawn a fresh child run first so the new row hangs off it
            # (the production rerun_items does the same).
            new_run_id = store.create_run(
                command="rerun",
                mode="single",
                db_backend="postgresql",
                db_profile="local",
                llm_provider="openai",
                llm_model="gpt-x",
                scope={"public": ["orders"]},
                settings={
                    "trigger": "variations",
                    "parent_run_id": run_id,
                    "alternatives_mode": kwargs["llm_overrides"]["alternatives_mode"],
                },
            )
            seq = store.next_rerun_seq(rid)
            [new_id] = store.save_run_results(
                int(new_run_id),
                [
                    {
                        "schema": "public",
                        "table": "orders",
                        "column": "status",
                        "asset_kind": "column",
                        "source": "rerun",
                        "confidence": "high",
                        "alternatives": [
                            seed,
                            "Coordinate atlas — paired latitudes.",
                            "Map points repository keyed by location.",
                        ],
                        "alternatives_mode": kwargs["llm_overrides"]["alternatives_mode"],
                        "model": "gpt-x",
                        "provider": "openai",
                        "parent_result_id": rid,
                        "rerun_seq": seq,
                    }
                ],
            )
            outcome = RerunOutcome(
                target_result_id=rid,
                new_result_id=int(new_id),
                rerun_seq=int(seq),
                schema="public",
                table="orders",
                column="status",
                asset_kind="column",
                alternatives=[
                    seed,
                    "Coordinate atlas — paired latitudes.",
                    "Map points repository keyed by location.",
                ],
                confidence="high",
                logprob_score=None,
                source="rerun",
            )
            return int(new_run_id), [outcome]

        monkeypatch.setattr(var_mod, "rerun_items", fake_rerun_items)

        cfg = AMXConfig(llm=LLMConfig(provider="openai", model="gpt-x", api_key="k"))
        new_run_id, outcome = variations_one_item(
            cfg,
            original_run_id=run_id,
            result_id=rid,
            alternative_index=1,
            seed_text=seed,
            mode="lexical",
            llm_overrides={"alternatives_mode": "semantic"},  # nested — must lose
        )

        # Top-level mode wins.
        assert captured["overrides"]["alternatives_mode"] == "lexical"
        # Audit columns landed on the new row.
        rows = store.get_run_results(new_run_id)
        assert len(rows) == 1
        new_row = rows[0]
        assert new_row["seed_alternative_id"] == f"{rid}:1"
        assert new_row["seed_alternative_text"] == seed
        assert new_row["parent_run_id"] == run_id
        # Seed filtered out of the alternatives.
        alts = new_row["alternatives_json"]
        # The store parses alternatives_json into a list[str|dict];
        # extract plain text for the comparison.
        alt_texts = [(alt if isinstance(alt, str) else alt.get("text", "")) for alt in alts]
        assert seed not in alt_texts, alt_texts
        assert len(alt_texts) == 2


class TestVariationsStructuredShapeAndWarning:
    """Bug-fix regression: ``_update_variation_columns`` previously
    overwrote ``alternatives_json`` with a plain ``list[str]``,
    stripping per-alternative confidence signal data and leaving v2
    / v3 rows badge-less in the Studio. Pin the fix.

    Same tests also pin the ``production_warning`` flag — the new
    Bug-1 column populated when the LLM (or parser) returned fewer
    alternatives than the active profile asked for."""

    def test_structured_alternatives_preserved_after_seed_filter(
        self, store: SQLiteHistoryStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        run_id, rid = _seed_original_run(store)
        seed = "Geographic reference table storing coordinates."

        def fake_rerun_items(*args, **kwargs):
            new_run_id = store.create_run(
                command="rerun",
                mode="single",
                db_backend="postgresql",
                db_profile="local",
                llm_provider="openai",
                llm_model="gpt-x",
                scope={"public": ["orders"]},
                settings={"trigger": "variations", "parent_run_id": run_id},
            )
            seq = store.next_rerun_seq(rid)
            # Persist with the STRUCTURED shape — ``alternative_scores``
            # tells ``build_alternatives_json`` to emit per-alt dicts.
            from amx.llm.confidence import AlternativeScore

            scores = [
                AlternativeScore(
                    text=seed,
                    signal="self_consistency",
                    score=0.85,
                    band="high",
                ),
                AlternativeScore(
                    text="Variation A — coords stored physically.",
                    signal="self_consistency",
                    score=0.72,
                    band="medium",
                ),
                AlternativeScore(
                    text="Variation B — coords mapped logically.",
                    signal="self_consistency",
                    score=0.65,
                    band="medium",
                ),
            ]
            [new_id] = store.save_run_results(
                int(new_run_id),
                [
                    {
                        "schema": "public",
                        "table": "orders",
                        "column": "status",
                        "asset_kind": "column",
                        "source": "rerun",
                        "confidence": "high",
                        "alternatives": [s.text for s in scores],
                        "alternative_scores": [s.to_json() for s in scores],
                        "alternatives_mode": kwargs["llm_overrides"][
                            "alternatives_mode"
                        ],
                        "model": "gpt-x",
                        "provider": "openai",
                        "parent_result_id": rid,
                        "rerun_seq": seq,
                    }
                ],
            )
            outcome = RerunOutcome(
                target_result_id=rid,
                new_result_id=int(new_id),
                rerun_seq=int(seq),
                schema="public",
                table="orders",
                column="status",
                asset_kind="column",
                alternatives=[s.text for s in scores],
                confidence="high",
                logprob_score=None,
                source="rerun",
            )
            return int(new_run_id), [outcome]

        monkeypatch.setattr(var_mod, "rerun_items", fake_rerun_items)

        cfg = AMXConfig(
            llm=LLMConfig(provider="openai", model="gpt-x", api_key="k", n_alternatives=3)
        )
        new_run_id, _ = variations_one_item(
            cfg,
            original_run_id=run_id,
            result_id=rid,
            alternative_index=1,
            seed_text=seed,
            mode="lexical",
        )

        rows = store.get_run_results(new_run_id)
        new_row = rows[0]
        alts = new_row["alternatives_json"]
        # Each surviving entry must be a structured dict — that's the
        # whole point of this fix. A plain ``list[str]`` here means
        # the post-step overwrite has regressed and v2/v3 rows will
        # render without confidence badges in the Studio.
        assert all(isinstance(a, dict) for a in alts), (
            f"alternatives_json lost structured shape: {alts!r}. "
            "Re-introduces the badge-less v2/v3 regression."
        )
        # Per-alternative signal preserved on the surviving entries.
        for entry in alts:
            assert entry.get("signal") == "self_consistency"
        # Seed dropped from the structured list (no entry with the
        # seed text remains).
        seed_texts = [a.get("text", "") for a in alts]
        assert seed not in seed_texts
        # n_alternatives=3 with seed echo (1 dropped) ⇒ expected 2;
        # this fixture returns 2 so no warning.
        assert new_row.get("production_warning") in (None, "")

    def test_production_warning_fires_on_under_production(
        self, store: SQLiteHistoryStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When the LLM returns 2 of 3 requested and NO seed echo,
        ``production_warning`` carries
        ``produced 2 of 3 requested``."""
        run_id, rid = _seed_original_run(store)
        seed = "completely unique seed nothing matches"

        def fake_rerun_items(*args, **kwargs):
            new_run_id = store.create_run(
                command="rerun",
                mode="single",
                db_backend="postgresql",
                db_profile="local",
                llm_provider="openai",
                llm_model="gpt-x",
                scope={"public": ["orders"]},
                settings={"trigger": "variations", "parent_run_id": run_id},
            )
            seq = store.next_rerun_seq(rid)
            # Only TWO alts — under-production. Neither matches seed.
            [new_id] = store.save_run_results(
                int(new_run_id),
                [
                    {
                        "schema": "public",
                        "table": "orders",
                        "column": "status",
                        "asset_kind": "column",
                        "source": "rerun",
                        "confidence": "high",
                        "alternatives": ["only-one", "only-two"],
                        "alternatives_mode": kwargs["llm_overrides"][
                            "alternatives_mode"
                        ],
                        "model": "gpt-x",
                        "provider": "openai",
                        "parent_result_id": rid,
                        "rerun_seq": seq,
                    }
                ],
            )
            outcome = RerunOutcome(
                target_result_id=rid,
                new_result_id=int(new_id),
                rerun_seq=int(seq),
                schema="public",
                table="orders",
                column="status",
                asset_kind="column",
                alternatives=["only-one", "only-two"],
                confidence="high",
                logprob_score=None,
                source="rerun",
            )
            return int(new_run_id), [outcome]

        monkeypatch.setattr(var_mod, "rerun_items", fake_rerun_items)

        cfg = AMXConfig(
            llm=LLMConfig(provider="openai", model="gpt-x", api_key="k", n_alternatives=3)
        )
        new_run_id, _ = variations_one_item(
            cfg,
            original_run_id=run_id,
            result_id=rid,
            alternative_index=0,
            seed_text=seed,
            mode="semantic",
        )

        new_row = store.get_run_results(new_run_id)[0]
        warning = new_row.get("production_warning")
        assert warning == "produced 2 of 3 requested", (
            f"Expected production_warning='produced 2 of 3 requested', got "
            f"{warning!r}"
        )

    def test_no_warning_on_full_set(
        self, store: SQLiteHistoryStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Three-of-three (with seed echo removing one): expected
        count is 2 and we got 2 ⇒ NO warning."""
        run_id, rid = _seed_original_run(store)
        seed = "Sample seed text"

        def fake_rerun_items(*args, **kwargs):
            new_run_id = store.create_run(
                command="rerun",
                mode="single",
                db_backend="postgresql",
                db_profile="local",
                llm_provider="openai",
                llm_model="gpt-x",
                scope={"public": ["orders"]},
                settings={"trigger": "variations", "parent_run_id": run_id},
            )
            seq = store.next_rerun_seq(rid)
            [new_id] = store.save_run_results(
                int(new_run_id),
                [
                    {
                        "schema": "public",
                        "table": "orders",
                        "column": "status",
                        "asset_kind": "column",
                        "source": "rerun",
                        "confidence": "high",
                        "alternatives": [seed, "v-A", "v-B"],
                        "alternatives_mode": kwargs["llm_overrides"][
                            "alternatives_mode"
                        ],
                        "model": "gpt-x",
                        "provider": "openai",
                        "parent_result_id": rid,
                        "rerun_seq": seq,
                    }
                ],
            )
            return int(new_run_id), [
                RerunOutcome(
                    target_result_id=rid,
                    new_result_id=int(new_id),
                    rerun_seq=int(seq),
                    schema="public",
                    table="orders",
                    column="status",
                    asset_kind="column",
                    alternatives=[seed, "v-A", "v-B"],
                    confidence="high",
                    logprob_score=None,
                    source="rerun",
                )
            ]

        monkeypatch.setattr(var_mod, "rerun_items", fake_rerun_items)

        cfg = AMXConfig(
            llm=LLMConfig(provider="openai", model="gpt-x", api_key="k", n_alternatives=3)
        )
        new_run_id, _ = variations_one_item(
            cfg,
            original_run_id=run_id,
            result_id=rid,
            alternative_index=0,
            seed_text=seed,
            mode="semantic",
        )
        assert store.get_run_results(new_run_id)[0].get("production_warning") in (
            None,
            "",
        )
