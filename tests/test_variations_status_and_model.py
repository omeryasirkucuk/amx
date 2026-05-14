"""Bug-fix regression tests: variations status transitions, model
override end-to-end persistence, on_run_created callback wiring.

Pins the contracts that drove the live-deploy bug report:

* Bug 2/3: a worker whose every target fails ends as ``status=failed``
  (with ``error_text`` populated) — NOT silently as ``partial``.
* Bug 4: ``rerun_items`` fires ``on_run_created`` immediately after
  ``hs.create_run`` so the router worker can bind ``job.run_id``
  BEFORE the (multi-second) target loop. Without this, the runs list
  + the run-detail page can't find the live worker by numeric id.
* Bug 5: the analysis_runs row records the DERIVED ``cfg.llm``
  identity (post-override) rather than inheriting the parent run's
  llm_provider/llm_model. Reading parent values would silently bury
  the override and leave the runs list reporting kimi-k2.6 even
  when claude-opus-4 actually answered the request.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.agents._orchestrator import rerun as rerun_mod
from amx.agents._orchestrator.rerun import RerunContextError, rerun_items
from amx.config import AMXConfig, LLMConfig
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    monkeypatch.setattr(rerun_mod, "history_store", lambda: s)
    # The rerun executor also imports ``history_store`` via
    # ``rerun_context`` for the snapshot writer.
    from amx.agents import rerun_context as rc_mod

    monkeypatch.setattr(rc_mod, "history_store", lambda: s)
    return s


def _cfg(
    active: LLMConfig | None = None, profiles: dict[str, LLMConfig] | None = None
) -> AMXConfig:
    """Build an AMXConfig with an active LLM + an alternate saved profile."""
    cfg = AMXConfig()
    cfg.llm = active or LLMConfig(
        provider="openrouter",
        model="moonshotai/kimi-k2-thinking",
        api_key="active-key",
    )
    cfg.llm_profiles = profiles or {
        "active": cfg.llm,
        "claude_opus": LLMConfig(
            provider="anthropic",
            model="claude-opus-4-5",
            api_key="opus-key",
        ),
    }
    cfg.active_llm_profile = "active"
    return cfg


def _seed_run(s: SQLiteHistoryStore) -> tuple[int, int]:
    """One run + one result row using the active (kimi) profile."""
    run_id = s.create_run(
        command="analyze.run",
        mode="chat",
        db_backend="postgresql",
        db_profile="local",
        llm_provider="openrouter",
        llm_model="moonshotai/kimi-k2-thinking",
        scope={"public": ["orders"]},
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
                "alternatives": ["a1", "a2", "a3"],
            }
        ],
    )
    return run_id, rid


class TestStatusFailedNotPartial:
    """Every-target-failed runs must surface as ``failed`` so the
    runs list visibly distinguishes a 0.0s no-LLM-call failure from
    a long mixed-result run."""

    def test_all_targets_failing_marks_run_failed(
        self, store: SQLiteHistoryStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _, rid = _seed_run(store)
        cfg = _cfg()
        # Force the snapshot builder to fail for every target so every
        # outcome carries an error.
        from amx.agents import rerun_context as rc_mod

        def boom(*args, **kwargs):
            raise RerunContextError("synthetic snapshot failure")

        monkeypatch.setattr(rc_mod, "build_context_snapshot", boom)
        monkeypatch.setattr(rerun_mod, "build_context_snapshot", boom)

        new_run_id, outcomes = rerun_items(cfg, target_result_ids=[rid])

        # Every outcome carries the error string.
        assert all(o.error and "synthetic snapshot failure" in o.error for o in outcomes)
        # And the run row is marked ``failed`` with the error text
        # surfaced — not silently ``partial``.
        new_run = store.get_run(int(new_run_id))
        assert new_run is not None, "expected analysis_runs row to exist"
        assert new_run.get("status") == "failed", (
            f"Expected status=failed, got {new_run.get('status')!r}. "
            "All-error runs must NOT collapse into 'partial'."
        )
        assert "synthetic snapshot failure" in (new_run.get("error_text") or ""), (
            f"error_text missing the failure cause. Got: {new_run.get('error_text')!r}"
        )

    def test_mixed_outcomes_still_partial(
        self, store: SQLiteHistoryStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One success + one failure must still surface as ``partial``
        — we only escalate to ``failed`` when EVERY target fails."""
        run_id, rid_a = _seed_run(store)
        [rid_b] = store.save_run_results(
            run_id,
            [
                {
                    "schema": "public",
                    "table": "orders",
                    "column": "shipped_at",
                    "asset_kind": "column",
                    "source": "llm",
                    "confidence": "medium",
                    "alternatives": ["b1"],
                }
            ],
        )

        from amx.agents import rerun_context as rc_mod
        from amx.agents.base import (
            AgentContext,
            Confidence,
            MetadataSuggestion,
        )

        call_count = [0]

        def half_failing(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RerunContextError("first one fails")
            # Second one writes a snapshot that hydrates cleanly.
            payload = {
                "schema": "public",
                "table": "orders",
                "column": "shipped_at",
                "asset_kind": "column",
                "db_profile": {"columns": [{"name": "shipped_at"}]},
                "rag_context": [],
                "rag_hits": [],
                "code_hits": [],
                "code_context": [],
                "existing_metadata": {"backend": "postgresql"},
                "user_instructions": "",
                "original": {
                    "run_id": run_id,
                    "result_id": rid_b,
                    "rerun_seq": 0,
                    "parent_result_id": None,
                    "doc_profile": None,
                    "code_profile": None,
                },
            }
            sid = "snap-mixed"
            store.save_rerun_snapshot(
                snapshot_id=sid,
                job_id=kwargs.get("job_id") or "j",
                target_result_id=int(kwargs.get("target_result_id") or 0),
                payload=payload,
            )
            return sid

        monkeypatch.setattr(rc_mod, "build_context_snapshot", half_failing)
        monkeypatch.setattr(rerun_mod, "build_context_snapshot", half_failing)

        # Patch the inner agent fan-out so the second target gets a
        # synthetic successful suggestion without a real LLM call.
        def fake_rerun(cfg_arg, *, ctx: AgentContext, llm, snapshot):
            return MetadataSuggestion(
                schema=ctx.schema,
                table=ctx.table,
                column=ctx.column,
                suggestions=["synth-a", "synth-b"],
                confidence=Confidence.MEDIUM,
                reasoning="synth",
                source="rerun",
            )

        monkeypatch.setattr(rerun_mod, "_rerun_table_or_column", fake_rerun)

        cfg = _cfg()
        new_run_id, outcomes = rerun_items(cfg, target_result_ids=[rid_a, rid_b])

        # One failure + one success.
        assert sum(1 for o in outcomes if o.error) == 1
        assert sum(1 for o in outcomes if not o.error) == 1
        new_run = store.get_run(int(new_run_id))
        assert new_run.get("status") == "partial", (
            f"Expected partial for 1 failure + 1 success, got {new_run.get('status')!r}"
        )


class TestOnRunCreatedCallback:
    """Bug 4 wire: the callback fires immediately after
    ``hs.create_run`` so the router can pin ``job.run_id`` before the
    (potentially long) target loop runs."""

    def test_callback_fires_before_target_loop(
        self, store: SQLiteHistoryStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _, rid = _seed_run(store)
        cfg = _cfg()

        # Sequence-of-events probe: build_context_snapshot is the
        # first thing the target loop calls. If on_run_created
        # fires AFTER it, the timeline list will have build_* first.
        timeline: list[str] = []

        from amx.agents import rerun_context as rc_mod

        def boom(*args, **kwargs):
            timeline.append("snapshot")
            raise RerunContextError("stop early")

        monkeypatch.setattr(rc_mod, "build_context_snapshot", boom)
        monkeypatch.setattr(rerun_mod, "build_context_snapshot", boom)

        def on_created(rid_int: int) -> None:
            timeline.append(f"run_created:{rid_int}")

        rerun_items(cfg, target_result_ids=[rid], on_run_created=on_created)

        # The callback must precede the snapshot attempt.
        assert timeline[0].startswith("run_created:"), (
            f"on_run_created must fire BEFORE the target loop. Got timeline: {timeline!r}"
        )

    def test_callback_exception_does_not_kill_worker(
        self, store: SQLiteHistoryStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A buggy router callback must not take the worker down with it."""
        _, rid = _seed_run(store)
        cfg = _cfg()

        from amx.agents import rerun_context as rc_mod

        monkeypatch.setattr(
            rc_mod,
            "build_context_snapshot",
            lambda *a, **k: (_ for _ in ()).throw(RerunContextError("stop")),
        )
        monkeypatch.setattr(
            rerun_mod,
            "build_context_snapshot",
            lambda *a, **k: (_ for _ in ()).throw(RerunContextError("stop")),
        )

        def bad_cb(rid_int: int) -> None:
            raise RuntimeError("router callback exploded")

        # Must not raise.
        rerun_items(cfg, target_result_ids=[rid], on_run_created=bad_cb)


class TestRunRowRecordsDerivedLlmIdentity:
    """Bug 5: when a profile override flips provider/model, the new
    analysis_runs row must record the DERIVED identity — not silently
    inherit the parent's. Reading parent values would leave the runs
    list reporting the original profile even when the override
    profile actually answered the request."""

    def test_profile_override_swaps_recorded_llm_on_new_run(
        self, store: SQLiteHistoryStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _, rid = _seed_run(store)
        cfg = _cfg()

        # Patch the snapshot path so the executor reaches finish_run
        # without making a real LLM call. We don't care about the
        # outcome here — only the analysis_runs row's recorded llm.
        from amx.agents import rerun_context as rc_mod

        def boom(*args, **kwargs):
            raise RerunContextError("skip llm path")

        monkeypatch.setattr(rc_mod, "build_context_snapshot", boom)
        monkeypatch.setattr(rerun_mod, "build_context_snapshot", boom)

        new_run_id, _ = rerun_items(
            cfg,
            target_result_ids=[rid],
            llm_overrides={"profile": "claude_opus"},
        )

        new_run = store.get_run(int(new_run_id))
        assert new_run is not None
        # The runs list reads ``llm_provider`` + ``llm_model``
        # directly. They must reflect the picked profile, NOT the
        # parent run's openrouter/kimi values.
        assert new_run.get("llm_provider") == "anthropic", (
            f"Expected llm_provider=anthropic (from profile override), "
            f"got {new_run.get('llm_provider')!r}. Reading parent_run "
            "first silently buries the override on the runs list."
        )
        assert new_run.get("llm_model") == "claude-opus-4-5", (
            f"Expected llm_model=claude-opus-4-5, got {new_run.get('llm_model')!r}"
        )

    def test_no_override_preserves_active_profile_on_recorded_run(
        self, store: SQLiteHistoryStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No-override case: the recorded llm matches cfg.llm (which
        equals the active profile). Same defensive fall-back to
        parent's value if cfg.llm is somehow empty."""
        _, rid = _seed_run(store)
        cfg = _cfg()

        from amx.agents import rerun_context as rc_mod

        def boom(*args, **kwargs):
            raise RerunContextError("skip llm path")

        monkeypatch.setattr(rc_mod, "build_context_snapshot", boom)
        monkeypatch.setattr(rerun_mod, "build_context_snapshot", boom)

        new_run_id, _ = rerun_items(cfg, target_result_ids=[rid])
        new_run = store.get_run(int(new_run_id))
        assert new_run.get("llm_provider") == "openrouter"
        assert new_run.get("llm_model") == "moonshotai/kimi-k2-thinking"
