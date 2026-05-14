"""Re-Run feature tests.

Covers:
* AgentContext.user_instructions wiring (empty path stays byte-identical
  to the legacy prompt; non-empty path appends the suffix block).
* sqlite_store re-run schema (versioning columns + snapshot table +
  helpers).
* RerunContextError happy / unhappy paths in build_context_snapshot.
* rerun_items end-to-end with stubbed agents — verifies snapshot
  cleanup, version chain ordering, multi-item, and asset_kind dispatch.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest

from amx.agents.base import (
    AgentContext,
    Confidence,
    MetadataSuggestion,
    _user_instructions_block,
)
from amx.agents.rerun_context import (
    RerunContextError,
    hydrate_context,
    serialize_context,
)
from amx.storage.sqlite_store import SQLiteHistoryStore

# ── AgentContext + prompt suffix ────────────────────────────────────────────


def test_user_instructions_block_empty_returns_empty_string() -> None:
    """Without any addendum, the suffix is byte-empty so legacy prompts
    stay regression-safe."""
    ctx = AgentContext(schema="public", table="orders")
    assert _user_instructions_block(ctx) == ""


def test_user_instructions_block_with_text_renders_suffix() -> None:
    ctx = AgentContext(
        schema="public",
        table="orders",
        user_instructions="treat soft-deleted rows as live",
    )
    block = _user_instructions_block(ctx)
    assert "Additional instructions from user (re-run):" in block
    assert "treat soft-deleted rows as live" in block
    # The instruction is described as guidance, not a replacement, so
    # the agents do not throw away the original DB / docs / code
    # context. This phrasing is load-bearing — keep it.
    assert "guidance" in block.lower()


def test_user_instructions_block_strips_whitespace() -> None:
    ctx = AgentContext(user_instructions="   \n  ")
    assert _user_instructions_block(ctx) == ""


def test_serialize_hydrate_round_trip() -> None:
    """Snapshot serialization preserves user_instructions + db_profile shape."""
    original = AgentContext(
        schema="public",
        table="orders",
        column="status",
        asset_kind="column",
        db_profile={"row_count": 42, "columns": [{"name": "status", "dtype": "text"}]},
        existing_metadata={"database": "amx_test"},
        user_instructions="bias toward retail-domain language",
    )
    payload = serialize_context(original)
    rebuilt = hydrate_context(payload)
    assert rebuilt == original


# ── SQLite history store: re-run schema + helpers ───────────────────────────


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _seed_original_run(store: SQLiteHistoryStore) -> tuple[int, int]:
    """Create one analysis_runs row + one run_results row; return ids."""
    run_id = store.create_run(
        command="analyze.run",
        mode="chat",
        db_backend="postgresql",
        db_profile="local",
        llm_provider="openai",
        llm_model="gpt-test",
        scope={"public": ["orders"]},
    )
    [result_id] = store.save_run_results(
        run_id,
        [
            {
                "schema": "public",
                "table": "orders",
                "column": "status",
                "asset_kind": "column",
                "source": "llm",
                "confidence": "medium",
                "alternatives": ["v1-alt-a", "v1-alt-b"],
            }
        ],
    )
    return run_id, result_id


def test_save_run_results_persists_rerun_fields(store: SQLiteHistoryStore) -> None:
    run_id, original_id = _seed_original_run(store)
    [child_id] = store.save_run_results(
        run_id,
        [
            {
                "schema": "public",
                "table": "orders",
                "column": "status",
                "asset_kind": "column",
                "source": "rerun",
                "confidence": "high",
                "alternatives": ["v2-alt"],
                "parent_result_id": original_id,
                "rerun_seq": 1,
                "user_instructions": "bias toward soft-delete",
            }
        ],
    )
    row = store.get_run_result(child_id)
    assert row is not None
    assert row["parent_result_id"] == original_id
    assert row["rerun_seq"] == 1
    assert row["user_instructions"] == "bias toward soft-delete"


def test_next_rerun_seq_increments_monotonically(store: SQLiteHistoryStore) -> None:
    run_id, original_id = _seed_original_run(store)
    assert store.next_rerun_seq(original_id) == 1
    store.save_run_results(
        run_id,
        [
            {
                "schema": "public",
                "table": "orders",
                "column": "status",
                "asset_kind": "column",
                "source": "rerun",
                "confidence": "medium",
                "alternatives": ["v2"],
                "parent_result_id": original_id,
                "rerun_seq": 1,
            }
        ],
    )
    assert store.next_rerun_seq(original_id) == 2


def test_get_result_chain_orders_by_rerun_seq(store: SQLiteHistoryStore) -> None:
    run_id, original_id = _seed_original_run(store)
    for seq, alt in enumerate(["v2", "v3", "v4"], start=1):
        store.save_run_results(
            run_id,
            [
                {
                    "schema": "public",
                    "table": "orders",
                    "column": "status",
                    "asset_kind": "column",
                    "source": "rerun",
                    "confidence": "medium",
                    "alternatives": [alt],
                    "parent_result_id": original_id,
                    "rerun_seq": seq,
                }
            ],
        )
    chain = store.get_result_chain(original_id)
    # Original (seq=0) plus three children, total 4 rows ordered by
    # rerun_seq ascending.
    assert [row["rerun_seq"] for row in chain] == [0, 1, 2, 3]


def test_snapshot_lifecycle_save_read_delete(store: SQLiteHistoryStore) -> None:
    _, original_id = _seed_original_run(store)
    store.save_rerun_snapshot(
        snapshot_id="snap-1",
        job_id="job-A",
        target_result_id=original_id,
        payload={"ctx": "frozen"},
    )
    fetched = store.read_rerun_snapshot("snap-1")
    assert fetched is not None
    assert fetched["payload"] == {"ctx": "frozen"}
    assert fetched["target_result_id"] == original_id

    deleted = store.delete_rerun_snapshots_for_job("job-A")
    assert deleted == 1
    assert store.read_rerun_snapshot("snap-1") is None


def test_orphan_snapshot_gc(store: SQLiteHistoryStore) -> None:
    _, original_id = _seed_original_run(store)
    store.save_rerun_snapshot(
        snapshot_id="snap-old",
        job_id="job-stale",
        target_result_id=original_id,
        payload={"x": 1},
    )
    # Backdate the row so the GC sees it as old.
    with store._connect() as conn:
        conn.execute(
            "UPDATE rerun_context_snapshots SET created_at = ? WHERE snapshot_id = ?",
            (time.time() - 7200, "snap-old"),
        )
    store.save_rerun_snapshot(
        snapshot_id="snap-fresh",
        job_id="job-live",
        target_result_id=original_id,
        payload={"x": 2},
    )
    removed = store.gc_orphan_rerun_snapshots(max_age_seconds=3600)
    assert removed == 1
    assert store.read_rerun_snapshot("snap-old") is None
    assert store.read_rerun_snapshot("snap-fresh") is not None


# ── build_context_snapshot guards ───────────────────────────────────────────


def test_build_context_snapshot_unknown_target_raises(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from amx.agents import rerun_context

    s = SQLiteHistoryStore(tmp_path / "h.db")
    s.init()
    monkeypatch.setattr(rerun_context, "history_store", lambda: s)

    cfg = _stub_cfg()
    with pytest.raises(RerunContextError, match="not found"):
        rerun_context.build_context_snapshot(cfg, target_result_id=99999, job_id="job-x")


# ── rerun_items executor (mocked) ───────────────────────────────────────────


def _stub_cfg():
    """Minimal AMXConfig stand-in for executor unit tests.

    The real ``AMXConfig`` reaches into the OS keyring + on-disk yaml
    on construction, which we don't want for a mocked executor test.
    A duck-typed namespace with the fields the executor actually
    reads is enough.
    """
    from types import SimpleNamespace

    llm = SimpleNamespace(
        provider="stub",
        model="stub-1",
        temperature=0.2,
        max_tokens=512,
        logprob_high=0.85,
        logprob_medium=0.5,
    )
    return SimpleNamespace(
        llm=llm,
        active_db_profile="local",
        active_llm_profile="stub-llm",
        active_doc_profile=None,
        active_code_profile=None,
        db_profiles={"local": SimpleNamespace(backend="postgresql")},
    )


def test_rerun_items_writes_versioned_row_and_cleans_snapshots(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Happy path for a single column re-run.

    Mocks every external boundary: ``LLMProvider`` (no network),
    ``ProfileAgent.run`` (returns one fake suggestion), and
    ``build_context_snapshot`` (no DB introspection). Asserts that:

    * the original ``run_results`` row is unchanged;
    * a new row is created with ``parent_result_id`` set + ``rerun_seq=1``;
    * the snapshot is gone after the worker finishes (storage stays
      empty between re-runs).
    """
    from amx.agents import rerun_context as rc_mod
    from amx.agents._orchestrator import rerun as rerun_mod

    s = SQLiteHistoryStore(tmp_path / "h.db")
    s.init()
    run_id, original_id = _seed_original_run(s)

    monkeypatch.setattr(rerun_mod, "history_store", lambda: s)
    monkeypatch.setattr(rc_mod, "history_store", lambda: s)

    # Stub the snapshot builder to bypass live DB profiling.
    def _fake_snapshot(cfg, *, target_result_id, job_id, user_instructions=None):
        snap_id = f"snap-{target_result_id}"
        s.save_rerun_snapshot(
            snapshot_id=snap_id,
            job_id=job_id,
            target_result_id=int(target_result_id),
            payload={
                "schema": "public",
                "table": "orders",
                "column": "status",
                "asset_kind": "column",
                "db_profile": {"columns": [{"name": "status", "dtype": "text"}]},
                "rag_context": [],
                "code_context": [],
                "existing_metadata": {},
                "user_instructions": (user_instructions or "").strip(),
                "original": {"run_id": run_id, "result_id": int(target_result_id)},
            },
        )
        return snap_id

    monkeypatch.setattr(rerun_mod, "build_context_snapshot", _fake_snapshot)

    # Stub LLMProvider so no network call is attempted.
    class _FakeLLM:
        model_name = "stub-1"

        def __init__(self, _cfg):
            self.cfg = _cfg

    monkeypatch.setattr(rerun_mod, "LLMProvider", _FakeLLM)

    # Stub ProfileAgent.run to return one suggestion matching the target.
    captured_ctx = {}

    def _fake_run(self, ctx):
        captured_ctx["last"] = ctx
        return [
            MetadataSuggestion(
                schema=ctx.schema,
                table=ctx.table,
                column=ctx.column,
                suggestions=["v2-alt-1", "v2-alt-2", "v2-alt-3"],
                confidence=Confidence.HIGH,
                reasoning="rerun stub",
                source="rerun",
            )
        ]

    monkeypatch.setattr(rerun_mod.ProfileAgent, "run", _fake_run)

    cfg = _stub_cfg()
    new_run_id, outcomes = rerun_mod.rerun_items(
        cfg,
        target_result_ids=[original_id],
        user_instructions="bias toward soft-delete",
    )

    assert len(outcomes) == 1
    outcome = outcomes[0]
    assert outcome.error is None
    assert outcome.alternatives == ["v2-alt-1", "v2-alt-2", "v2-alt-3"]
    assert outcome.rerun_seq == 1

    # Original row untouched.
    original = s.get_run_result(original_id)
    assert original is not None
    assert original["alternatives_json"] == ["v1-alt-a", "v1-alt-b"]
    assert original["rerun_seq"] == 0

    # New row exists and links back to the original.
    new_row = s.get_run_result(outcome.new_result_id)
    assert new_row is not None
    assert new_row["parent_result_id"] == original_id
    assert new_row["rerun_seq"] == 1
    assert new_row["user_instructions"] == "bias toward soft-delete"

    # Snapshot cleaned up — table is empty even though the executor
    # wrote one snapshot row mid-flight.
    with s._connect() as conn:
        count = conn.execute("SELECT COUNT(*) FROM rerun_context_snapshots").fetchone()[0]
    assert count == 0

    # The agent received the user_instructions in its AgentContext so
    # the prompt-suffix path actually fires.
    assert captured_ctx["last"].user_instructions == "bias toward soft-delete"

    # The new analysis_runs row carries command='rerun' so /history
    # can render it distinctly from analyze.run rows.
    new_run = s.get_run(int(new_run_id))
    assert new_run is not None
    assert new_run["command"] == "rerun"


def test_rerun_items_multi_target_single_job(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Three targets share one analysis_runs parent + one job_id."""
    from amx.agents import rerun_context as rc_mod
    from amx.agents._orchestrator import rerun as rerun_mod

    s = SQLiteHistoryStore(tmp_path / "h.db")
    s.init()

    run_id = s.create_run(
        command="analyze.run",
        mode="chat",
        db_backend="postgresql",
        db_profile="local",
        llm_provider="openai",
        llm_model="gpt-test",
        scope={"public": ["orders", "users", "items"]},
    )
    ids = s.save_run_results(
        run_id,
        [
            {
                "schema": "public",
                "table": "orders",
                "column": "status",
                "asset_kind": "column",
                "source": "llm",
                "confidence": "medium",
                "alternatives": ["a"],
            },
            {
                "schema": "public",
                "table": "users",
                "column": "email",
                "asset_kind": "column",
                "source": "llm",
                "confidence": "medium",
                "alternatives": ["b"],
            },
            {
                "schema": "public",
                "table": "items",
                "column": "name",
                "asset_kind": "column",
                "source": "llm",
                "confidence": "medium",
                "alternatives": ["c"],
            },
        ],
    )

    monkeypatch.setattr(rerun_mod, "history_store", lambda: s)
    monkeypatch.setattr(rc_mod, "history_store", lambda: s)

    def _fake_snapshot(cfg, *, target_result_id, job_id, user_instructions=None):
        snap_id = f"snap-{target_result_id}"
        target = s.get_run_result(int(target_result_id))
        s.save_rerun_snapshot(
            snapshot_id=snap_id,
            job_id=job_id,
            target_result_id=int(target_result_id),
            payload={
                "schema": target["schema_name"],
                "table": target["table_name"],
                "column": target["column_name"],
                "asset_kind": target["asset_kind"],
                "db_profile": {"columns": [{"name": target["column_name"], "dtype": "text"}]},
                "rag_context": [],
                "code_context": [],
                "existing_metadata": {},
                "user_instructions": (user_instructions or "").strip(),
                "original": {
                    "run_id": int(target["run_id"]),
                    "result_id": int(target_result_id),
                },
            },
        )
        return snap_id

    monkeypatch.setattr(rerun_mod, "build_context_snapshot", _fake_snapshot)

    class _FakeLLM:
        model_name = "stub-1"

        def __init__(self, _cfg):
            self.cfg = _cfg

    monkeypatch.setattr(rerun_mod, "LLMProvider", _FakeLLM)
    monkeypatch.setattr(
        rerun_mod.ProfileAgent,
        "run",
        lambda self, ctx: [
            MetadataSuggestion(
                schema=ctx.schema,
                table=ctx.table,
                column=ctx.column,
                suggestions=[f"new-desc-{ctx.column}"],
                confidence=Confidence.MEDIUM,
                reasoning="multi rerun",
                source="rerun",
            )
        ],
    )

    captured_events: list[tuple[str, dict]] = []

    new_run_id, outcomes = rerun_mod.rerun_items(
        _stub_cfg(),
        target_result_ids=ids,
        user_instructions="shared addendum",
        on_event=lambda etype, payload: captured_events.append((etype, payload)),
    )

    assert len(outcomes) == 3
    assert all(o.error is None for o in outcomes)
    assert all(o.rerun_seq == 1 for o in outcomes)

    # All three new rows hang off the same new analysis_runs id.
    for o in outcomes:
        assert s.get_run_result(o.new_result_id)["run_id"] == new_run_id

    # The on_event hook was driven once-per-target with activity events.
    activity_kinds = {e[0] for e in captured_events}
    assert "activity.added" in activity_kinds
    assert "activity.complete" in activity_kinds

    # Snapshot table is empty afterwards.
    with s._connect() as conn:
        count = conn.execute("SELECT COUNT(*) FROM rerun_context_snapshots").fetchone()[0]
    assert count == 0


def test_rerun_runs_rag_agent_when_doc_profile_present(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When the original run captured a doc_profile, the executor opens
    its RAGStore and runs RAGAgent in parallel with ProfileAgent.

    Verified by stubbing ``_try_load_rag_store`` to return a sentinel
    object and asserting RAGAgent was constructed with it. Both agent
    runs are stubbed to return one suggestion each; the merge step
    picks the higher-confidence one and unions the alternatives —
    that's the contract the executor relies on for diversity."""
    from amx.agents._orchestrator import rerun as rerun_mod

    s = SQLiteHistoryStore(tmp_path / "h.db")
    s.init()
    run_id, original_id = _seed_original_run(s)
    # Stamp the parent run with a non-null doc_profile so the executor
    # picks up the "RAG is wired" branch.
    with s._connect() as conn:
        conn.execute(
            "UPDATE analysis_runs SET doc_profile = 'team-handbook' WHERE id = ?",
            (run_id,),
        )

    monkeypatch.setattr(rerun_mod, "history_store", lambda: s)

    sentinel_store = object()
    monkeypatch.setattr(rerun_mod, "_try_load_rag_store", lambda cfg, name: sentinel_store)
    monkeypatch.setattr(rerun_mod, "_try_make_code_report", lambda cfg, name: None)

    captured: dict[str, Any] = {}
    real_rag_init = rerun_mod.RAGAgent.__init__

    def _spy_init(self, llm, store):
        captured["rag_store"] = store
        # Don't actually construct (RAGAgent requires real Chroma deps).
        self.llm = llm
        self.rag = store

    monkeypatch.setattr(rerun_mod.RAGAgent, "__init__", _spy_init)
    monkeypatch.setattr(
        rerun_mod.RAGAgent,
        "run",
        lambda self, ctx: [
            MetadataSuggestion(
                schema=ctx.schema,
                table=ctx.table,
                column=ctx.column,
                suggestions=["from-rag-1", "from-rag-2"],
                confidence=Confidence.HIGH,
                reasoning="rag stub",
                source="rag",
            )
        ],
    )

    def _fake_snapshot(cfg, *, target_result_id, job_id, user_instructions=None):
        snap_id = f"snap-{target_result_id}"
        s.save_rerun_snapshot(
            snapshot_id=snap_id,
            job_id=job_id,
            target_result_id=int(target_result_id),
            payload={
                "schema": "public",
                "table": "orders",
                "column": "status",
                "asset_kind": "column",
                "db_profile": {"columns": [{"name": "status", "dtype": "text"}]},
                "rag_context": [],
                "code_context": [],
                "existing_metadata": {},
                "user_instructions": (user_instructions or "").strip(),
                "original": {
                    "run_id": run_id,
                    "result_id": int(target_result_id),
                    "doc_profile": "team-handbook",
                },
            },
        )
        return snap_id

    monkeypatch.setattr(rerun_mod, "build_context_snapshot", _fake_snapshot)

    class _FakeLLM:
        model_name = "stub-1"

        def __init__(self, _cfg):
            self.cfg = _cfg

    monkeypatch.setattr(rerun_mod, "LLMProvider", _FakeLLM)
    monkeypatch.setattr(
        rerun_mod.ProfileAgent,
        "run",
        lambda self, ctx: [
            MetadataSuggestion(
                schema=ctx.schema,
                table=ctx.table,
                column=ctx.column,
                suggestions=["from-profile-1"],
                confidence=Confidence.MEDIUM,
                reasoning="profile stub",
                source="profile",
            )
        ],
    )

    _, outcomes = rerun_mod.rerun_items(
        _stub_cfg(),
        target_result_ids=[original_id],
    )
    # RAGAgent was constructed with the sentinel store the loader
    # returned — proving the executor flowed through to RAG.
    assert captured["rag_store"] is sentinel_store
    assert len(outcomes) == 1
    assert outcomes[0].error is None
    # The merge step picked the HIGH-confidence RAG suggestion as the
    # primary and unioned alternatives across both agents.
    assert "from-rag-1" in outcomes[0].alternatives
    assert "from-profile-1" in outcomes[0].alternatives
    assert outcomes[0].source == "combined"
    assert outcomes[0].confidence == "high"

    # Restore the real init so subsequent tests get a clean slate.
    monkeypatch.setattr(rerun_mod.RAGAgent, "__init__", real_rag_init)


def test_rerun_runs_code_agent_when_code_profile_present(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Same shape as the RAG test but for the Code agent path.

    Asserts the executor constructs a ``CodebaseReport`` with empty
    ``references`` so :class:`CodeAgent` falls into the semantic-only
    branch — the heavy full-codebase scan is intentionally skipped to
    keep the per-item budget under 20s."""
    from amx.agents._orchestrator import rerun as rerun_mod

    s = SQLiteHistoryStore(tmp_path / "h.db")
    s.init()
    run_id, original_id = _seed_original_run(s)
    with s._connect() as conn:
        conn.execute(
            "UPDATE analysis_runs SET code_profile = 'monorepo' WHERE id = ?",
            (run_id,),
        )

    monkeypatch.setattr(rerun_mod, "history_store", lambda: s)

    fake_report = object()
    monkeypatch.setattr(rerun_mod, "_try_load_rag_store", lambda cfg, name: None)
    monkeypatch.setattr(rerun_mod, "_try_make_code_report", lambda cfg, name: fake_report)

    captured: dict[str, Any] = {}
    real_init = rerun_mod.CodeAgent.__init__

    def _spy_init(self, llm, report=None):
        captured["report"] = report
        self.llm = llm
        self.report = report

    monkeypatch.setattr(rerun_mod.CodeAgent, "__init__", _spy_init)
    monkeypatch.setattr(
        rerun_mod.CodeAgent,
        "run",
        lambda self, ctx: [
            MetadataSuggestion(
                schema=ctx.schema,
                table=ctx.table,
                column=ctx.column,
                suggestions=["from-code"],
                confidence=Confidence.LOW,
                reasoning="code stub",
                source="code",
            )
        ],
    )

    def _fake_snapshot(cfg, *, target_result_id, job_id, user_instructions=None):
        snap_id = f"snap-{target_result_id}"
        s.save_rerun_snapshot(
            snapshot_id=snap_id,
            job_id=job_id,
            target_result_id=int(target_result_id),
            payload={
                "schema": "public",
                "table": "orders",
                "column": "status",
                "asset_kind": "column",
                "db_profile": {"columns": [{"name": "status", "dtype": "text"}]},
                "rag_context": [],
                "code_context": [],
                "existing_metadata": {},
                "user_instructions": "",
                "original": {
                    "run_id": run_id,
                    "result_id": int(target_result_id),
                    "code_profile": "monorepo",
                },
            },
        )
        return snap_id

    monkeypatch.setattr(rerun_mod, "build_context_snapshot", _fake_snapshot)

    class _FakeLLM:
        model_name = "stub-1"

        def __init__(self, _cfg):
            self.cfg = _cfg

    monkeypatch.setattr(rerun_mod, "LLMProvider", _FakeLLM)
    monkeypatch.setattr(
        rerun_mod.ProfileAgent,
        "run",
        lambda self, ctx: [
            MetadataSuggestion(
                schema=ctx.schema,
                table=ctx.table,
                column=ctx.column,
                suggestions=["from-profile"],
                confidence=Confidence.MEDIUM,
                reasoning="profile stub",
                source="profile",
            )
        ],
    )

    _, outcomes = rerun_mod.rerun_items(
        _stub_cfg(),
        target_result_ids=[original_id],
    )
    assert captured["report"] is fake_report
    assert outcomes[0].error is None
    # MEDIUM-confidence Profile beats LOW-confidence Code.
    assert outcomes[0].confidence == "medium"
    assert "from-profile" in outcomes[0].alternatives
    assert "from-code" in outcomes[0].alternatives

    monkeypatch.setattr(rerun_mod.CodeAgent, "__init__", real_init)


def test_rerun_skips_rag_when_no_doc_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """No doc_profile + no code_profile = Profile-only fan-out (back to v1).

    Belt-and-suspenders: even if the loaders are mocked permissive,
    an executor that uses only the original run's profile names will
    skip both agents because the seed run sets neither."""
    from amx.agents._orchestrator import rerun as rerun_mod

    s = SQLiteHistoryStore(tmp_path / "h.db")
    s.init()
    run_id, original_id = _seed_original_run(s)
    monkeypatch.setattr(rerun_mod, "history_store", lambda: s)

    rag_calls: list[Any] = []
    code_calls: list[Any] = []
    monkeypatch.setattr(
        rerun_mod,
        "_try_load_rag_store",
        lambda cfg, name: rag_calls.append(name) or None,
    )
    monkeypatch.setattr(
        rerun_mod,
        "_try_make_code_report",
        lambda cfg, name: code_calls.append(name) or None,
    )

    def _fake_snapshot(cfg, *, target_result_id, job_id, user_instructions=None):
        snap_id = f"snap-{target_result_id}"
        s.save_rerun_snapshot(
            snapshot_id=snap_id,
            job_id=job_id,
            target_result_id=int(target_result_id),
            payload={
                "schema": "public",
                "table": "orders",
                "column": "status",
                "asset_kind": "column",
                "db_profile": {"columns": [{"name": "status", "dtype": "text"}]},
                "rag_context": [],
                "code_context": [],
                "existing_metadata": {},
                "user_instructions": "",
                "original": {
                    "run_id": run_id,
                    "result_id": int(target_result_id),
                    "doc_profile": None,
                    "code_profile": None,
                },
            },
        )
        return snap_id

    monkeypatch.setattr(rerun_mod, "build_context_snapshot", _fake_snapshot)

    class _FakeLLM:
        model_name = "stub-1"

        def __init__(self, _cfg):
            self.cfg = _cfg

    monkeypatch.setattr(rerun_mod, "LLMProvider", _FakeLLM)
    monkeypatch.setattr(
        rerun_mod.ProfileAgent,
        "run",
        lambda self, ctx: [
            MetadataSuggestion(
                schema=ctx.schema,
                table=ctx.table,
                column=ctx.column,
                suggestions=["only-profile"],
                confidence=Confidence.MEDIUM,
                reasoning="profile stub",
                source="profile",
            )
        ],
    )

    _, outcomes = rerun_mod.rerun_items(
        _stub_cfg(),
        target_result_ids=[original_id],
    )
    # Loaders queried with None — both returned None, both agents skipped.
    assert rag_calls == [None]
    assert code_calls == [None]
    assert outcomes[0].alternatives == ["only-profile"]


def test_rerun_warns_when_per_item_budget_exceeded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The 20s soft cap fires a WARNING log when an item runs long.

    Real budget is 20s; for the test we override it to 0.0 so any
    non-trivial run trips the threshold. The behavior under test is
    purely the *warning* — the executor keeps the result, since
    LLM HTTP calls aren't cancellable from inside the agent."""
    import logging

    from amx.agents._orchestrator import rerun as rerun_mod

    s = SQLiteHistoryStore(tmp_path / "h.db")
    s.init()
    run_id, original_id = _seed_original_run(s)
    monkeypatch.setattr(rerun_mod, "history_store", lambda: s)
    monkeypatch.setattr(rerun_mod, "_try_load_rag_store", lambda cfg, name: None)
    monkeypatch.setattr(rerun_mod, "_try_make_code_report", lambda cfg, name: None)
    monkeypatch.setattr(rerun_mod, "RERUN_PER_ITEM_BUDGET_SEC", 0.0)

    def _fake_snapshot(cfg, *, target_result_id, job_id, user_instructions=None):
        snap_id = f"snap-{target_result_id}"
        s.save_rerun_snapshot(
            snapshot_id=snap_id,
            job_id=job_id,
            target_result_id=int(target_result_id),
            payload={
                "schema": "public",
                "table": "orders",
                "column": "status",
                "asset_kind": "column",
                "db_profile": {"columns": [{"name": "status", "dtype": "text"}]},
                "rag_context": [],
                "code_context": [],
                "existing_metadata": {},
                "user_instructions": "",
                "original": {"run_id": run_id, "result_id": int(target_result_id)},
            },
        )
        return snap_id

    monkeypatch.setattr(rerun_mod, "build_context_snapshot", _fake_snapshot)

    class _FakeLLM:
        model_name = "stub-1"

        def __init__(self, _cfg):
            self.cfg = _cfg

    monkeypatch.setattr(rerun_mod, "LLMProvider", _FakeLLM)
    monkeypatch.setattr(
        rerun_mod.ProfileAgent,
        "run",
        lambda self, ctx: [
            MetadataSuggestion(
                schema=ctx.schema,
                table=ctx.table,
                column=ctx.column,
                suggestions=["x"],
                confidence=Confidence.MEDIUM,
                reasoning="",
                source="profile",
            )
        ],
    )

    with caplog.at_level(logging.WARNING, logger="amx.agents._orchestrator.rerun"):
        rerun_mod.rerun_items(_stub_cfg(), target_result_ids=[original_id])
    assert any(
        "soft budget" in record.message and "0s soft budget" in record.message
        for record in caplog.records
    )


def test_rerun_items_cleans_up_snapshots_on_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """If the agent raises, the executor still deletes its snapshots."""
    from amx.agents import rerun_context as rc_mod
    from amx.agents._orchestrator import rerun as rerun_mod

    s = SQLiteHistoryStore(tmp_path / "h.db")
    s.init()
    _, original_id = _seed_original_run(s)

    monkeypatch.setattr(rerun_mod, "history_store", lambda: s)
    monkeypatch.setattr(rc_mod, "history_store", lambda: s)

    monkeypatch.setattr(
        rerun_mod,
        "build_context_snapshot",
        lambda cfg, *, target_result_id, job_id, user_instructions=None: (
            s.save_rerun_snapshot(
                snapshot_id=f"snap-{target_result_id}",
                job_id=job_id,
                target_result_id=int(target_result_id),
                payload={
                    "schema": "public",
                    "table": "orders",
                    "column": "status",
                    "asset_kind": "column",
                    "db_profile": {"columns": [{"name": "status", "dtype": "text"}]},
                    "rag_context": [],
                    "code_context": [],
                    "existing_metadata": {},
                    "user_instructions": "",
                    "original": {"run_id": 0, "result_id": int(target_result_id)},
                },
            )
            or f"snap-{target_result_id}"
        ),
    )

    class _FakeLLM:
        model_name = "stub-1"

        def __init__(self, _cfg):
            self.cfg = _cfg

    monkeypatch.setattr(rerun_mod, "LLMProvider", _FakeLLM)

    def _explode(self, ctx):
        raise RuntimeError("agent boom")

    monkeypatch.setattr(rerun_mod.ProfileAgent, "run", _explode)

    _, outcomes = rerun_mod.rerun_items(
        _stub_cfg(),
        target_result_ids=[original_id],
    )
    assert len(outcomes) == 1
    # The parallel fan-out swallows per-agent crashes (RAG / Code can
    # fail without killing Profile output); when *every* sub-agent
    # ends up empty the executor surfaces a generic "no parseable
    # description" error instead of the agent's stack trace.
    assert outcomes[0].error is not None
    assert outcomes[0].new_result_id == 0

    with s._connect() as conn:
        count = conn.execute("SELECT COUNT(*) FROM rerun_context_snapshots").fetchone()[0]
    assert count == 0


# ── run_context_cache (P4: skip live profile_table on re-run) ──


def test_save_and_lookup_run_context_cache_round_trips(
    store: SQLiteHistoryStore,
) -> None:
    """Round-trip the cache key + payload."""
    payload = {
        "db_profile": {
            "row_count": 42,
            "columns": [{"name": "id"}, {"name": "status"}],
        },
        "existing_metadata": {"database": "shop"},
    }
    store.save_run_context_cache(
        db_profile="prod_pg",
        database="shop",
        schema="public",
        table="orders",
        payload=payload,
        source_run_id=7,
    )
    hit = store.lookup_run_context_cache(
        db_profile="prod_pg",
        database="shop",
        schema="public",
        table="orders",
    )
    assert hit is not None
    assert hit["payload"] == payload
    assert hit["source_run_id"] == 7


def test_lookup_run_context_cache_misses_on_different_table(
    store: SQLiteHistoryStore,
) -> None:
    """Wrong (schema, table) coordinates return ``None`` not the wrong row."""
    store.save_run_context_cache(
        db_profile="prod_pg",
        database="shop",
        schema="public",
        table="orders",
        payload={"db_profile": {}, "existing_metadata": {}},
    )
    miss = store.lookup_run_context_cache(
        db_profile="prod_pg",
        database="shop",
        schema="public",
        table="customers",
    )
    assert miss is None


def test_lookup_run_context_cache_treats_expired_as_missing(
    store: SQLiteHistoryStore,
) -> None:
    """A row past ``expires_at`` reads back as ``None`` so callers rebuild."""
    store.save_run_context_cache(
        db_profile="prod_pg",
        database="",
        schema="public",
        table="orders",
        payload={"db_profile": {}, "existing_metadata": {}},
        ttl_seconds=0.001,
    )
    import time as _time

    _time.sleep(0.01)
    miss = store.lookup_run_context_cache(
        db_profile="prod_pg",
        database="",
        schema="public",
        table="orders",
    )
    assert miss is None


def test_delete_run_context_cache_drops_row(
    store: SQLiteHistoryStore,
) -> None:
    """The apply path calls this once a COMMENT writes successfully."""
    store.save_run_context_cache(
        db_profile="prod_pg",
        database="shop",
        schema="public",
        table="orders",
        payload={"db_profile": {}, "existing_metadata": {}},
    )
    deleted = store.delete_run_context_cache(
        db_profile="prod_pg",
        database="shop",
        schema="public",
        table="orders",
    )
    assert deleted == 1
    assert (
        store.lookup_run_context_cache(
            db_profile="prod_pg",
            database="shop",
            schema="public",
            table="orders",
        )
        is None
    )


def test_save_run_context_cache_replaces_on_conflict(
    store: SQLiteHistoryStore,
) -> None:
    """Re-analyzing the same table refreshes -- does not duplicate -- the row."""
    first_payload = {"db_profile": {"row_count": 1}, "existing_metadata": {}}
    second_payload = {"db_profile": {"row_count": 999}, "existing_metadata": {}}
    for payload in (first_payload, second_payload):
        store.save_run_context_cache(
            db_profile="prod_pg",
            database="",
            schema="public",
            table="orders",
            payload=payload,
        )
    hit = store.lookup_run_context_cache(
        db_profile="prod_pg",
        database="",
        schema="public",
        table="orders",
    )
    assert hit is not None
    assert hit["payload"] == second_payload
    with store._connect() as conn:
        rows = conn.execute(
            "SELECT COUNT(*) FROM run_context_cache "
            "WHERE db_profile=? AND schema_name=? AND table_name=?",
            ("prod_pg", "public", "orders"),
        ).fetchone()
    assert rows[0] == 1


def test_gc_run_context_cache_reaps_only_expired_rows(
    store: SQLiteHistoryStore,
) -> None:
    """Long-TTL rows survive GC; expired rows are dropped."""
    store.save_run_context_cache(
        db_profile="prod_pg",
        database="",
        schema="public",
        table="orders",
        payload={"db_profile": {}, "existing_metadata": {}},
        ttl_seconds=0.001,
    )
    store.save_run_context_cache(
        db_profile="prod_pg",
        database="",
        schema="public",
        table="customers",
        payload={"db_profile": {}, "existing_metadata": {}},
        ttl_seconds=86400,
    )
    import time as _time

    _time.sleep(0.01)
    deleted = store.gc_run_context_cache()
    assert deleted == 1
    survivors = store.lookup_run_context_cache(
        db_profile="prod_pg",
        database="",
        schema="public",
        table="customers",
    )
    assert survivors is not None


# ── Re-Run modal Advanced parity (PR: rerun-modal-parity) ─────────────
#
# Pins the new ``llm_overrides`` block on ``rerun_items``. The Re-Run
# modal in Studio now mounts the same ``AdvancedLLMOverrides`` form as
# RunNew, so every knob in ``LLMOverrides`` (alternatives_mode,
# confidence_signal, max_tokens, …) must flow through the rerun path
# end-to-end. The CLI ``/rerun --temperature`` shim keeps working as
# the lone single-knob exception.


def _real_cfg_with_llm() -> Any:
    """A *real* AMXConfig + LLMConfig so the dataclasses.replace path in
    ``_llm_for_rerun`` actually has dataclass instances to replace.

    The ``_stub_cfg()`` SimpleNamespace shortcut works for the legacy
    code path (no overrides → ``dataclasses.replace`` never called) but
    blows up the moment we exercise the override path. These tests use
    real dataclasses so the replace machinery is genuinely covered.
    """
    from amx.config import AMXConfig as _AMXConfig
    from amx.config import LLMConfig as _LLMConfig

    cfg = _AMXConfig()
    cfg.llm = _LLMConfig(
        provider="stub",
        model="stub-1",
        temperature=0.2,
        max_tokens=512,
        n_alternatives=3,
        logprob_high=0.85,
        logprob_medium=0.50,
    )
    cfg.active_llm_profile = "stub-llm"
    return cfg


def _wire_rerun_stubs(
    monkeypatch: pytest.MonkeyPatch,
    store: SQLiteHistoryStore,
) -> dict[str, Any]:
    """Common fixture wiring for the override-path tests."""
    from amx.agents import rerun_context as rc_mod
    from amx.agents._orchestrator import rerun as rerun_mod

    monkeypatch.setattr(rerun_mod, "history_store", lambda: store)
    monkeypatch.setattr(rc_mod, "history_store", lambda: store)

    def _fake_snapshot(cfg, *, target_result_id, job_id, user_instructions=None):
        snap_id = f"snap-{target_result_id}"
        store.save_rerun_snapshot(
            snapshot_id=snap_id,
            job_id=job_id,
            target_result_id=int(target_result_id),
            payload={
                "schema": "public",
                "table": "orders",
                "column": "status",
                "asset_kind": "column",
                "db_profile": {"columns": [{"name": "status", "dtype": "text"}]},
                "rag_context": [],
                "code_context": [],
                "existing_metadata": {},
                "user_instructions": (user_instructions or "").strip(),
                "original": {"run_id": 0, "result_id": int(target_result_id)},
            },
        )
        return snap_id

    monkeypatch.setattr(rerun_mod, "build_context_snapshot", _fake_snapshot)

    seen_llm_cfgs: list[Any] = []

    class _FakeLLM:
        model_name = "stub-1"

        def __init__(self, _cfg):
            self.cfg = _cfg
            seen_llm_cfgs.append(_cfg)

    monkeypatch.setattr(rerun_mod, "LLMProvider", _FakeLLM)

    def _fake_run(self, ctx):
        return [
            MetadataSuggestion(
                schema=ctx.schema,
                table=ctx.table,
                column=ctx.column,
                suggestions=["v2-alt-1", "v2-alt-2", "v2-alt-3"],
                confidence=Confidence.HIGH,
                reasoning="rerun stub",
                source="rerun",
            )
        ]

    monkeypatch.setattr(rerun_mod.ProfileAgent, "run", _fake_run)
    return {"seen_llm_cfgs": seen_llm_cfgs}


def test_rerun_walks_all_overrides(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Every LLMOverrides field reaches the LLMConfig the agents see.

    Sends a full overrides dict and asserts the seeded ``LLMProvider``
    was constructed against a derived ``LLMConfig`` carrying each value.
    """
    from amx.agents._orchestrator import rerun as rerun_mod

    s = SQLiteHistoryStore(tmp_path / "h.db")
    s.init()
    _, original_id = _seed_original_run(s)
    state = _wire_rerun_stubs(monkeypatch, s)

    cfg = _real_cfg_with_llm()
    rerun_mod.rerun_items(
        cfg,
        target_result_ids=[original_id],
        llm_overrides={
            "temperature": 0.9,
            "max_tokens": 4096,
            "n_alternatives": 5,
            "prompt_detail": "full",
            "description_verbosity": "exhaustive",
            "confidence_signal": "judge",
            "alternatives_mode": "lexical",
            "thinking_budget": 8192,
            "logprob_high": 0.9,
            "logprob_medium": 0.6,
        },
    )

    used = state["seen_llm_cfgs"][-1]
    assert used.temperature == 0.9
    assert used.max_tokens == 4096
    assert used.n_alternatives == 5
    assert used.prompt_detail == "full"
    assert used.description_verbosity == "exhaustive"
    assert used.confidence_signal == "judge"
    assert used.alternatives_mode == "lexical"
    assert used.thinking_budget == 8192
    assert used.logprob_high == 0.9
    assert used.logprob_medium == 0.6


def test_rerun_profile_unchanged_after_rerun(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The saved LLM profile instance on ``cfg.llm`` is byte-identical
    before and after the rerun, regardless of overrides. Proves the
    immutable ``dataclasses.replace`` path doesn't leak."""
    from amx.agents._orchestrator import rerun as rerun_mod

    s = SQLiteHistoryStore(tmp_path / "h.db")
    s.init()
    _, original_id = _seed_original_run(s)
    _wire_rerun_stubs(monkeypatch, s)

    cfg = _real_cfg_with_llm()
    saved_profile = cfg.llm
    before_snapshot = (
        cfg.llm.temperature,
        cfg.llm.alternatives_mode,
        cfg.llm.confidence_signal,
        cfg.llm.n_alternatives,
    )

    rerun_mod.rerun_items(
        cfg,
        target_result_ids=[original_id],
        llm_overrides={
            "temperature": 0.99,
            "alternatives_mode": "lexical",
            "confidence_signal": "judge",
            "n_alternatives": 5,
        },
    )

    # ``cfg.llm`` may be replaced with the derived dataclass internally
    # for the duration of the run, but the *original* profile instance
    # held by the caller stays untouched — that's the contract.
    after_snapshot = (
        saved_profile.temperature,
        saved_profile.alternatives_mode,
        saved_profile.confidence_signal,
        saved_profile.n_alternatives,
    )
    assert after_snapshot == before_snapshot


def test_rerun_batch_applies_overrides_to_all_items(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A multi-target rerun applies the same overrides to every target."""
    from amx.agents._orchestrator import rerun as rerun_mod

    s = SQLiteHistoryStore(tmp_path / "h.db")
    s.init()
    # Seed two separate originals so we exercise the batch path.
    _, original_a = _seed_original_run(s)
    _, original_b = _seed_original_run(s)
    state = _wire_rerun_stubs(monkeypatch, s)

    cfg = _real_cfg_with_llm()
    new_run_id, outcomes = rerun_mod.rerun_items(
        cfg,
        target_result_ids=[original_a, original_b],
        llm_overrides={
            "alternatives_mode": "lexical",
            "confidence_signal": "judge",
        },
    )

    assert len(outcomes) == 2
    # Both new rows write under the same derived cfg ⇒ same mode.
    for outcome in outcomes:
        new_row = s.get_run_result(outcome.new_result_id)
        assert new_row is not None
        assert new_row["alternatives_mode"] == "lexical"

    # The LLMProvider sees the derived cfg once (Studio shares the
    # provider across the batch); confidence_signal lands on the
    # underlying cfg passed at construction.
    used = state["seen_llm_cfgs"][-1]
    assert used.alternatives_mode == "lexical"
    assert used.confidence_signal == "judge"


def test_rerun_alternatives_mode_skipped_when_n_is_1(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When the override sets ``n_alternatives=1``, an empty
    ``alternatives_mode`` is not forced — the caller decides whether to
    emit one. This pins the backend's permissive contract: the
    front-end disables the row, so the backend should never inject a
    bogus value of its own when N=1."""
    from amx.agents._orchestrator import rerun as rerun_mod

    s = SQLiteHistoryStore(tmp_path / "h.db")
    s.init()
    _, original_id = _seed_original_run(s)
    state = _wire_rerun_stubs(monkeypatch, s)

    cfg = _real_cfg_with_llm()
    cfg.llm.alternatives_mode = "semantic"
    rerun_mod.rerun_items(
        cfg,
        target_result_ids=[original_id],
        llm_overrides={"n_alternatives": 1},  # no alternatives_mode key
    )

    used = state["seen_llm_cfgs"][-1]
    assert used.n_alternatives == 1
    # alternatives_mode inherits the profile default; backend doesn't
    # force a particular value when the override omits it.
    assert used.alternatives_mode == "semantic"


def test_rerun_temperature_override_legacy_back_compat(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The legacy single-knob ``temperature_override`` field still
    drives the derived cfg's temperature when sent alone — the path
    every in-flight Studio bundle + the CLI ``/rerun --temperature``
    flag relies on."""
    from amx.agents._orchestrator import rerun as rerun_mod

    s = SQLiteHistoryStore(tmp_path / "h.db")
    s.init()
    _, original_id = _seed_original_run(s)
    state = _wire_rerun_stubs(monkeypatch, s)

    cfg = _real_cfg_with_llm()
    rerun_mod.rerun_items(
        cfg,
        target_result_ids=[original_id],
        temperature_override=0.7,
    )

    used = state["seen_llm_cfgs"][-1]
    assert used.temperature == 0.7
    # All other knobs inherit the profile.
    assert used.alternatives_mode == "semantic"
    assert used.n_alternatives == 3


def test_rerun_settings_json_records_effective_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The new rerun's ``analysis_runs.settings_json`` records the
    *effective* (post-override) LLM config so ``/history show <run>``
    reports what actually ran."""
    from amx.agents._orchestrator import rerun as rerun_mod

    s = SQLiteHistoryStore(tmp_path / "h.db")
    s.init()
    _, original_id = _seed_original_run(s)
    _wire_rerun_stubs(monkeypatch, s)

    cfg = _real_cfg_with_llm()
    new_run_id, _ = rerun_mod.rerun_items(
        cfg,
        target_result_ids=[original_id],
        llm_overrides={
            "alternatives_mode": "lexical",
            "confidence_signal": "judge",
            "temperature": 0.45,
        },
    )

    new_run = s.get_run(int(new_run_id))
    assert new_run is not None
    settings = new_run.get("settings_json") or new_run.get("settings")
    if isinstance(settings, str):
        import json as _json

        settings = _json.loads(settings)
    assert isinstance(settings, dict)
    assert settings.get("alternatives_mode") == "lexical"
    assert settings.get("confidence_signal") == "judge"
    assert float(settings.get("temperature") or 0) == 0.45
    # ``llm_overrides`` records ONLY the fields the user overrode so a
    # reviewer can tell at a glance what was changed vs inherited.
    overrides = settings.get("llm_overrides") or {}
    assert overrides.get("alternatives_mode") == "lexical"
    assert overrides.get("confidence_signal") == "judge"
    assert float(overrides.get("temperature") or 0) == 0.45
