"""Tests for /api/generate/* — single-shot description endpoints.

These cover the user-configurable knobs (n_alternatives, verbosity,
temperature, prompt_detail) being threaded through to the LLM call
and the resulting alternatives surviving the persistence path
(history store + pending queue).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock

import pytest

from amx.config import AMXConfig
from amx.db.connector import ColumnProfile, DatabaseConnector, TableProfile
from amx.llm.provider import ChatResult
from amx.web.routers import generate as generate_router


class FakeHistoryStore:
    """Tiny in-memory stand-in for ``HistoryStore``.

    Captures the rows save_run_results would persist so tests can
    assert that ``alternatives_json`` carries every alternative the
    LLM produced.
    """

    def __init__(self) -> None:
        self.runs: list[dict[str, Any]] = []
        self.run_results: dict[int, dict[str, Any]] = {}
        self._next_run_id = 0
        self._next_result_id = 0

    def create_run(self, **kwargs: Any) -> int:
        self._next_run_id += 1
        self.runs.append({"id": self._next_run_id, **kwargs})
        return self._next_run_id

    def save_run_results(self, run_id: int, rows: list[dict[str, Any]]) -> list[int]:
        ids: list[int] = []
        for row in rows:
            self._next_result_id += 1
            self.run_results[self._next_result_id] = {
                "run_id": run_id,
                "alternatives_json": list(row.get("alternatives") or []),
                **row,
            }
            ids.append(self._next_result_id)
        return ids

    def update_run_status(self, run_id: int, status: str) -> None:
        for r in self.runs:
            if r["id"] == run_id:
                r["status"] = status
                return

    def finish_run(self, run_id: int, **kwargs: Any) -> None:
        """Capture the tokens / metrics / status payload the
        generate.* endpoints now write so tests can assert that
        ``tokens_json`` carries USD cost + per-step records."""
        for r in self.runs:
            if r["id"] == run_id:
                r["status"] = kwargs.get("status", r.get("status"))
                r["finish_metrics"] = kwargs.get("metrics") or {}
                r["finish_tokens"] = kwargs.get("tokens") or {}
                r["finish_results"] = kwargs.get("results") or {}
                r["finish_error_text"] = kwargs.get("error_text", "")
                return

    def fetch_run_result(self, result_id: int) -> dict[str, Any] | None:
        return self.run_results.get(result_id)


class FakeConnector:
    """Stand-in for ``DatabaseConnector`` shaped just enough for the
    four prompt builders. Returns deterministic metadata for every
    accessor so a single-shot endpoint always has data to fold into
    the prompt.
    """

    backend = "duckdb"

    def __init__(self) -> None:
        self.profile_table_calls: list[tuple[str, str]] = []

    def list_schemas(self) -> list[str]:
        return ["sales", "inventory"]

    def list_assets(self, schema: str) -> list[tuple[str, Any]]:
        return [("orders", "table"), ("customers", "table")]

    def list_column_profiles(self, schema: str, table: str) -> list[ColumnProfile]:
        return [
            ColumnProfile(name="id", dtype="INTEGER", nullable=False),
            ColumnProfile(name="customer_id", dtype="INTEGER", nullable=True),
            ColumnProfile(name="amount", dtype="DECIMAL", nullable=True),
        ]

    def get_database_comment(self) -> str | None:
        return "Operational data store for the demo shop."

    def get_schema_comment(self, schema: str) -> str | None:
        return f"{schema} domain artefacts." if schema in ("sales", "inventory") else None

    def get_table_comment(self, schema: str, table: str) -> str | None:
        return f"{schema}.{table} business records."

    def profile_table(self, schema: str, table: str) -> TableProfile:
        self.profile_table_calls.append((schema, table))
        return TableProfile(
            schema=schema,
            name=table,
            row_count=1234,
            columns=[
                ColumnProfile(
                    name="id",
                    dtype="INTEGER",
                    nullable=False,
                    row_count=1234,
                    null_count=0,
                    distinct_count=1234,
                    cardinality_ratio=1.0,
                    min_val=1,
                    max_val=1234,
                    samples=[1, 2, 3, 4, 5],
                ),
                ColumnProfile(
                    name="customer_id",
                    dtype="INTEGER",
                    nullable=True,
                    row_count=1234,
                    null_count=12,
                    distinct_count=300,
                    cardinality_ratio=0.24,
                    min_val=1,
                    max_val=400,
                    samples=[42, 7, 99],
                ),
                ColumnProfile(
                    name="amount",
                    dtype="DECIMAL",
                    nullable=True,
                    row_count=1234,
                    null_count=4,
                    distinct_count=900,
                    cardinality_ratio=0.73,
                    min_val=10,
                    max_val=9999,
                    samples=[10, 250, 9999],
                ),
            ],
            primary_key=["id"],
            foreign_keys=[{"constrained_columns": ["customer_id"], "referred_table": "customers"}],
            unique_constraints=[["id"]],
            stats_seq_scan=12,
            stats_idx_scan=400,
            stats_n_live_tup=1234,
        )


@pytest.fixture()
def chat_spy() -> MagicMock:
    """Mock ``LLMProvider.chat``; tests assert on its call args."""
    spy = MagicMock(return_value=ChatResult(content="A single description sentence."))
    return spy


@pytest.fixture()
def fake_db() -> FakeConnector:
    return FakeConnector()


@pytest.fixture()
def fake_history() -> FakeHistoryStore:
    return FakeHistoryStore()


@pytest.fixture(autouse=True)
def patch_router_deps(
    monkeypatch: pytest.MonkeyPatch,
    chat_spy: MagicMock,
    fake_db: FakeConnector,
    fake_history: FakeHistoryStore,
    tmp_path,
) -> Iterator[None]:
    """Bypass the real LLMProvider + connector + history store + pending file.

    ``_resolve_generate_connector`` returns our :class:`FakeConnector`
    plus profile / backend labels. ``LLMProvider`` is faked via a
    minimal stub whose ``chat`` is the shared spy — tests can inspect
    or reconfigure ``return_value`` per-case. The history store is
    swapped for an in-memory :class:`FakeHistoryStore` and the pending
    queue lands in a temp file so concurrent test runs don't stomp the
    user's real ``~/.amx/pending_metadata.json``.
    """

    def fake_resolve(
        cfg: AMXConfig,
        profile: str,
        database: str | None,
        catalog: str | None,
    ) -> tuple[DatabaseConnector, str, str]:
        return fake_db, profile, "duckdb"  # type: ignore[return-value]

    class FakeLLM:
        def __init__(self, _llmcfg: Any) -> None:
            pass

        def chat(self, **kwargs: Any) -> ChatResult:
            return chat_spy(**kwargs)

    monkeypatch.setattr(generate_router, "_resolve_generate_connector", fake_resolve)
    monkeypatch.setattr(generate_router, "LLMProvider", FakeLLM)
    monkeypatch.setattr(generate_router, "history_store", lambda: fake_history)

    pending_file = tmp_path / "pending_metadata.json"
    monkeypatch.setattr("amx.pending_review.PENDING_FILE", pending_file)

    yield


# ── helpers ────────────────────────────────────────────────────────────


def _post(client, path: str, headers: dict[str, str]) -> Any:
    return client.post(f"{path}?profile=demo", headers=headers)


def _system_prompt(chat_spy: MagicMock) -> str:
    msgs = chat_spy.call_args.kwargs["messages"]
    return next(m["content"] for m in msgs if m["role"] == "system")


def _user_prompt(chat_spy: MagicMock) -> str:
    msgs = chat_spy.call_args.kwargs["messages"]
    return next(m["content"] for m in msgs if m["role"] == "user")


# ── baseline (regression): N=1, brief, t=0.2, standard ────────────────


def test_database_baseline_returns_single_alternative(
    cfg: AMXConfig, client, auth_headers, chat_spy: MagicMock
) -> None:
    cfg.llm.n_alternatives = 1
    cfg.llm.description_verbosity = "brief"
    cfg.llm.temperature = 0.2

    resp = _post(client, "/api/generate/database", auth_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["description"] == "A single description sentence."
    assert body["alternatives_count"] == 1
    assert body["verbosity"] == "brief"
    assert body["run_id"] is not None

    # System prompt should not include the multi-alternative scaffolding.
    sys_prompt = _system_prompt(chat_spy)
    assert "DESCRIPTION_2" not in sys_prompt
    assert "1-2 sentences" in sys_prompt  # brief length-rule signature
    # Temperature passes through unchanged.
    assert chat_spy.call_args.kwargs["temperature"] == pytest.approx(0.2)


# ── n_alternatives ────────────────────────────────────────────────────


def test_database_n3_parses_three_alternatives(
    cfg: AMXConfig, client, auth_headers, chat_spy: MagicMock
) -> None:
    cfg.llm.n_alternatives = 3
    cfg.llm.description_verbosity = "brief"
    chat_spy.return_value = ChatResult(
        content=(
            "DESCRIPTION_1: Operational order data store.\n"
            "DESCRIPTION_2: Transactional sales records database.\n"
            "DESCRIPTION_3: Source-of-truth for the demo shop's orders.\n"
        )
    )

    resp = _post(client, "/api/generate/database", auth_headers)
    body = resp.json()
    assert body["alternatives_count"] == 3
    assert body["description"] == "Operational order data store."
    sys_prompt = _system_prompt(chat_spy)
    assert "Provide exactly 3 alternative descriptions" in sys_prompt
    assert "DESCRIPTION_3" in sys_prompt


def test_n3_with_malformed_response_keeps_one_alternative(
    cfg: AMXConfig, client, auth_headers, chat_spy: MagicMock
) -> None:
    cfg.llm.n_alternatives = 3
    chat_spy.return_value = ChatResult(content="A plain blob with no DESCRIPTION_ labels.")

    resp = _post(client, "/api/generate/database", auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["alternatives_count"] == 1
    assert body["description"] == "A plain blob with no DESCRIPTION_ labels."


def test_n_clamped_to_max_5(cfg: AMXConfig, client, auth_headers, chat_spy: MagicMock) -> None:
    cfg.llm.n_alternatives = 99  # config drift; expect clamp
    chat_spy.return_value = ChatResult(
        content="\n".join(f"DESCRIPTION_{i}: Alt {i}" for i in range(1, 8))
    )

    resp = _post(client, "/api/generate/database", auth_headers)
    body = resp.json()
    assert body["alternatives_count"] == 5  # parser caps at requested N=5
    sys_prompt = _system_prompt(chat_spy)
    assert "DESCRIPTION_5" in sys_prompt
    assert "DESCRIPTION_6" not in sys_prompt


def test_empty_response_returns_502(
    cfg: AMXConfig, client, auth_headers, chat_spy: MagicMock
) -> None:
    chat_spy.return_value = ChatResult(content="")
    resp = _post(client, "/api/generate/database", auth_headers)
    assert resp.status_code == 502


# ── verbosity ─────────────────────────────────────────────────────────


def test_detailed_verbosity_threads_through_system_prompt(
    cfg: AMXConfig, client, auth_headers, chat_spy: MagicMock
) -> None:
    cfg.llm.description_verbosity = "detailed"
    _post(client, "/api/generate/database", auth_headers)
    sys_prompt = _system_prompt(chat_spy)
    assert "DETAILED description" in sys_prompt
    assert "2-4 sentences" in sys_prompt


def test_invalid_verbosity_falls_back_to_brief(
    cfg: AMXConfig, client, auth_headers, chat_spy: MagicMock
) -> None:
    cfg.llm.description_verbosity = "bogus"
    _post(client, "/api/generate/database", auth_headers)
    sys_prompt = _system_prompt(chat_spy)
    assert "1-2 sentences" in sys_prompt


# ── temperature ───────────────────────────────────────────────────────


def test_temperature_passes_through(
    cfg: AMXConfig, client, auth_headers, chat_spy: MagicMock
) -> None:
    cfg.llm.temperature = 0.85
    _post(client, "/api/generate/database", auth_headers)
    assert chat_spy.call_args.kwargs["temperature"] == pytest.approx(0.85)


# ── prompt_detail (table endpoint — richest signal surface) ───────────


def test_table_minimal_prompt_detail_skips_data_signals(
    cfg: AMXConfig,
    client,
    auth_headers,
    chat_spy: MagicMock,
    fake_db: FakeConnector,
) -> None:
    cfg.llm.prompt_detail = "minimal"
    resp = client.post(
        "/api/generate/table/sales/orders?profile=demo",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    user_prompt = _user_prompt(chat_spy)
    assert "samples" not in user_prompt.lower()
    assert "range:" not in user_prompt.lower()
    assert "distinct=" not in user_prompt.lower()
    # minimal preset still exposes PK/FK metadata, so profile_table is
    # still called; the assertion below would be too strict otherwise.


def test_table_detailed_prompt_detail_includes_data_signals(
    cfg: AMXConfig,
    client,
    auth_headers,
    chat_spy: MagicMock,
    fake_db: FakeConnector,
) -> None:
    """When the caller opts into ``profile_data=true`` the full evidence
    path runs — samples, min/max, cardinality, PK/FK all land in the
    prompt. Without the flag the endpoint is lite-by-default (covered
    by ``test_table_lite_by_default_skips_profile_table``).
    """
    cfg.llm.prompt_detail = "detailed"
    resp = client.post(
        "/api/generate/table/sales/orders?profile=demo&profile_data=true",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    user_prompt = _user_prompt(chat_spy)
    # samples + min/max + cardinality should all appear under detailed.
    assert "samples:" in user_prompt
    assert "range:" in user_prompt
    assert "distinct=" in user_prompt
    # Existing comments + PK/FK metadata also flow through.
    assert "Primary key: id" in user_prompt
    assert "customer_id→customers" in user_prompt
    # profile_table got called once; metadata-mode prompt detail wouldn't.
    assert ("sales", "orders") in fake_db.profile_table_calls


def test_column_full_prompt_detail_renders_full_evidence(
    cfg: AMXConfig,
    client,
    auth_headers,
    chat_spy: MagicMock,
) -> None:
    cfg.llm.prompt_detail = "full"
    resp = client.post(
        "/api/generate/column/sales/orders/customer_id?profile=demo&profile_data=true",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    user_prompt = _user_prompt(chat_spy)
    assert "Sample values:" in user_prompt
    assert "Range:" in user_prompt
    assert "Null count:" in user_prompt
    assert "Distinct values:" in user_prompt
    assert "references customers" in user_prompt


def test_table_lite_by_default_skips_profile_table(
    cfg: AMXConfig,
    client,
    auth_headers,
    chat_spy: MagicMock,
    fake_db: FakeConnector,
) -> None:
    """Single-asset table generate is lite-by-default. Even with the
    ``detailed`` preset, the endpoint should not call ``profile_table``
    unless the caller explicitly passes ``profile_data=true``. This is
    the contract that brings response time in line with Atlan /
    Databricks AI Generate.
    """
    cfg.llm.prompt_detail = "detailed"
    resp = client.post(
        "/api/generate/table/sales/orders?profile=demo",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    user_prompt = _user_prompt(chat_spy)
    # None of the heavy signals make it into the prompt.
    assert "samples:" not in user_prompt
    assert "range:" not in user_prompt
    assert "distinct=" not in user_prompt
    assert "Primary key:" not in user_prompt
    # And profile_table was never called.
    assert ("sales", "orders") not in fake_db.profile_table_calls


def test_column_lite_by_default_skips_profile_table(
    cfg: AMXConfig,
    client,
    auth_headers,
    chat_spy: MagicMock,
    fake_db: FakeConnector,
) -> None:
    cfg.llm.prompt_detail = "full"
    resp = client.post(
        "/api/generate/column/sales/orders/customer_id?profile=demo",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    user_prompt = _user_prompt(chat_spy)
    assert "Sample values:" not in user_prompt
    assert "Range:" not in user_prompt
    assert "Distinct values:" not in user_prompt
    assert ("sales", "orders") not in fake_db.profile_table_calls


def test_generate_records_source_path_in_run_settings(
    cfg: AMXConfig,
    client,
    auth_headers,
    fake_history: FakeHistoryStore,
) -> None:
    """``settings_json`` carries ``source_path`` so a future regression
    investigation can tell whether a run used the lite (catalog cache
    only) or the heavier ``profile`` path. Default endpoint call →
    ``"lite"``; opt-in via ``profile_data=true`` → ``"profile"``."""
    client.post(
        "/api/generate/table/sales/orders?profile=demo",
        headers=auth_headers,
    )
    lite_run = fake_history.runs[-1]
    assert lite_run["settings"]["source_path"] == "lite"

    client.post(
        "/api/generate/table/sales/orders?profile=demo&profile_data=true",
        headers=auth_headers,
    )
    profile_run = fake_history.runs[-1]
    assert profile_run["settings"]["source_path"] == "profile"


def test_table_profile_failure_falls_back(
    cfg: AMXConfig,
    client,
    auth_headers,
    chat_spy: MagicMock,
    fake_db: FakeConnector,
) -> None:
    """profile_table raising should NOT 500 the endpoint — the prompt
    should fall back to the metadata-only path and generation should
    still succeed.
    """
    cfg.llm.prompt_detail = "full"

    def boom(schema: str, table: str) -> TableProfile:
        raise RuntimeError("permissions issue on stats view")

    fake_db.profile_table = boom  # type: ignore[method-assign]

    resp = client.post(
        "/api/generate/table/sales/orders?profile=demo",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["alternatives_count"] == 1
    user_prompt = _user_prompt(chat_spy)
    # Metadata-only fallback: column list still renders, but data
    # signals are absent (no samples / range / distinct).
    assert "id (INTEGER)" in user_prompt
    assert "samples" not in user_prompt.lower()


# ── persistence (alternatives reach the pending queue) ────────────────


def test_n3_persists_all_alternatives_in_run_results(
    cfg: AMXConfig,
    client,
    auth_headers,
    chat_spy: MagicMock,
    fake_history: FakeHistoryStore,
) -> None:
    """N=3 → run_results.alternatives_json must hold all three so the
    Pending page can render A/B/C buttons.
    """
    cfg.llm.n_alternatives = 3
    chat_spy.return_value = ChatResult(
        content=(
            "DESCRIPTION_1: First take.\n"
            "DESCRIPTION_2: Second framing.\n"
            "DESCRIPTION_3: Third alternative.\n"
        )
    )
    resp = _post(client, "/api/generate/database", auth_headers)
    body = resp.json()
    result_id = body["result_id"]
    assert result_id is not None

    rr = fake_history.fetch_run_result(int(result_id))
    assert rr is not None
    assert rr["alternatives_json"] == [
        "First take.",
        "Second framing.",
        "Third alternative.",
    ]


# ── tokens_json + USD cost recording (PR-token-cost-tracking) ──


def test_generate_database_records_tokens_in_finish_run(
    cfg: AMXConfig,
    client,
    auth_headers,
    chat_spy: MagicMock,
    fake_history: FakeHistoryStore,
) -> None:
    """The Studio Run detail Metrics card was rendering "No metrics
    recorded" for ``generate.*`` runs because ``finish_run`` was never
    called. Pin the contract: every successful generate triggers
    ``finish_run(tokens={...})`` with a non-zero total_tokens summary,
    so the SPA can show input/output + USD cost just like analyze.run.
    """
    cfg.llm.n_alternatives = 1
    chat_spy.return_value = ChatResult(
        content="A single description sentence.",
        usage={"prompt_tokens": 100, "completion_tokens": 30, "total_tokens": 130},
    )

    resp = _post(client, "/api/generate/database", auth_headers)
    assert resp.status_code == 200

    assert len(fake_history.runs) == 1
    run = fake_history.runs[0]
    tokens = run.get("finish_tokens") or {}
    assert tokens.get("total_tokens", 0) > 0, (
        "generate.database must populate tokens_json.total_tokens "
        "(Run detail Metrics card depends on it)"
    )
    summary = tokens.get("summary") or []
    assert summary and summary[0][0].startswith("generate."), (
        "summary[0][0] should be the per-step label so the Run detail page can render the breakdown"
    )
    assert "records" in tokens
    assert run.get("status") == "ready_for_review"


def test_generate_column_records_per_step_label(
    cfg: AMXConfig,
    client,
    auth_headers,
    chat_spy: MagicMock,
    fake_history: FakeHistoryStore,
) -> None:
    """Each generate.* endpoint passes its asset_kind into the
    tracker step label so the breakdown reads ``generate.column``,
    not a generic ``llm.chat`` -- the Run detail page surfaces the
    label as the row name in the Metrics card."""
    cfg.llm.n_alternatives = 1
    chat_spy.return_value = ChatResult(
        content="A column-focused sentence.",
        usage={"prompt_tokens": 50, "completion_tokens": 20, "total_tokens": 70},
    )

    resp = client.post(
        "/api/generate/column/sales/orders/customer_id?profile=demo",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    run = fake_history.runs[0]
    summary = run["finish_tokens"]["summary"]
    assert summary[0][0] == "generate.column"


def test_generate_endpoint_resets_tracker_between_calls(
    cfg: AMXConfig,
    client,
    auth_headers,
    chat_spy: MagicMock,
    fake_history: FakeHistoryStore,
) -> None:
    """Two back-to-back generate calls must not bleed token totals
    into each other. The shared ``TokenTracker`` singleton is reset
    at endpoint top -- without that guard the second run's
    ``tokens_json.total_tokens`` would equal the SUM of both LLM
    calls instead of just its own."""
    cfg.llm.n_alternatives = 1

    chat_spy.return_value = ChatResult(
        content="First.",
        usage={"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    )
    _post(client, "/api/generate/database", auth_headers)

    chat_spy.return_value = ChatResult(
        content="Second.",
        usage={"prompt_tokens": 200, "completion_tokens": 50, "total_tokens": 250},
    )
    _post(client, "/api/generate/database", auth_headers)

    first = fake_history.runs[0]["finish_tokens"]
    second = fake_history.runs[1]["finish_tokens"]
    # The second run must reflect ONLY its own LLM round-trip.
    assert second["total_tokens"] < first["total_tokens"] + second["total_tokens"]
    # Stricter: tracker reset means the two records lists are
    # length-1 each (one llm.chat per generate).
    assert len(first["records"]) == 1
    assert len(second["records"]) == 1


def test_generate_schema_captures_target_database_in_settings(
    cfg: AMXConfig,
    client,
    auth_headers,
    chat_spy: MagicMock,
    fake_history: FakeHistoryStore,
) -> None:
    """User report: opening Run detail for a generate.schema run
    showed "this run didn't capture the target database" even
    though the request URL clearly carried ``?database=bird_train``.
    Pin the contract: the URL's ``database`` and ``catalog`` query
    args must round-trip into ``settings_json`` so
    ``GET /api/history/runs/{id}`` (history.py:92-97) can flatten
    them onto the run row and the Apply CTA stays unblocked.
    """
    cfg.llm.n_alternatives = 1
    chat_spy.return_value = ChatResult(
        content="schema-level description",
        usage={"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
    )

    resp = client.post(
        "/api/generate/schema/sales?profile=demo&database=bird_train",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    run = fake_history.runs[0]
    settings = run["settings"]
    assert settings["database"] == "bird_train"
    assert settings["catalog"] is None
