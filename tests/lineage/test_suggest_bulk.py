"""``suggest_lineage_llm_bulk`` — schema-wide LLM with hard budgets."""

from __future__ import annotations

import json
from unittest.mock import patch

from amx.config import AMXConfig, DBConfig, LLMConfig
from amx.lineage.service import suggest_lineage_llm_bulk

from .conftest import (
    seed_column_comments_cache_for_table,
    seed_table_entity,
)


class _StubChatResult:
    def __init__(self, content: str, total_tokens: int) -> None:
        self.content = content
        self.usage = {"total_tokens": total_tokens}


def _make_cfg() -> AMXConfig:
    cfg = AMXConfig()
    cfg.llm = LLMConfig(provider="openai", model="gpt-4o-mini")
    cfg.db_profiles = {"p": DBConfig(backend="postgresql", database="")}
    return cfg


def _seed_three_tables(hs):
    for name in ("orders", "customers", "items"):
        seed_table_entity(hs, schema="public", table=name)
        seed_column_comments_cache_for_table(
            hs,
            schema="public",
            table=name,
            columns={"id": {"type": "integer"}},
        )


def test_bulk_suggest_halts_when_token_budget_exhausted(hs):
    _seed_three_tables(hs)
    cfg = _make_cfg()

    def fake_chat(self, *, messages, **kwargs):
        # Every call costs more than the budget — first call halts iteration.
        return _StubChatResult(
            content=json.dumps({"edges": []}),
            total_tokens=10_000,
        )

    with patch("amx.llm.provider.LLMProvider.chat", new=fake_chat):
        rollup = suggest_lineage_llm_bulk(
            hs=hs,
            profile="p",
            schema="public",
            database="",
            cfg=cfg,
            budget_tokens=5_000,
            budget_tables=10,
        )

    assert rollup.tables_examined == 1
    assert rollup.halted_by == "budget_tokens"
    assert rollup.total_tokens_used == 10_000


def test_bulk_suggest_halts_when_table_budget_exhausted(hs):
    _seed_three_tables(hs)
    cfg = _make_cfg()

    def fake_chat(self, *, messages, **kwargs):
        return _StubChatResult(content=json.dumps({"edges": []}), total_tokens=100)

    with patch("amx.llm.provider.LLMProvider.chat", new=fake_chat):
        rollup = suggest_lineage_llm_bulk(
            hs=hs,
            profile="p",
            schema="public",
            database="",
            cfg=cfg,
            budget_tokens=1_000_000,
            budget_tables=2,
        )

    assert rollup.tables_examined == 2
    assert rollup.halted_by == "budget_tables"


def test_bulk_suggest_completes_when_budgets_loose(hs):
    _seed_three_tables(hs)
    cfg = _make_cfg()

    def fake_chat(self, *, messages, **kwargs):
        return _StubChatResult(content=json.dumps({"edges": []}), total_tokens=10)

    with patch("amx.llm.provider.LLMProvider.chat", new=fake_chat):
        rollup = suggest_lineage_llm_bulk(
            hs=hs,
            profile="p",
            schema="public",
            database="",
            cfg=cfg,
            budget_tokens=1_000_000,
            budget_tables=100,
        )

    assert rollup.tables_examined == 3
    assert rollup.halted_by == "no_more_tables"
