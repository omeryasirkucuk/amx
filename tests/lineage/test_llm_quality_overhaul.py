"""Quality-overhaul regressions: self-loop, candidate ranker, rich context, column-pair.

These tests pin the behaviour changes introduced by the AI Generate
quality pass:

* ``parse_response`` rejects self-loops (``from_table == to_table``)
  unconditionally and now defaults to ``min_confidence=0.6``.
* ``parse_response`` accepts the new scalar ``from_column`` /
  ``to_column`` shape in addition to the legacy ``column_pairs`` list.
* ``score_candidates`` ranks by deterministic evidence — FK partners
  + view co-mentions + query-log co-occurrence beat alphabetical
  order, even when sibling tables share a name prefix.
* ``build_rich_context`` surfaces table description, FK partners, and
  view references for the anchor so the LLM prompt isn't bare.
* ``_verdict_examples`` carries column-pair detail into
  ``FeedbackExample`` so the few-shot prompt block tells the LLM
  exactly which column pairs the user approved or rejected.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from amx.lineage import llm_prompt
from amx.lineage._anchor_context import build_rich_context
from amx.lineage._candidate_ranker import score_candidates
from amx.lineage.extractors.llm import _verdict_examples
from amx.lineage.types import ColumnRef, Scope
from amx.storage.sqlite_store import SQLiteHistoryStore
from tests.lineage.conftest import (
    seed_column_comments_cache_for_table,
    seed_table_entity,
)

# ── parser fixes (Layer 1) ──────────────────────────────────────────────


def test_parser_rejects_self_loop():
    """``from_table == to_table`` must not survive parsing."""
    raw = json.dumps(
        {
            "edges": [
                {
                    "from_table": "public.orders",
                    "to_table": "public.orders",
                    "confidence": 0.95,
                    "reasoning": "model glitch",
                },
                {
                    "from_table": "public.orders",
                    "to_table": "public.customers",
                    "from_column": "customer_id",
                    "to_column": "id",
                    "confidence": 0.9,
                },
            ]
        }
    )
    out = llm_prompt.parse_response(
        raw,
        anchor_fqn="public.orders",
        valid_candidate_fqns={"public.customers"},
    )
    assert len(out) == 1, "self-loop must be dropped"
    assert out[0].from_fqn == "public.orders"
    assert out[0].to_fqn == "public.customers"


def test_parser_default_min_confidence_is_0_6():
    """Bumped default keeps marginal hallucinations off the canvas."""
    raw = json.dumps(
        {
            "edges": [
                {
                    "from_table": "public.orders",
                    "to_table": "public.customers",
                    "from_column": "customer_id",
                    "to_column": "id",
                    "confidence": 0.5,
                },
            ]
        }
    )
    out = llm_prompt.parse_response(
        raw,
        anchor_fqn="public.orders",
        valid_candidate_fqns={"public.customers"},
    )
    assert out == [], "0.5 < default 0.6 floor"


def test_parser_accepts_scalar_column_pair():
    """The new prompt asks for ``from_column``/``to_column`` scalars."""
    raw = json.dumps(
        {
            "edges": [
                {
                    "from_table": "public.orders",
                    "to_table": "public.customers",
                    "from_column": "customer_id",
                    "to_column": "id",
                    "confidence": 0.9,
                },
            ]
        }
    )
    out = llm_prompt.parse_response(
        raw,
        anchor_fqn="public.orders",
        valid_candidate_fqns={"public.customers"},
    )
    assert out[0].column_pairs == [("customer_id", "id")]


def test_parser_still_accepts_legacy_column_pairs():
    """Cached responses from before the prompt rewrite must keep parsing."""
    raw = json.dumps(
        {
            "edges": [
                {
                    "from_table": "public.orders",
                    "to_table": "public.customers",
                    "column_pairs": [["customer_id", "id"]],
                    "confidence": 0.85,
                },
            ]
        }
    )
    out = llm_prompt.parse_response(
        raw,
        anchor_fqn="public.orders",
        valid_candidate_fqns={"public.customers"},
    )
    assert out[0].column_pairs == [("customer_id", "id")]


# ── prompt rendering (Layer 2) ──────────────────────────────────────────


def test_prompt_renders_rich_anchor_signals():
    """When FK / view / co-occur signals are present they ride into the prompt."""
    anchor = llm_prompt.AnchorContext(
        fqn="public.orders",
        columns=[{"name": "id", "dtype": "int"}],
        description="customer purchase records",
        fk_partners=[
            {
                "direction": "inbound",
                "other_fqn": "public.line_items",
                "from_column": "order_id",
                "to_column": "id",
            }
        ],
        view_references=[{"view_fqn": "public.v_orders_summary", "other_tables": ["customers"]}],
        co_occurrence_partners=[{"other_fqn": "public.shipments", "count": 4}],
    )
    candidate = llm_prompt.CandidateTable(
        fqn="public.customers",
        columns=[{"name": "id", "dtype": "int"}],
        score=1.4,
        reasons=["FK", "shared-cols×3"],
    )
    msgs = llm_prompt.build_messages(anchor, [candidate])
    user_payload = msgs[1]["content"]
    # Anchor signals reach the prompt
    assert "known_foreign_keys" in user_payload
    assert "line_items" in user_payload
    assert "v_orders_summary" in user_payload
    assert "shipments" in user_payload
    # Candidate score + reasons reach the prompt
    assert '"score": 1.4' in user_payload
    assert "FK" in user_payload
    assert "shared-cols" in user_payload


def test_prompt_system_message_forbids_self_loop():
    """The anti-self-loop rule must be in the system prompt verbatim."""
    msgs = llm_prompt.build_messages(
        llm_prompt.AnchorContext(fqn="x.y", columns=[]),
        [llm_prompt.CandidateTable(fqn="x.z", columns=[])],
    )
    sys_msg = msgs[0]["content"]
    assert "from_table == to_table" in sys_msg


# ── feedback examples (Layer 3 partial) ──────────────────────────────────


def test_feedback_example_formats_column_pair():
    """Column-pair detail must render in the few-shot block."""
    ex = llm_prompt.FeedbackExample(
        from_fqn="public.orders",
        to_fqn="public.customers",
        from_column="customer_id",
        to_column="id",
        note="approved by Ali",
    )
    rendered = llm_prompt._format_feedback(ex)
    assert "customer_id" in rendered
    assert "→" in rendered
    assert "customers.id" in rendered
    assert "approved by Ali" in rendered


def test_feedback_example_falls_back_to_table_level():
    """Legacy rows with no column pair still render cleanly."""
    ex = llm_prompt.FeedbackExample(from_fqn="a.b", to_fqn="c.d")
    assert llm_prompt._format_feedback(ex) == "a.b → c.d"


# ── candidate ranker (Layer 2) ──────────────────────────────────────────


@pytest.fixture
def hs(tmp_path: Path) -> SQLiteHistoryStore:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    return store


def _seed_lineage_fk(
    hs: SQLiteHistoryStore,
    *,
    profile: str,
    from_id: int,
    to_id: int,
    from_column: str = "",
    to_column: str = "",
) -> None:
    """Insert a deterministic FK relationship row the ranker reads."""
    with hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO catalog_relationships
                (from_entity_id, to_entity_id, relationship_type, score, source,
                 from_column, to_column, details_json, last_seen)
            VALUES (?, ?, 'lineage_fk', 1.0, 'fk', ?, ?, '{}', ?)
            """,
            (from_id, to_id, from_column, to_column, time.time()),
        )


def _seed_view_definition(
    hs: SQLiteHistoryStore,
    *,
    profile: str,
    database: str,
    schema: str,
    view: str,
    parsed: list[dict],
) -> None:
    with hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO view_definitions_cache
                (db_profile, database_name, schema_name, view_name, dialect,
                 ddl_text, parsed_lineage_json, parse_status, parse_error,
                 fetched_at, expires_at)
            VALUES (?, ?, ?, ?, '', '', ?, 'ok', '', ?, ?)
            """,
            (
                profile,
                database,
                schema,
                view,
                json.dumps(parsed),
                time.time(),
                time.time() + 3600,
            ),
        )


def test_ranker_lifts_fk_partners_above_alphabetical_siblings(hs):
    """Even when ``ADRX`` table names cluster alphabetically, an FK
    partner whose name sorts later must rank higher."""
    profile = "sap"
    database = "erp"
    anchor_id = seed_table_entity(
        hs,
        profile=profile,
        backend="postgresql",
        database=database,
        schema="public",
        table="adrc",
    )
    # Sibling tables with shared prefix — old ranker would put these first.
    seed_table_entity(
        hs, profile=profile, backend="postgresql", database=database, schema="public", table="adr1"
    )
    seed_table_entity(
        hs, profile=profile, backend="postgresql", database=database, schema="public", table="adr2"
    )
    seed_table_entity(
        hs, profile=profile, backend="postgresql", database=database, schema="public", table="adr3"
    )
    # An FK partner that sorts AFTER the siblings — must still rank above them.
    partner_id = seed_table_entity(
        hs,
        profile=profile,
        backend="postgresql",
        database=database,
        schema="public",
        table="zorders",
    )
    _seed_lineage_fk(
        hs,
        profile=profile,
        from_id=anchor_id,
        to_id=partner_id,
        from_column="addr_id",
        to_column="id",
    )

    scope = Scope(
        profile=profile,
        anchor=ColumnRef(database=database, schema="public", table="adrc", column=""),
        depth_up=1,
        depth_down=1,
        database=database,
        schema="public",
    )
    ranked = score_candidates(hs, scope, max_count=10)
    fqns_in_order = [c.fqn for c in ranked]
    assert "public.zorders" in fqns_in_order
    # The FK partner outranks every prefix sibling.
    z_idx = fqns_in_order.index("public.zorders")
    for sib in ("public.adr1", "public.adr2", "public.adr3"):
        if sib in fqns_in_order:
            assert z_idx < fqns_in_order.index(sib), (
                f"FK partner {sib!r} should rank above prefix sibling — got order {fqns_in_order}"
            )
    # The FK candidate's score includes the FK weight + reason.
    fk_row = next(c for c in ranked if c.fqn == "public.zorders")
    assert "FK" in fk_row.reasons


def test_ranker_lifts_view_co_mentions(hs):
    """A candidate that appears alongside the anchor in a parsed view
    must outrank a name-prefix sibling with no other signal."""
    profile = "p"
    database = "db"
    anchor_id = seed_table_entity(
        hs,
        profile=profile,
        backend="postgresql",
        database=database,
        schema="public",
        table="orders",
    )
    # No-signal sibling
    seed_table_entity(
        hs,
        profile=profile,
        backend="postgresql",
        database=database,
        schema="public",
        table="orders_archive",
    )
    # View co-mention partner
    partner_id = seed_table_entity(
        hs,
        profile=profile,
        backend="postgresql",
        database=database,
        schema="public",
        table="customers",
    )
    _seed_view_definition(
        hs,
        profile=profile,
        database=database,
        schema="public",
        view="v_orders_summary",
        parsed=[
            {"target": "customer_id", "sources": [{"table": "orders", "column": "customer_id"}]},
            {"target": "customer_name", "sources": [{"table": "customers", "column": "name"}]},
        ],
    )
    del anchor_id, partner_id  # silence linter; we resolve by FQN below

    scope = Scope(
        profile=profile,
        anchor=ColumnRef(database=database, schema="public", table="orders", column=""),
        depth_up=1,
        depth_down=1,
        database=database,
        schema="public",
    )
    ranked = score_candidates(hs, scope, max_count=10)
    fqns_in_order = [c.fqn for c in ranked]
    if "public.orders_archive" in fqns_in_order:
        assert fqns_in_order.index("public.customers") < fqns_in_order.index(
            "public.orders_archive"
        ), f"view partner should outrank prefix sibling — got {fqns_in_order}"


# ── rich anchor context (Layer 2) ──────────────────────────────────────


def test_build_rich_context_assembles_columns_and_description(hs):
    """Description + columns must arrive intact for the prompt builder."""
    profile = "p"
    database = "db"
    # 1) Create the table entity. 2) Insert a description row pointing
    # back at it via ``entity_id``. 3) Update the entity's
    # ``effective_description_id`` so the JOIN in ``build_rich_context``
    # picks it up.
    entity_id = seed_table_entity(
        hs,
        profile=profile,
        backend="postgresql",
        database=database,
        schema="public",
        table="orders",
    )
    with hs._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO catalog_descriptions
                (entity_id, description_text, source_kind, created_at, chosen_description)
            VALUES (?, ?, 'manual', ?, 1)
            """,
            (entity_id, "Customer purchase records.", time.time()),
        )
        desc_id = int(cur.lastrowid)
        conn.execute(
            "UPDATE catalog_entities SET effective_description_id = ? WHERE id = ?",
            (desc_id, entity_id),
        )
    seed_column_comments_cache_for_table(
        hs,
        profile=profile,
        database=database,
        schema="public",
        table="orders",
        columns={
            "id": {"type": "int", "description": "Primary key"},
            "customer_id": {"type": "int", "description": "FK to customers.id"},
        },
    )

    scope = Scope(
        profile=profile,
        anchor=ColumnRef(database=database, schema="public", table="orders", column=""),
        depth_up=1,
        depth_down=1,
        database=database,
        schema="public",
    )
    ctx = build_rich_context(hs, scope)
    assert ctx.fqn == "public.orders"
    assert ctx.table_description == "Customer purchase records."
    col_names = [c["name"] for c in ctx.columns]
    assert "id" in col_names and "customer_id" in col_names
    # FK partner / view / co-occur lists default to [] when DB is empty.
    assert ctx.fk_partners == []
    assert ctx.view_references == []


# ── feedback round-trip with column-pair (Layer 3) ──────────────────────


def seed_table_entity_with_id(  # helper local to this test
    hs: SQLiteHistoryStore, *, profile: str, database: str, schema: str, table: str
) -> int:
    return seed_table_entity(
        hs,
        profile=profile,
        backend="postgresql",
        database=database,
        schema=schema,
        table=table,
    )


def test_verdict_examples_carry_column_pair(hs):
    """An approved column-grain edge round-trips into FeedbackExample."""
    profile = "p"
    database = "db"
    src_id = seed_table_entity_with_id(
        hs, profile=profile, database=database, schema="public", table="orders"
    )
    tgt_id = seed_table_entity_with_id(
        hs, profile=profile, database=database, schema="public", table="customers"
    )
    with hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO catalog_relationships
                (from_entity_id, to_entity_id, relationship_type, score, source,
                 from_column, to_column, details_json, last_seen,
                 verdict, audit_at)
            VALUES (?, ?, 'lineage_llm', 0.85, 'llm',
                    'customer_id', 'id', '{}', ?, 'approved', ?)
            """,
            (src_id, tgt_id, time.time(), time.time()),
        )

    scope = Scope(
        profile=profile,
        anchor=ColumnRef(database=database, schema="public", table="orders", column=""),
        depth_up=1,
        depth_down=1,
        database=database,
        schema="public",
    )
    approved, rejected = _verdict_examples(hs, scope)
    assert len(approved) == 1
    assert approved[0].from_column == "customer_id"
    assert approved[0].to_column == "id"
    assert rejected == []
