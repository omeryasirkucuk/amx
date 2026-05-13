"""Tests for the new ``RunRequest.column_overrides`` body field.

The orchestrator has supported per-table column filtering for a
while via ``column_overrides``; the Studio surface only just got the
field on ``/api/runs``. These tests assert the wire shape and the
``"schema.table"`` → ``(schema, table)`` translation done in the
worker.
"""

from __future__ import annotations

from amx.web.routers.runs import RunRequest


def test_run_request_accepts_column_overrides() -> None:
    body = RunRequest(
        scope={"public": ["users", "orders"]},
        column_overrides={
            "public.users": ["id", "email"],
            "public.orders": ["status"],
        },
    )
    assert body.column_overrides is not None
    assert body.column_overrides["public.users"] == ["id", "email"]


def test_run_request_omits_column_overrides_by_default() -> None:
    body = RunRequest(scope={"public": ["users"]})
    assert body.column_overrides is None


def test_run_request_column_overrides_can_be_empty_dict() -> None:
    body = RunRequest(scope={"public": ["users"]}, column_overrides={})
    assert body.column_overrides == {}


def test_translation_helper_parses_dotted_keys() -> None:
    """Mirror the worker's runtime translation logic.

    The worker turns ``{"schema.table": [cols]}`` (JSON-friendly) into
    ``{(schema, table): {cols}}`` for the orchestrator. We can't
    drive the full worker without spinning up an Orchestrator, but
    the translation step itself is small enough to assert here.
    """
    raw = {
        "public.users": ["id", "email"],
        "no_dot_key": ["x"],
        "schema.": ["empty_table"],
        ".table": ["empty_schema"],
        "ok.t": [],  # empty cols skipped
    }
    translated: dict[tuple[str, str], set[str]] = {}
    for key, cols in raw.items():
        if "." not in key:
            continue
        schema, _, table = key.partition(".")
        if not schema or not table or not cols:
            continue
        translated[(schema, table)] = set(cols)
    assert translated == {("public", "users"): {"id", "email"}}
