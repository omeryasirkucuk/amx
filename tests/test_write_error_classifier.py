"""Write-path error classifier.

When a COMMENT ON / ALTER TABLE fails during Apply pending queue,
the Studio SPA needs to render an actionable banner — "missing
ALTER privilege on samples.nyctaxi.trips" — not the raw driver
text. ``classify_write_error`` turns the driver exception into a
stable kind slug plus title / body / suggested_action.

These tests pin the slugs (the SPA pivots on them) and the
backend-specific suggested_action wording for the privilege case
that prompted this work.
"""

from __future__ import annotations

from amx.core.errors import classify_write_error


def test_databricks_insufficient_permissions_classifies_as_alter_privilege() -> None:
    """The Databricks screenshot evidence — exact wording of the
    server-side rejection — must land in the alter_privilege_denied
    bucket so the SPA renders the DBA-grant hint."""
    exc = RuntimeError(
        "[INSUFFICIENT_PERMISSIONS] User does not have ALTER on table `samples`.`nyctaxi`.`trips`"
    )
    cls = classify_write_error(
        exc,
        backend="databricks",
        schema="nyctaxi",
        table="trips",
    )
    assert cls.kind == "alter_privilege_denied"
    assert "nyctaxi.trips" in cls.title
    assert "ALTER" in cls.suggested_action or "workspace admin" in cls.suggested_action.lower()


def test_postgres_permission_denied_classifies_with_grant_hint() -> None:
    exc = RuntimeError("permission denied for table orders (SQLSTATE 42501)")
    cls = classify_write_error(
        exc,
        backend="postgresql",
        schema="public",
        table="orders",
    )
    assert cls.kind == "alter_privilege_denied"
    assert "GRANT ALTER" in cls.suggested_action


def test_bigquery_access_denied_classifies_with_role_hint() -> None:
    exc = RuntimeError(
        "403 accessDenied: Permission bigquery.tables.update denied on table proj:ds.trips"
    )
    cls = classify_write_error(
        exc,
        backend="bigquery",
        schema="ds",
        table="trips",
    )
    assert cls.kind == "alter_privilege_denied"
    assert "Data Editor" in cls.suggested_action


def test_table_not_found_classifies_separately() -> None:
    """A dropped/renamed table during the apply window is a distinct
    failure from privilege denial — the SPA shows a 're-sync the
    catalog' hint instead of a privilege hint."""
    exc = RuntimeError('relation "public.orders" does not exist')
    cls = classify_write_error(
        exc,
        backend="postgresql",
        schema="public",
        table="orders",
    )
    assert cls.kind == "table_not_found"
    assert "/sync" in cls.suggested_action or "re-sync" in cls.suggested_action.lower()


def test_savepoint_unsupported_surfaces_loudly() -> None:
    """Defence in depth: if the writeback ever regresses and ships a
    SAVEPOINT to a backend that doesn't support it, the classifier
    flags it as a bug rather than a permission issue."""
    exc = RuntimeError(
        "[PARSE_SYNTAX_ERROR] Syntax error at or near 'SAVEPOINT': extra input. line 1 pos 0."
    )
    cls = classify_write_error(exc, backend="databricks")
    assert cls.kind == "savepoint_unsupported"


def test_unknown_error_falls_through_with_raw_text() -> None:
    """Drivers we have not special-cased still produce a usable
    outcome — the SPA gets the raw text trimmed and a generic
    instruction."""
    exc = RuntimeError("ORA-12345: something deeply Oracle-specific")
    cls = classify_write_error(exc, backend="oracle")
    assert cls.kind == "unknown"
    assert "ORA-12345" in cls.body


def test_classifier_never_returns_none() -> None:
    """Every code path returns a WriteErrorClass — the writeback
    relies on this so RowApplyOutcome.error_kind is always populated
    on a failure."""
    cls = classify_write_error(RuntimeError(""), backend="")
    assert cls is not None
    assert cls.kind  # non-empty slug
