"""Search-result action dispatch + approval flow.

Extracted from :mod:`amx.cli_support.commands.search`. The three
functions drive the "After you pick an action on a search result"
flow:

- ``_run_search_action`` — dispatch a single action name (sync /
  approve_join / surface_evidence / ...).
- ``_run_approved_search_actions`` — interactive picker over actions
  the user is allowed to approve.
- ``_sync_cached_code_evidence`` — push cached code-scan evidence
  into the search catalog.

The functions cross-call into helpers that still live in
``search.py`` (``_interactive_sync_scope``, ``_answer_scope``,
``_sync_db_scope``); those calls go through the search module via
lazy import inside each function so tests that monkeypatch the
parent module keep working.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from amx.config import AMXConfig
from amx.utils.console import confirm, info, success, warn
from amx.utils.live_commands import command_display

if TYPE_CHECKING:
    from amx.search.catalog import SearchCatalog
    from amx.search.service import SearchService


LogEvent = Callable[..., None]


def _sync_cached_code_evidence(
    cfg: AMXConfig,
    catalog: SearchCatalog,
    *,
    scope: dict[str, list[str]] | None = None,
) -> bool:
    try:
        from amx.codebase.cache import load_latest_cached_report
        from amx.utils.console import step_spinner

        code_path = cfg.resolve_code_path(cfg.active_code_profile or None, None)
        if not code_path:
            warn("No active code profile path is configured.")
            return False
        profile_nm = (cfg.active_code_profile or "default").strip() or "default"
        with step_spinner("Loading cached code evidence"):
            manifest, report = load_latest_cached_report(profile_nm, code_path)
        if report is None or manifest is None:
            warn("No cached code-scan report found. Run `/code scan` first.")
            return False
        schema_name = str(
            manifest.get("schema") or next(iter((scope or {}).keys()), cfg.current_schema or "")
        )
        with step_spinner("Refreshing /search code evidence"):
            catalog.sync_code_report(
                db_profile=cfg.active_db_profile or "default",
                db_backend=cfg.db.backend,
                database_name=cfg.db.database or cfg.db.catalog or cfg.db.project or "",
                schema_name=schema_name,
                source_path=code_path,
                report=report,
            )
        success("Refreshed `/search` code evidence from the latest cached code scan.")
        return True
    except Exception as exc:
        warn(f"Could not refresh code evidence: {exc}")
        return False


def _run_search_action(
    cfg: AMXConfig,
    catalog: SearchCatalog,
    answer: Any,
    action_name: str,
) -> dict[str, Any]:
    from amx.cli_support.commands import search as _search_mod

    scope = _search_mod._answer_scope(answer, cfg)
    db_profile = cfg.active_db_profile or "default"
    if action_name == "sync_catalog":
        if not scope:
            cfg, scope = _search_mod._interactive_sync_scope(
                cfg, cfg.current_schema or None, cfg.current_table or None
            )
        if not scope:
            return {"action": action_name, "status": "skipped", "reason": "no_scope"}
        job_id = catalog.start_sync_job(
            db_profile, "sync", {"scope": scope, "trigger": "search_action"}
        )
        inserted = 0
        updated = 0
        try:
            with command_display(
                schema=next(iter(scope.keys()), ""),
                table=f"{sum(len(v) for v in scope.values())} assets",
                mode="search-sync",
                provider=cfg.llm.provider,
                model=cfg.llm.model,
            ):
                inserted, updated = _search_mod._sync_db_scope(cfg, catalog, scope=scope)
                _sync_cached_code_evidence(cfg, catalog, scope=scope)
            catalog.finish_sync_job(
                job_id, status="success", inserted_count=inserted, updated_count=updated
            )
            success(
                f"Approved search action complete: sync_catalog inserted={inserted}, updated={updated}"
            )
            return {
                "action": action_name,
                "status": "success",
                "inserted": inserted,
                "updated": updated,
                "scope": scope,
            }
        except Exception as exc:
            catalog.finish_sync_job(
                job_id,
                status="failed",
                inserted_count=inserted,
                updated_count=updated,
                error_text=str(exc),
            )
            warn(f"Approved search action failed: {exc}")
            return {"action": action_name, "status": "failed", "reason": str(exc), "scope": scope}
    if action_name == "refresh_code_evidence":
        with command_display(
            schema=next(iter(scope.keys()), "") if scope else "",
            mode="search-sync",
            provider=cfg.llm.provider,
            model=cfg.llm.model,
        ):
            ok = _sync_cached_code_evidence(cfg, catalog, scope=scope)
        return {"action": action_name, "status": "success" if ok else "skipped", "scope": scope}
    if action_name == "analyze_table":
        tables = answer.details.get("retrieval", {}).get("resolved_tables") or []
        if not tables and answer.rows:
            row = answer.rows[0]
            if row.get("schema_name") and row.get("table_name"):
                tables = [f"{row.get('schema_name')}.{row.get('table_name')}"]
        if not tables:
            return {"action": action_name, "status": "skipped", "reason": "no_resolved_table"}
        schema_name, table_name = str(tables[0]).split(".", 1)
        try:
            from amx.core.inference import infer_table_metadata

            results = infer_table_metadata(
                cfg, schema_name, table_name, include_rag=True, include_codebase=False
            )
            success(
                f"Approved search action complete: analyze_table produced {len(results)} suggestions for {schema_name}.{table_name}"
            )
            return {
                "action": action_name,
                "status": "success",
                "table": tables[0],
                "suggestions": len(results),
            }
        except Exception as exc:
            warn(f"Approved search action failed: {exc}")
            return {
                "action": action_name,
                "status": "failed",
                "table": tables[0],
                "reason": str(exc),
            }
    info(f"Action `{action_name}` is advisory and has no automatic executor.")
    return {"action": action_name, "status": "advisory"}


def _run_approved_search_actions(
    cfg: AMXConfig, svc: SearchService, answer: Any
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    actions = answer.details.get("actions", []) or []
    if not actions:
        return records
    for action in actions:
        action_name = str((action or {}).get("action") or "").strip()
        if action_name not in {"sync_catalog", "refresh_code_evidence", "analyze_table"}:
            continue
        reason = str((action or {}).get("reason") or "").strip()
        prompt = f"Run search action `{action_name}`?"
        if reason:
            prompt += f" {reason}"
        try:
            approved = confirm(prompt, default=False)
        except (EOFError, KeyboardInterrupt):
            approved = False
        if not approved:
            records.append({"action": action_name, "status": "declined"})
            continue
        records.append(_run_search_action(cfg, svc.catalog, answer, action_name))
    return records
