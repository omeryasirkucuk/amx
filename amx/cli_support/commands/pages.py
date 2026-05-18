"""Flat ``/pages-*`` commands for the AMX interactive CLI.

Each command lives directly on the root Click group so the REPL
exposes ``/pages-new``, ``/pages-list``, etc. (per CLAUDE.md rule #9).
A bare ``/pages-new`` runs the wizard; flags are optional power-user
shortcuts.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click
from rich.console import Console
from rich.table import Table

from amx.config import AMXConfig
from amx.pages.factory import build_pages_service
from amx.pages.service import PagesService
from amx.pages.types import AssetRef, SourceRef
from amx.utils.console import error, info, success, warn

FinalizeScope = Callable[..., Any]

_ASSET_KINDS: frozenset[str] = frozenset(
    {
        "db_profile",
        "db_database",
        "db_schema",
        "db_table",
        "db_column",
        "doc_profile",
        "lineage_artifact",
    }
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _svc(cfg: AMXConfig) -> PagesService:
    return build_pages_service(cfg)


def _parse_asset_flag(raw: str) -> AssetRef:
    if ":" not in raw:
        raise click.BadParameter(
            f"asset must be 'KIND:REF' (got {raw!r}). "
            f"Valid kinds: {', '.join(sorted(_ASSET_KINDS))}."
        )
    kind, ref = raw.split(":", 1)
    kind = kind.strip()
    ref = ref.strip()
    if kind not in _ASSET_KINDS:
        raise click.BadParameter(
            f"unknown asset kind {kind!r}. Valid: {', '.join(sorted(_ASSET_KINDS))}."
        )
    if not ref:
        raise click.BadParameter(f"asset {raw!r} has an empty ref")
    return AssetRef(kind=kind, ref=ref)  # type: ignore[arg-type]


def _collect_assets_wizard(cfg: AMXConfig) -> list[AssetRef]:
    """Walk the user through DB / doc-profile / lineage pickers."""
    assets: list[AssetRef] = []

    # DB profile assets — list configured profiles, let the user pick by name.
    db_names = sorted((cfg.db_profiles or {}).keys())
    if db_names:
        info("Configured DB profiles: " + ", ".join(db_names))
    pick_db = click.prompt(
        "Attach DB profile(s)? Comma-separated names (or empty to skip)",
        default="",
        show_default=False,
    ).strip()
    if pick_db:
        for name in [p.strip() for p in pick_db.split(",") if p.strip()]:
            assets.append(AssetRef(kind="db_profile", ref=name))

    # Doc profile assets.
    doc_names = sorted((cfg.doc_profiles or {}).keys())
    if doc_names:
        info("Configured doc profiles: " + ", ".join(doc_names))
    pick_doc = click.prompt(
        "Attach doc profile(s)? Comma-separated names (or empty to skip)",
        default="",
        show_default=False,
    ).strip()
    if pick_doc:
        for name in [p.strip() for p in pick_doc.split(",") if p.strip()]:
            assets.append(AssetRef(kind="doc_profile", ref=name))

    # Lineage artifacts — read from the history store.
    try:
        from amx.lineage.store import list_lineage_artifacts
        from amx.storage.sqlite_store import history_store

        hs = history_store()
        artifact_rows = list_lineage_artifacts(hs) if hs is not None else []
    except Exception:  # noqa: BLE001 — best-effort listing
        artifact_rows = []
    if artifact_rows:
        info(
            "Lineage artifacts: "
            + ", ".join(str(r.get("name") or r.get("id")) for r in artifact_rows[:20])
        )
    pick_lin = click.prompt(
        "Attach lineage artifact(s)? Comma-separated names/ids (or empty to skip)",
        default="",
        show_default=False,
    ).strip()
    if pick_lin:
        for ref in [p.strip() for p in pick_lin.split(",") if p.strip()]:
            assets.append(AssetRef(kind="lineage_artifact", ref=ref))

    return assets


def _collect_sources_wizard() -> list[Path]:
    """Prompt for local file paths to attach as sources."""
    raw = click.prompt(
        "Attach local source files? Comma-separated paths (or empty to skip)",
        default="",
        show_default=False,
    ).strip()
    if not raw:
        return []
    paths: list[Path] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        path = Path(chunk).expanduser()
        if not path.is_file():
            warn(f"Skipping {path}: not a regular file")
            continue
        paths.append(path)
    return paths


def _attach_sources(
    cfg: AMXConfig,
    page_id: str,
    paths: list[Path],
) -> list[SourceRef]:
    """Copy each path into the per-page upload root and return SourceRefs."""
    if not paths:
        return []
    from amx.docs.uploads import UploadError, save_uploaded_file

    refs: list[SourceRef] = []
    profile = f"pages/{page_id}"
    for path in paths:
        try:
            payload = path.read_bytes()
            res = save_uploaded_file(cfg, profile, path.name, payload)
        except UploadError as exc:
            error(f"{path}: {exc}")
            continue
        except OSError as exc:
            error(f"{path}: {exc}")
            continue
        refs.append(SourceRef(kind="upload", path=res.saved_path, original_name=res.original_name))
        info(f"Attached source: {path.name}")
    return refs


def _hard_delete_page(page_id: str) -> bool:
    """Hard-delete a page and its child rows. Returns True on success."""
    from amx.storage.sqlite_store import history_store

    hs = history_store()
    if hs is None:
        return False
    try:
        with hs._connect() as conn:
            conn.execute("DELETE FROM documentation_page_versions WHERE page_id = ?", (page_id,))
            conn.execute("DELETE FROM documentation_page_sources WHERE page_id = ?", (page_id,))
            conn.execute("DELETE FROM documentation_page_assets WHERE page_id = ?", (page_id,))
            conn.execute("DELETE FROM documentation_pages WHERE id = ?", (page_id,))
    except Exception as exc:  # noqa: BLE001
        error(f"Hard delete failed: {exc}")
        return False
    return True


def register_pages_commands(
    main: click.Group,
    *,
    finalize_scope: FinalizeScope | None = None,
) -> None:
    """Attach ``/pages-*`` flat commands to the main Click group."""
    del finalize_scope  # reserved for future per-asset scope expansion

    @main.command("pages-new")
    @click.option("--title", default=None, help="Page title (prompted when omitted).")
    @click.option("--intent", default=None, help="Free-text generation intent.")
    @click.option(
        "--asset",
        "assets_flag",
        multiple=True,
        help="Repeatable. Format: KIND:REF (e.g. db_profile:prod, doc_profile:eng).",
    )
    @click.option(
        "--source",
        "sources_flag",
        multiple=True,
        type=click.Path(exists=True, dir_okay=False, path_type=Path),
        help="Repeatable. Local file path to attach as a source.",
    )
    @click.option(
        "--no-generate",
        is_flag=True,
        default=False,
        help="Create the draft but skip the immediate LLM composition.",
    )
    @click.pass_obj
    def pages_new(
        cfg: AMXConfig,
        title: str | None,
        intent: str | None,
        assets_flag: tuple[str, ...],
        sources_flag: tuple[Path, ...],
        no_generate: bool,
    ) -> None:
        """Create a new documentation page (wizard when flags are omitted)."""
        svc = _svc(cfg)

        # Title: flag wins, otherwise prompt.
        final_title = (title or "").strip()
        if not final_title:
            final_title = click.prompt("Page title").strip()
            if not final_title:
                error("Title is required.")
                return

        # Assets: flag wins; otherwise run the wizard.
        if assets_flag:
            try:
                final_assets = [_parse_asset_flag(a) for a in assets_flag]
            except click.BadParameter as exc:
                error(str(exc))
                return
        else:
            final_assets = _collect_assets_wizard(cfg)

        # Intent: flag wins, otherwise prompt.
        final_intent = (intent or "").strip()
        if not final_intent:
            final_intent = click.prompt("Intent (free text)", default="").strip()

        # Sources: flag wins; otherwise prompt for paths.
        source_paths = list(sources_flag) if sources_flag else _collect_sources_wizard()

        now = _utcnow()
        page_id = svc.create_draft(
            title=final_title,
            intent=final_intent,
            assets=final_assets,
            sources=[],
            created_by=None,
            now=now,
        )

        attached = _attach_sources(cfg, page_id, source_paths)
        for ref in attached:
            svc.store.history.attach_documentation_page_source(
                page_id,
                source_kind=ref.kind,
                source_path=ref.path,
                original_name=ref.original_name,
                created_at=now,
            )

        success(f"Created page {page_id}")

        if no_generate:
            info("Skipped LLM composition (--no-generate). Use /pages-edit to write the body.")
            return

        try:
            svc.generate(page_id, now=_utcnow())
        except Exception as exc:  # noqa: BLE001
            error(f"Generation failed: {exc}. Draft was saved; re-run with /pages-edit.")
            return

        page = svc.store.get(page_id)
        body = (page or {}).get("markdown_body", "") or ""
        preview = "\n".join(body.splitlines()[:10])
        info("Preview (first 10 lines):")
        click.echo(preview)

    @main.command("pages-list")
    @click.pass_obj
    def pages_list(cfg: AMXConfig) -> None:
        """List active documentation pages."""
        svc = _svc(cfg)
        rows = svc.store.list_active()
        if not rows:
            info("No pages yet. Create one with /pages-new.")
            return
        table = Table(title="Documentation pages", show_lines=False)
        table.add_column("ID")
        table.add_column("Title")
        table.add_column("Status")
        table.add_column("Updated")
        for r in rows:
            table.add_row(
                str(r.get("id", "")),
                str(r.get("title", "")),
                str(r.get("status", "")),
                str(r.get("updated_at", "")),
            )
        Console().print(table)

    @main.command("pages-show")
    @click.argument("page_id")
    @click.pass_obj
    def pages_show(cfg: AMXConfig, page_id: str) -> None:
        """Print the markdown body of a page to stdout."""
        svc = _svc(cfg)
        page = svc.store.get(page_id)
        if page is None:
            error(f"Page {page_id} not found.")
            return
        click.echo(page.get("markdown_body", "") or "")

    @main.command("pages-edit")
    @click.argument("page_id")
    @click.option("--note", default=None, help="Optional revision note.")
    @click.pass_obj
    def pages_edit(cfg: AMXConfig, page_id: str, note: str | None) -> None:
        """Open the page body in $EDITOR and save the result as a new revision."""
        svc = _svc(cfg)
        page = svc.store.get(page_id)
        if page is None:
            error(f"Page {page_id} not found.")
            return
        original = page.get("markdown_body", "") or ""

        import tempfile

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".md", delete=False, encoding="utf-8"
        ) as fh:
            fh.write(original)
            tmp_path = Path(fh.name)
        try:
            click.edit(filename=str(tmp_path), require_save=False, extension=".md")
            edited = tmp_path.read_text(encoding="utf-8")
        finally:
            try:
                tmp_path.unlink()
            except OSError:
                pass

        if edited == original:
            info("No changes — revision not saved.")
            return

        svc.save_revision(
            page_id,
            markdown_body=edited,
            now=_utcnow(),
            saved_by=None,
            note=note,
        )
        success(f"Saved new revision for {page_id}")

    @main.command("pages-export")
    @click.argument("page_id")
    @click.option(
        "--format",
        "fmt",
        type=click.Choice(["md", "pdf"]),
        required=True,
        help="Export format.",
    )
    @click.option(
        "--out",
        "out_path",
        type=click.Path(dir_okay=False, path_type=Path),
        default=None,
        help="Output file path (stdout when omitted).",
    )
    @click.pass_obj
    def pages_export(
        cfg: AMXConfig,
        page_id: str,
        fmt: str,
        out_path: Path | None,
    ) -> None:
        """Export a page as Markdown or PDF."""
        svc = _svc(cfg)
        try:
            payload = svc.export(page_id, fmt)  # type: ignore[arg-type]
        except KeyError:
            error(f"Page {page_id} not found.")
            return

        if out_path is not None:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(payload, bytes):
                out_path.write_bytes(payload)
            else:
                out_path.write_text(payload, encoding="utf-8")
            success(f"Exported page {page_id} -> {out_path}")
            return

        if isinstance(payload, bytes):
            sys.stdout.buffer.write(payload)
        else:
            click.echo(payload, nl=False)

    @main.command("pages-delete")
    @click.argument("page_id")
    @click.option(
        "--purge",
        is_flag=True,
        default=False,
        help="Hard-delete the page and all related rows.",
    )
    @click.pass_obj
    def pages_delete(cfg: AMXConfig, page_id: str, purge: bool) -> None:
        """Soft-delete a page (use --purge to hard-delete)."""
        svc = _svc(cfg)
        page = svc.store.get(page_id)
        if page is None:
            error(f"Page {page_id} not found.")
            return

        if purge:
            if not _hard_delete_page(page_id):
                error(f"Could not purge {page_id}.")
                return
            success(f"Purged page {page_id}")
            return

        svc.soft_delete(page_id, now=_utcnow())
        success(f"Soft-deleted page {page_id}")


__all__ = ["register_pages_commands"]
