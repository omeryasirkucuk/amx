"""Persist codebase scan results under ~/.amx/code_cache for reuse across /run."""

from __future__ import annotations

import hashlib
import json
import subprocess
import time
from pathlib import Path
from typing import Any

from amx.codebase.analyzer import CodeReference, CodebaseReport
from amx.docs.scanner import normalize_github_url
from amx.utils.logging import get_logger

log = get_logger("codebase.cache")

CACHE_VERSION = 1
CACHE_ROOT = Path.home() / ".amx" / "code_cache"


def _slug(profile: str, source: str) -> str:
    raw = f"{profile}|{normalize_github_url(source)}"
    h = hashlib.sha256(raw.encode()).hexdigest()[:20]
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in profile)[:40]
    return f"{safe}_{h}"


def _remote_head_sha(url: str) -> str | None:
    u = normalize_github_url(url.strip())
    if not u.startswith("http") and not u.startswith("git@"):
        return None
    try:
        r = subprocess.run(
            ["git", "ls-remote", u, "HEAD"],
            capture_output=True,
            text=True,
            timeout=90,
        )
        if r.returncode != 0 or not r.stdout.strip():
            return None
        line = r.stdout.splitlines()[0].split()
        return line[0] if line else None
    except Exception as exc:
        log.warning("git ls-remote for cache manifest: %s", exc)
        return None


def _refs_to_json(refs: dict[str, list[CodeReference]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for k, lst in refs.items():
        out[k] = [
            {
                "file": r.file,
                "line_no": r.line_no,
                "line_text": r.line_text,
                "matched_asset": r.matched_asset,
                "context": r.context,
            }
            for r in lst
        ]
    return out


def _refs_from_json(data: dict[str, list[dict[str, Any]]]) -> dict[str, list[CodeReference]]:
    out: dict[str, list[CodeReference]] = {}
    for k, lst in data.items():
        out[k] = [
            CodeReference(
                file=d["file"],
                line_no=int(d["line_no"]),
                line_text=d["line_text"],
                matched_asset=d["matched_asset"],
                context=d.get("context", ""),
            )
            for d in lst
        ]
    return out


def report_to_dict(report: CodebaseReport) -> dict[str, Any]:
    return {
        "path": report.path,
        "total_files": report.total_files,
        "scanned_files": report.scanned_files,
        "references": _refs_to_json(report.references),
        "external_mentions": _refs_to_json(report.external_mentions),
    }


def report_from_dict(data: dict[str, Any]) -> CodebaseReport:
    return CodebaseReport(
        path=data["path"],
        total_files=int(data.get("total_files", 0)),
        scanned_files=int(data.get("scanned_files", 0)),
        references=_refs_from_json(data.get("references", {})),
        external_mentions=_refs_from_json(data.get("external_mentions", {})),
    )


def asset_fingerprint(schema: str, tables: list[str], column_names: list[str]) -> str:
    t = "|".join(sorted(tables))
    c = "|".join(sorted(column_names))
    return hashlib.sha256(f"{schema}|{t}|{c}".encode()).hexdigest()


def save_cached_report(
    *,
    profile_name: str,
    source_path: str,
    schema: str,
    tables: list[str],
    column_names: list[str],
    report: CodebaseReport,
) -> Path:
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    slug = _slug(profile_name or "default", source_path)
    dirp = CACHE_ROOT / slug
    dirp.mkdir(parents=True, exist_ok=True)

    afp = asset_fingerprint(schema, tables, column_names)
    head = _remote_head_sha(source_path) if source_path.startswith(("http", "git@")) else None

    manifest = {
        "version": CACHE_VERSION,
        "profile_name": profile_name,
        "source_path": source_path,
        "normalized_source": normalize_github_url(source_path),
        "schema": schema,
        "tables": sorted(tables),
        "asset_fingerprint": afp,
        "remote_head": head,
        "scanned_at": int(time.time()),
    }
    (dirp / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (dirp / "report.json").write_text(json.dumps(report_to_dict(report), indent=2), encoding="utf-8")
    log.info("Saved codebase cache under %s", dirp)
    return dirp


def load_cached_report(
    *,
    profile_name: str,
    source_path: str,
    schema: str,
    tables: list[str],
    column_names: list[str],
    force_refresh: bool = False,
    max_age_days: int = 7,
) -> CodebaseReport | None:
    if force_refresh:
        return None

    slug = _slug(profile_name or "default", source_path)
    dirp = CACHE_ROOT / slug
    man_path = dirp / "manifest.json"
    rep_path = dirp / "report.json"
    if not man_path.is_file() or not rep_path.is_file():
        return None

    try:
        manifest = json.loads(man_path.read_text(encoding="utf-8"))
        if int(manifest.get("version", 0)) != CACHE_VERSION:
            return None
        if manifest.get("schema") != schema:
            return None
        if manifest.get("tables") != sorted(tables):
            return None
        if manifest.get("asset_fingerprint") != asset_fingerprint(schema, tables, column_names):
            return None

        age = int(time.time()) - int(manifest.get("scanned_at", 0))
        if age > max_age_days * 86400:
            log.info("Codebase cache expired (age %ds)", age)
            return None

        if source_path.startswith(("http", "git@")):
            head_now = _remote_head_sha(source_path)
            head_old = manifest.get("remote_head")
            if head_now and head_old and head_now != head_old:
                log.info("Remote HEAD changed; invalidating codebase cache")
                return None

        data = json.loads(rep_path.read_text(encoding="utf-8"))
        return report_from_dict(data)
    except Exception as exc:
        log.warning("Failed to load codebase cache: %s", exc)
        return None


def load_latest_cached_report(
    profile_name: str,
    source_path: str,
) -> tuple[dict[str, Any] | None, CodebaseReport | None]:
    """Load the most recent cached report for a profile/path without fingerprint checks.

    Returns ``(manifest_dict, report)`` or ``(None, None)`` if nothing cached.
    """
    slug = _slug(profile_name or "default", source_path)
    dirp = CACHE_ROOT / slug
    man_path = dirp / "manifest.json"
    rep_path = dirp / "report.json"
    if not man_path.is_file() or not rep_path.is_file():
        return None, None
    try:
        manifest = json.loads(man_path.read_text(encoding="utf-8"))
        data = json.loads(rep_path.read_text(encoding="utf-8"))
        return manifest, report_from_dict(data)
    except Exception:
        return None, None


def invalidate_cache(profile_name: str, source_path: str) -> bool:
    slug = _slug(profile_name or "default", source_path)
    dirp = CACHE_ROOT / slug
    if not dirp.is_dir():
        return False
    for p in dirp.iterdir():
        p.unlink(missing_ok=True)
    try:
        dirp.rmdir()
    except OSError:
        pass
    log.info("Invalidated codebase cache %s", dirp)
    return True
