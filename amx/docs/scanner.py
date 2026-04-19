"""Scan and ingest documents from multiple sources into a vector store for RAG."""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from amx.utils.logging import get_logger

log = get_logger("docs.scanner")

SUPPORTED_EXTENSIONS = {
    ".pdf", ".docx", ".doc", ".txt", ".md", ".csv", ".xlsx", ".xls",
    ".html", ".htm", ".json", ".yaml", ".yml", ".rst", ".rtf", ".pptx",
}


@dataclass
class DocInfo:
    path: str
    size_bytes: int
    extension: str
    source_type: str  # local | github | s3 | gcs | azure | sharepoint | drive


def _resolve_local(path: str) -> Iterator[DocInfo]:
    p = Path(path).expanduser().resolve()
    if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS:
        yield DocInfo(str(p), p.stat().st_size, p.suffix.lower(), "local")
    elif p.is_dir():
        for f in sorted(p.rglob("*")):
            if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS:
                yield DocInfo(str(f), f.stat().st_size, f.suffix.lower(), "local")


def _resolve_github(url: str, target_dir: str | None = None) -> Iterator[DocInfo]:
    """Clone a GitHub repo to a temp dir and scan files."""
    import git as gitpython

    dest = target_dir or tempfile.mkdtemp(prefix="amx_gh_")
    log.info("Cloning %s → %s", url, dest)
    gitpython.Repo.clone_from(url, dest, depth=1)
    yield from _resolve_local(dest)


def _resolve_s3(uri: str, target_dir: str | None = None) -> Iterator[DocInfo]:
    import boto3

    dest = Path(target_dir or tempfile.mkdtemp(prefix="amx_s3_"))
    dest.mkdir(parents=True, exist_ok=True)
    parts = uri.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            ext = Path(key).suffix.lower()
            if ext in SUPPORTED_EXTENSIONS:
                local_path = dest / Path(key).name
                s3.download_file(bucket, key, str(local_path))
                yield DocInfo(str(local_path), obj["Size"], ext, "s3")


def scan_source(path: str) -> list[DocInfo]:
    if path.startswith("s3://"):
        return list(_resolve_s3(path))
    if path.startswith("https://github.com") or path.startswith("git@"):
        return list(_resolve_github(path))
    return list(_resolve_local(path))


def scan_all_sources(paths: list[str]) -> list[DocInfo]:
    all_docs: list[DocInfo] = []
    for p in paths:
        try:
            all_docs.extend(scan_source(p))
        except Exception as exc:
            log.error("Failed to scan %s: %s", p, exc)
    return all_docs


def total_size_mb(docs: list[DocInfo]) -> float:
    return sum(d.size_bytes for d in docs) / (1024 * 1024)
