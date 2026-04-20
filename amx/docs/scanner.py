"""Scan and ingest documents from multiple sources into a vector store for RAG."""

from __future__ import annotations

import base64
import os
import re
import tempfile
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import requests

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


def _is_google_drive_url(url: str) -> bool:
    u = url.lower()
    return "drive.google.com" in u or "docs.google.com" in u


def _is_sharepoint_or_onedrive_url(url: str) -> bool:
    u = url.lower()
    return "sharepoint.com" in u or "onedrive.live.com" in u


def _parse_google_drive_file_id(url: str) -> str | None:
    """Extract file id from common Drive / Docs URL shapes."""
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    if "id" in qs and qs["id"]:
        return qs["id"][0]
    m = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
    if m:
        return m.group(1)
    return None


def _parse_google_drive_folder_id(url: str) -> str | None:
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    if m:
        return m.group(1)
    return None


def _google_drive_credentials():
    """Return credentials for Drive API or raise with setup hints."""
    try:
        from google.oauth2 import service_account
        from google.oauth2.credentials import Credentials
    except ImportError as exc:
        raise RuntimeError(
            "Google Drive support requires google-auth. Install AMX with its full dependencies."
        ) from exc

    sa_path = os.environ.get("AMX_GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if sa_path and Path(sa_path).is_file():
        return service_account.Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )

    token_path = os.environ.get("AMX_GOOGLE_OAUTH_TOKEN_JSON", "").strip()
    if token_path and Path(token_path).is_file():
        return Credentials.from_authorized_user_file(
            token_path,
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )

    raise RuntimeError(
        "Google Drive: set AMX_GOOGLE_SERVICE_ACCOUNT_JSON (service account JSON path) "
        "or AMX_GOOGLE_OAUTH_TOKEN_JSON (OAuth token JSON from a prior consent flow). "
        "The Drive must be shared with that principal."
    )


def _google_drive_build_service():
    try:
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
    except ImportError as exc:
        raise RuntimeError(
            "Google Drive support requires google-api-python-client. "
            "Install AMX with its full dependencies."
        ) from exc

    creds = _google_drive_credentials()
    return build("drive", "v3", credentials=creds, cache_discovery=False), HttpError


def _google_mime_to_export_extension(mime: str) -> tuple[str, str]:
    """Return (export_mime, file_extension) for Google Workspace native files."""
    if mime == "application/vnd.google-apps.document":
        return "application/pdf", ".pdf"
    if mime == "application/vnd.google-apps.spreadsheet":
        return "text/csv", ".csv"
    if mime == "application/vnd.google-apps.presentation":
        return "application/pdf", ".pdf"
    return "", ""


def _download_google_drive_file(
    service: Any,
    HttpError: type,
    file_id: str,
    dest_dir: Path,
) -> Iterator[DocInfo]:
    try:
        meta = service.files().get(fileId=file_id, fields="id,name,mimeType,size").execute()
    except HttpError as exc:
        raise RuntimeError(f"Drive API files.get failed: {exc}") from exc

    name = str(meta.get("name") or file_id)
    mime = str(meta.get("mimeType") or "")

    if mime == "application/vnd.google-apps.folder":
        yield from _list_google_drive_folder(service, HttpError, file_id, dest_dir)
        return

    export_mime, ext = _google_mime_to_export_extension(mime)
    if export_mime:
        safe = re.sub(r"[^\w.\-]+", "_", Path(name).stem)[:120] or "export"
        out = dest_dir / f"{safe}{ext}"
        try:
            data = service.files().export_media(fileId=file_id, mimeType=export_mime).execute()
        except HttpError as exc:
            raise RuntimeError(f"Drive export failed for {name!r}: {exc}") from exc
        out.write_bytes(data)
        yield DocInfo(str(out), out.stat().st_size, ext, "drive")
        return

    ext = Path(name).suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        log.warning("Skipping Drive file %r (unsupported extension %s)", name, ext or "(none)")
        return

    out = dest_dir / Path(name).name
    try:
        data = service.files().get_media(fileId=file_id).execute()
    except HttpError as exc:
        raise RuntimeError(f"Drive download failed for {name!r}: {exc}") from exc
    out.write_bytes(data)
    yield DocInfo(str(out), out.stat().st_size, ext, "drive")


def _list_google_drive_folder(
    service: Any,
    HttpError: type,
    folder_id: str,
    dest_dir: Path,
) -> Iterator[DocInfo]:
    page_token: str | None = None
    q = f"'{folder_id}' in parents and trashed = false"
    while True:
        try:
            resp = (
                service.files()
                .list(
                    q=q,
                    spaces="drive",
                    fields="nextPageToken, files(id, name, mimeType)",
                    pageToken=page_token,
                    pageSize=100,
                )
                .execute()
            )
        except HttpError as exc:
            raise RuntimeError(f"Drive API files.list failed: {exc}") from exc
        for f in resp.get("files", []):
            fid = f["id"]
            yield from _download_google_drive_file(service, HttpError, fid, dest_dir)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break


def _resolve_google_drive(url: str, target_dir: str | None = None) -> Iterator[DocInfo]:
    dest = Path(target_dir or tempfile.mkdtemp(prefix="amx_gdrive_"))
    dest.mkdir(parents=True, exist_ok=True)
    service, HttpError = _google_drive_build_service()

    folder_id = _parse_google_drive_folder_id(url)
    if folder_id:
        yield from _list_google_drive_folder(service, HttpError, folder_id, dest)
        return

    file_id = _parse_google_drive_file_id(url)
    if file_id:
        yield from _download_google_drive_file(service, HttpError, file_id, dest)
        return

    raise RuntimeError(
        "Could not parse Google Drive URL. Use a link that contains /folders/<id> or /d/<id> "
        "or ?id=<id>."
    )


def _graph_app_token() -> str:
    tenant = os.environ.get("AMX_AZURE_TENANT_ID", "").strip()
    client_id = os.environ.get("AMX_AZURE_CLIENT_ID", "").strip()
    secret = os.environ.get("AMX_AZURE_CLIENT_SECRET", "").strip()
    if not (tenant and client_id and secret):
        raise RuntimeError(
            "SharePoint / OneDrive (Graph): set AMX_AZURE_TENANT_ID, AMX_AZURE_CLIENT_ID, "
            "and AMX_AZURE_CLIENT_SECRET for application (client credentials) access. "
            "Grant Microsoft Graph application permissions: Files.Read.All (and Sites.Read.All "
            "if you use sharepoint.com links)."
        )
    try:
        import msal
    except ImportError as exc:
        raise RuntimeError(
            "SharePoint / OneDrive support requires msal. Install AMX with its full dependencies."
        ) from exc

    app = msal.ConfidentialClientApplication(
        client_id,
        authority=f"https://login.microsoftonline.com/{tenant}",
        client_credential=secret,
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if not result or "access_token" not in result:
        err = result.get("error_description") if result else "unknown"
        raise RuntimeError(f"MSAL token acquisition failed: {err}")
    return str(result["access_token"])


def _graph_share_encode(url: str) -> str:
    raw = f"u!{url}"
    b64 = base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")
    return b64


def _graph_get(url: str, token: str) -> dict[str, Any]:
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=120,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Graph request failed {r.status_code}: {r.text[:500]}")
    return r.json()


def _graph_download_file(download_url: str, dest: Path) -> int:
    r = requests.get(download_url, timeout=300, stream=True)
    if r.status_code >= 400:
        raise RuntimeError(f"Download failed {r.status_code}: {r.text[:300]}")
    n = 0
    with dest.open("wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)
                n += len(chunk)
    return n


def _download_graph_drive_item(
    token: str,
    drive_id: str,
    item_id: str,
    name_hint: str,
    dest_dir: Path,
) -> Iterator[DocInfo]:
    meta = _graph_get(
        f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}",
        token,
    )
    name = str(meta.get("name") or name_hint)

    ext = Path(name).suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        log.warning("Skipping Graph item %r (unsupported extension %s)", name, ext or "(none)")
        return

    dl = meta.get("@microsoft.graph.downloadUrl")
    if not dl:
        raise RuntimeError(f"No download URL for {name!r}")
    out = dest_dir / Path(name).name
    size = _graph_download_file(str(dl), out)
    yield DocInfo(str(out), size, ext, "sharepoint")


def _list_graph_folder(
    token: str,
    drive_id: str,
    folder_id: str,
    dest_dir: Path,
) -> Iterator[DocInfo]:
    url = (
        f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}/children"
        f"?$top=200"
    )
    while url:
        data = _graph_get(url, token)
        for item in data.get("value", []):
            iid = item["id"]
            name = str(item.get("name") or iid)
            if item.get("folder") is not None:
                yield from _list_graph_folder(token, drive_id, iid, dest_dir)
            else:
                yield from _download_graph_drive_item(token, drive_id, iid, name, dest_dir)
        url = data.get("@odata.nextLink")


def _resolve_sharepoint_or_onedrive(url: str, target_dir: str | None = None) -> Iterator[DocInfo]:
    token = _graph_app_token()
    enc = _graph_share_encode(url.strip())
    share = _graph_get(
        f"https://graph.microsoft.com/v1.0/shares/{enc}/driveItem",
        token,
    )
    drive_id = share.get("parentReference", {}).get("driveId")
    item_id = share.get("id")
    name = str(share.get("name") or "root")
    if not drive_id or not item_id:
        raise RuntimeError("Graph share resolution returned no driveItem id")

    dest = Path(target_dir or tempfile.mkdtemp(prefix="amx_sp_"))
    dest.mkdir(parents=True, exist_ok=True)

    if share.get("folder") is not None:
        yield from _list_graph_folder(token, str(drive_id), str(item_id), dest)
    else:
        yield from _download_graph_drive_item(
            token, str(drive_id), str(item_id), name, dest
        )


def scan_source(path: str) -> list[DocInfo]:
    if path.startswith("s3://"):
        return list(_resolve_s3(path))
    if path.startswith("https://github.com") or path.startswith("git@"):
        return list(_resolve_github(path))
    if path.startswith("http://") or path.startswith("https://"):
        if _is_google_drive_url(path):
            return list(_resolve_google_drive(path))
        if _is_sharepoint_or_onedrive_url(path):
            return list(_resolve_sharepoint_or_onedrive(path))
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
