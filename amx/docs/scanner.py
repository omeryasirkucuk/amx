"""Scan and ingest documents from multiple sources into a vector store for RAG."""

from __future__ import annotations

import base64
import os
import re
import shutil
import subprocess
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


def _download_to_file(url: str, dest: Path, *, timeout: int = 300) -> int:
    """Stream-download a URL to a local file; return byte count."""
    r = requests.get(url, timeout=timeout, stream=True, allow_redirects=True)
    if r.status_code >= 400:
        raise RuntimeError(f"Download failed {r.status_code}: {r.text[:300]}")
    n = 0
    with dest.open("wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)
                n += len(chunk)
    return n


def _gdrive_public_download(file_id: str, dest_dir: Path) -> DocInfo | None:
    """Try downloading a Google Drive file via the public export endpoint (no credentials)."""
    dl_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    sess = requests.Session()
    r = sess.get(dl_url, stream=True, timeout=60, allow_redirects=True)

    if r.status_code == 404:
        return None
    if r.status_code >= 400:
        return None

    cd = r.headers.get("Content-Disposition", "")
    ct = r.headers.get("Content-Type", "")

    if "text/html" in ct and "confirm" not in cd:
        html = r.content.decode("utf-8", errors="replace")
        m = re.search(r'id="download-form"[^>]*action="([^"]+)"', html)
        if not m:
            m = re.search(r'href="(/uc\?export=download[^"]*confirm=[^"]+)"', html)
        if m:
            confirm_url = m.group(1).replace("&amp;", "&")
            if confirm_url.startswith("/"):
                confirm_url = "https://drive.google.com" + confirm_url
            r = sess.get(confirm_url, stream=True, timeout=120, allow_redirects=True)
            cd = r.headers.get("Content-Disposition", "")
            ct = r.headers.get("Content-Type", "")

    if "text/html" in ct and "filename" not in cd:
        return None

    fname_match = re.search(r'filename="?([^";]+)"?', cd)
    fname = fname_match.group(1).strip() if fname_match else f"{file_id}.bin"
    ext = Path(fname).suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        log.warning("Drive file %r has unsupported extension %s", fname, ext)
        return None

    out = dest_dir / fname
    n = 0
    with out.open("wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)
                n += len(chunk)
    if n == 0:
        out.unlink(missing_ok=True)
        return None

    return DocInfo(str(out), n, ext, "drive")


def _gdrive_public_export(file_id: str, dest_dir: Path, fmt: str, ext: str) -> DocInfo | None:
    """Export a Google Workspace native file (Doc/Sheet/Slides) via public export URL."""
    export_url = f"https://docs.google.com/document/d/{file_id}/export?format={fmt}"
    if fmt == "csv":
        export_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv"
    elif fmt == "pptx":
        export_url = f"https://docs.google.com/presentation/d/{file_id}/export/{fmt}"

    out = dest_dir / f"{file_id}{ext}"
    try:
        n = _download_to_file(export_url, out)
    except Exception:
        return None
    if n == 0:
        out.unlink(missing_ok=True)
        return None
    return DocInfo(str(out), n, ext, "drive")


def _gdrive_has_api_credentials() -> bool:
    sa = os.environ.get("AMX_GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    tok = os.environ.get("AMX_GOOGLE_OAUTH_TOKEN_JSON", "").strip()
    return bool((sa and Path(sa).is_file()) or (tok and Path(tok).is_file()))


def _google_drive_credentials():
    """Return credentials for Drive API or raise with setup hints."""
    try:
        from google.oauth2 import service_account
        from google.oauth2.credentials import Credentials
    except ImportError as exc:
        raise RuntimeError(
            "Google Drive API requires google-auth. Install AMX with its full dependencies."
        ) from exc

    sa_path = os.environ.get("AMX_GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if sa_path and Path(sa_path).is_file():
        return service_account.Credentials.from_service_account_file(
            sa_path, scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
    token_path = os.environ.get("AMX_GOOGLE_OAUTH_TOKEN_JSON", "").strip()
    if token_path and Path(token_path).is_file():
        return Credentials.from_authorized_user_file(
            token_path, scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
    raise RuntimeError("No Drive API credentials configured.")


def _google_drive_build_service():
    try:
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
    except ImportError as exc:
        raise RuntimeError(
            "Google Drive API requires google-api-python-client."
        ) from exc
    creds = _google_drive_credentials()
    return build("drive", "v3", credentials=creds, cache_discovery=False), HttpError


def _download_google_drive_file_api(
    service: Any, HttpError: type, file_id: str, dest_dir: Path,
) -> Iterator[DocInfo]:
    try:
        meta = service.files().get(fileId=file_id, fields="id,name,mimeType,size").execute()
    except HttpError as exc:
        raise RuntimeError(f"Drive API files.get failed: {exc}") from exc

    name = str(meta.get("name") or file_id)
    mime = str(meta.get("mimeType") or "")

    if mime == "application/vnd.google-apps.folder":
        yield from _list_google_drive_folder_api(service, HttpError, file_id, dest_dir)
        return

    export_map = {
        "application/vnd.google-apps.document": ("application/pdf", ".pdf"),
        "application/vnd.google-apps.spreadsheet": ("text/csv", ".csv"),
        "application/vnd.google-apps.presentation": ("application/pdf", ".pdf"),
    }
    if mime in export_map:
        exp_mime, ext = export_map[mime]
        safe = re.sub(r"[^\w.\-]+", "_", Path(name).stem)[:120] or "export"
        out = dest_dir / f"{safe}{ext}"
        data = service.files().export_media(fileId=file_id, mimeType=exp_mime).execute()
        out.write_bytes(data)
        yield DocInfo(str(out), out.stat().st_size, ext, "drive")
        return

    ext = Path(name).suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        log.warning("Skipping Drive file %r (unsupported extension %s)", name, ext or "(none)")
        return
    out = dest_dir / Path(name).name
    data = service.files().get_media(fileId=file_id).execute()
    out.write_bytes(data)
    yield DocInfo(str(out), out.stat().st_size, ext, "drive")


def _list_google_drive_folder_api(
    service: Any, HttpError: type, folder_id: str, dest_dir: Path,
) -> Iterator[DocInfo]:
    page_token: str | None = None
    q = f"'{folder_id}' in parents and trashed = false"
    while True:
        resp = service.files().list(
            q=q, spaces="drive", fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token, pageSize=100,
        ).execute()
        for f in resp.get("files", []):
            yield from _download_google_drive_file_api(service, HttpError, f["id"], dest_dir)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break


def _resolve_google_drive(url: str, target_dir: str | None = None) -> Iterator[DocInfo]:
    dest = Path(target_dir or tempfile.mkdtemp(prefix="amx_gdrive_"))
    dest.mkdir(parents=True, exist_ok=True)

    folder_id = _parse_google_drive_folder_id(url)
    file_id = _parse_google_drive_file_id(url) if not folder_id else None

    if not folder_id and not file_id:
        raise RuntimeError(
            "Could not parse Google Drive URL. Use a link containing /folders/<id> or /d/<id>."
        )

    if file_id:
        doc = _gdrive_public_download(file_id, dest)
        if doc:
            log.info("Downloaded Drive file via public link (no credentials needed)")
            yield doc
            return

        for fmt, ext in [("pdf", ".pdf"), ("csv", ".csv")]:
            doc = _gdrive_public_export(file_id, dest, fmt, ext)
            if doc:
                log.info("Exported Google Workspace file via public export")
                yield doc
                return

    if _gdrive_has_api_credentials():
        service, HttpError = _google_drive_build_service()
        if folder_id:
            yield from _list_google_drive_folder_api(service, HttpError, folder_id, dest)
        elif file_id:
            yield from _download_google_drive_file_api(service, HttpError, file_id, dest)
        return

    hint = (
        "Could not download this Drive file publicly (it may be private or restricted). "
        "To access private files or folders, set one of:\n"
        "  AMX_GOOGLE_SERVICE_ACCOUNT_JSON — path to a service account JSON (share the file with it)\n"
        "  AMX_GOOGLE_OAUTH_TOKEN_JSON     — path to an OAuth user token JSON"
    )
    if folder_id:
        raise RuntimeError(
            "Google Drive folders require API credentials to list contents. " + hint
        )
    raise RuntimeError(hint)


# ── SharePoint / OneDrive ───────────────────────────────────────────────────


def _onedrive_try_public_download(url: str, dest_dir: Path) -> DocInfo | None:
    """Try to download a OneDrive/SharePoint sharing link via public redirect."""
    try:
        r = requests.head(url, allow_redirects=True, timeout=30)
    except Exception:
        return None

    cd = r.headers.get("Content-Disposition", "")
    ct = r.headers.get("Content-Type", "")
    fname_match = re.search(r'filename="?([^";]+)"?', cd)

    if not fname_match:
        download_url = url.split("?")[0]
        if download_url.endswith("/"):
            download_url = download_url.rstrip("/")
        if "download=1" not in url:
            sep = "&" if "?" in url else "?"
            download_url = url + sep + "download=1"
        try:
            r2 = requests.head(download_url, allow_redirects=True, timeout=30)
            cd = r2.headers.get("Content-Disposition", "")
            fname_match = re.search(r'filename="?([^";]+)"?', cd)
            if fname_match:
                url = download_url
        except Exception:
            pass

    if not fname_match:
        return None

    fname = fname_match.group(1).strip()
    ext = Path(fname).suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        log.warning("SharePoint file %r has unsupported extension %s", fname, ext)
        return None

    out = dest_dir / fname
    try:
        n = _download_to_file(url, out)
    except Exception:
        return None
    if n == 0:
        out.unlink(missing_ok=True)
        return None
    return DocInfo(str(out), n, ext, "sharepoint")


def _graph_has_credentials() -> bool:
    t = os.environ.get("AMX_AZURE_TENANT_ID", "").strip()
    c = os.environ.get("AMX_AZURE_CLIENT_ID", "").strip()
    s = os.environ.get("AMX_AZURE_CLIENT_SECRET", "").strip()
    return bool(t and c and s)


def _graph_app_token() -> str:
    tenant = os.environ.get("AMX_AZURE_TENANT_ID", "").strip()
    client_id = os.environ.get("AMX_AZURE_CLIENT_ID", "").strip()
    secret = os.environ.get("AMX_AZURE_CLIENT_SECRET", "").strip()
    try:
        import msal
    except ImportError as exc:
        raise RuntimeError(
            "SharePoint / OneDrive API requires msal."
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
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def _graph_get(url: str, token: str) -> dict[str, Any]:
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=120)
    if r.status_code >= 400:
        raise RuntimeError(f"Graph request failed {r.status_code}: {r.text[:500]}")
    return r.json()


def _download_graph_drive_item(
    token: str, drive_id: str, item_id: str, name_hint: str, dest_dir: Path,
) -> Iterator[DocInfo]:
    meta = _graph_get(
        f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}", token,
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
    size = _download_to_file(str(dl), out)
    yield DocInfo(str(out), size, ext, "sharepoint")


def _list_graph_folder(
    token: str, drive_id: str, folder_id: str, dest_dir: Path,
) -> Iterator[DocInfo]:
    url: str | None = (
        f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}/children?$top=200"
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
    dest = Path(target_dir or tempfile.mkdtemp(prefix="amx_sp_"))
    dest.mkdir(parents=True, exist_ok=True)

    doc = _onedrive_try_public_download(url, dest)
    if doc:
        log.info("Downloaded SharePoint/OneDrive file via public sharing link")
        yield doc
        return

    if _graph_has_credentials():
        token = _graph_app_token()
        enc = _graph_share_encode(url.strip())
        share = _graph_get(f"https://graph.microsoft.com/v1.0/shares/{enc}/driveItem", token)
        drive_id = share.get("parentReference", {}).get("driveId")
        item_id = share.get("id")
        name = str(share.get("name") or "root")
        if not drive_id or not item_id:
            raise RuntimeError("Graph share resolution returned no driveItem id")
        if share.get("folder") is not None:
            yield from _list_graph_folder(token, str(drive_id), str(item_id), dest)
        else:
            yield from _download_graph_drive_item(token, str(drive_id), str(item_id), name, dest)
        return

    raise RuntimeError(
        "Could not download this SharePoint/OneDrive file publicly (it may be private). "
        "To access private files, set:\n"
        "  AMX_AZURE_TENANT_ID\n"
        "  AMX_AZURE_CLIENT_ID\n"
        "  AMX_AZURE_CLIENT_SECRET\n"
        "with an Azure AD app registration that has Files.Read.All Graph permission."
    )


def test_git_remote_reachable(url: str) -> None:
    """Verify a Git remote exists and is readable (no clone). Raises RuntimeError on failure."""
    u = url.strip()
    if not u:
        raise RuntimeError("Empty Git URL")
    if not shutil.which("git"):
        raise RuntimeError("git is not installed — cannot verify Git remote URLs.")
    r = subprocess.run(
        ["git", "ls-remote", u],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if r.returncode != 0:
        err = (r.stderr or r.stdout or "").strip() or "git ls-remote failed"
        raise RuntimeError(err)


def _test_s3_reachable(uri: str) -> None:
    import boto3
    from botocore.exceptions import ClientError

    rest = uri.replace("s3://", "").strip()
    if not rest:
        raise RuntimeError("Invalid s3:// URI")
    bucket, _, prefix = rest.partition("/")
    bucket = bucket.strip()
    if not bucket:
        raise RuntimeError("Invalid s3:// URI (missing bucket)")
    s3 = boto3.client("s3")
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        raise RuntimeError(str(exc)) from exc
    if prefix.strip():
        pfx = prefix.strip()
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=pfx, MaxKeys=1)
        if not resp.get("Contents"):
            resp2 = s3.list_objects_v2(Bucket=bucket, Prefix=pfx.rstrip("/") + "/", MaxKeys=1)
            if not resp2.get("Contents"):
                raise RuntimeError(
                    f"No objects found under s3://{bucket}/{pfx} (bucket OK; check prefix)"
                )


def _test_local_reachable(path: str) -> None:
    p = Path(path).expanduser().resolve()
    if not p.exists():
        raise RuntimeError(f"Path does not exist: {p}")
    if not (p.is_dir() or p.is_file()):
        raise RuntimeError(f"Not a file or directory: {p}")


def _test_google_drive_reachable(url: str) -> None:
    folder_id = _parse_google_drive_folder_id(url)
    file_id = _parse_google_drive_file_id(url) if not folder_id else None
    if not folder_id and not file_id:
        raise RuntimeError(
            "Could not parse Google Drive URL (expected /folders/…, /d/…, or ?id=…)."
        )
    if file_id:
        u = f"https://drive.google.com/uc?export=download&id={file_id}"
        r = requests.head(u, allow_redirects=True, timeout=30)
        if r.status_code >= 400:
            raise RuntimeError(f"Drive file not reachable (HTTP {r.status_code})")
        return
    u = f"https://drive.google.com/drive/folders/{folder_id}"
    r = requests.head(u, allow_redirects=True, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Drive folder not reachable (HTTP {r.status_code})")


def _test_sharepoint_reachable(url: str) -> None:
    r = requests.head(url.strip(), allow_redirects=True, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"SharePoint/OneDrive URL not reachable (HTTP {r.status_code})")


def test_source_reachable(path: str) -> None:
    """Lightweight reachability check only (no clone, no full file listing). Raises RuntimeError on failure."""
    p = path.strip()
    if not p:
        raise RuntimeError("Empty path")
    if p.startswith("s3://"):
        _test_s3_reachable(p)
    elif p.startswith("https://github.com") or p.startswith("git@"):
        test_git_remote_reachable(p)
    elif p.startswith("http://") or p.startswith("https://"):
        if _is_google_drive_url(p):
            _test_google_drive_reachable(p)
        elif _is_sharepoint_or_onedrive_url(p):
            _test_sharepoint_reachable(p)
        else:
            raise RuntimeError(
                "Unsupported HTTP(S) document source (use Google Drive, SharePoint/OneDrive, or GitHub)."
            )
    else:
        _test_local_reachable(p)


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
