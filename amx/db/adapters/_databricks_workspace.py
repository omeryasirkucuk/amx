"""Thin HTTP client for Databricks Workspace + Jobs + Pipelines + SQL APIs.

Kept separate from ``databricks.py`` so the adapter remains focused on
metadata-style queries; this module owns all REST plumbing for the
asset-ingestion path.
"""

from __future__ import annotations

import base64
import logging
from collections.abc import Iterator
from typing import Any

import requests

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30  # seconds; one request


class DatabricksAuthError(RuntimeError):
    """401/403 from the Databricks REST API."""


class DatabricksApiError(RuntimeError):
    """Non-2xx that is not specifically auth-related."""


class DatabricksWorkspaceClient:
    def __init__(self, *, host: str, token: str, timeout: int = DEFAULT_TIMEOUT) -> None:
        self.host = host.rstrip("/")
        self._headers = {"Authorization": f"Bearer {token}"}
        self._timeout = timeout

    # ---- workspace ----------------------------------------------------

    def list_workspace_objects(self, *, path: str = "/") -> Iterator[dict[str, Any]]:
        """List every object under ``path`` with pagination support.

        Yields one dict per object as returned by the API.  Callers that need
        a full recursive walk should call this method for each DIRECTORY they
        encounter in the results.
        """
        for page in self._paginated_get(
            "/api/2.0/workspace/list",
            params={"path": path},
            page_token_field="next_page_token",
            items_field="objects",
        ):
            yield from page

    def export_notebook_source(self, *, workspace_path: str) -> str:
        """Return the notebook source text in SOURCE format (Databricks # COMMAND ---------- shape).

        Caller is responsible for normalizing to .ipynb via ``notebook_normalize``.
        """
        resp = self._get(
            "/api/2.0/workspace/export",
            params={"path": workspace_path, "format": "SOURCE"},
        )
        body = resp.json()
        return base64.b64decode(body["content"]).decode("utf-8")

    def path_for_object_id(self, object_id: str) -> str:
        """Resolve a Databricks workspace object_id back to its path.

        Used when callers store object_id as external_id but later need the
        path to drive the /export call.
        """
        resp = self._get("/api/2.0/workspace/get-status", params={"object_id": object_id})
        return resp.json()["path"]

    # ---- jobs ---------------------------------------------------------

    def list_jobs_full(self, *, runs_per_job: int = 20) -> Iterator[dict[str, Any]]:
        """Yield each job's full settings + ``recent_runs`` list.

        The list endpoint returns a thin record; we follow each with /get and /runs/list
        so callers receive everything in one pass.
        """
        for page in self._paginated_get(
            "/api/2.2/jobs/list",
            params={"limit": 25},
            page_token_field="next_page_token",
            items_field="jobs",
        ):
            for thin in page:
                job_id = thin["job_id"]
                full = self._get("/api/2.2/jobs/get", params={"job_id": job_id}).json()
                runs = self._get(
                    "/api/2.2/jobs/runs/list",
                    params={"job_id": job_id, "limit": runs_per_job, "expand_tasks": False},
                ).json().get("runs", [])
                full["recent_runs"] = runs
                yield full

    # ---- pipelines (DLT) ---------------------------------------------

    def list_pipelines(self) -> Iterator[dict[str, Any]]:
        for page in self._paginated_get(
            "/api/2.0/pipelines",
            params={"max_results": 25},
            page_token_field="next_page_token",
            items_field="statuses",
        ):
            for thin in page:
                pid = thin["pipeline_id"]
                full = self._get(f"/api/2.0/pipelines/{pid}").json()
                yield full

    # ---- SQL queries -------------------------------------------------

    def list_saved_queries(self) -> Iterator[dict[str, Any]]:
        for page in self._paginated_get(
            "/api/2.0/sql/queries",
            params={"page_size": 100},
            page_token_field=None,  # uses page/page_size
            items_field="results",
            page_param="page",
        ):
            yield from page

    def list_query_history(self, *, history_days: int, limit: int) -> Iterator[dict[str, Any]]:
        """Use /api/2.0/sql/history/queries with a start_time_ms filter."""
        import time
        start_ms = int((time.time() - history_days * 86400) * 1000)
        body = {
            "filter_by": {"query_start_time_range": {"start_time_ms": start_ms}},
            "max_results": min(limit, 1000),
            "include_metrics": False,
        }
        url = f"{self.host}/api/2.0/sql/history/queries"
        next_token: str | None = None
        yielded = 0
        while True:
            payload = dict(body)
            if next_token:
                payload["page_token"] = next_token
            resp = requests.get(url, headers=self._headers, json=payload, timeout=self._timeout)
            self._raise_if_error(resp)
            data = resp.json()
            for row in data.get("res", []):
                yield row
                yielded += 1
                if yielded >= limit:
                    return
            next_token = data.get("next_page_token")
            if not next_token:
                return

    # ---- internals ---------------------------------------------------

    def _get(self, path: str, *, params: dict[str, Any] | None = None) -> requests.Response:
        resp = requests.get(f"{self.host}{path}", headers=self._headers, params=params, timeout=self._timeout)
        self._raise_if_error(resp)
        return resp

    def _paginated_get(
        self,
        path: str,
        *,
        params: dict[str, Any],
        page_token_field: str | None,
        items_field: str,
        page_param: str = "page_token",
    ) -> Iterator[list[dict[str, Any]]]:
        current_params = dict(params)
        page_no = 1
        while True:
            if page_token_field is None and page_no > 1:
                current_params[page_param] = page_no
            resp = self._get(path, params=current_params)
            data = resp.json()
            items = data.get(items_field, [])
            if not items:
                return
            yield items
            if page_token_field is None:
                if len(items) < params.get("page_size", 100):
                    return
                page_no += 1
                continue
            token = data.get(page_token_field)
            if not token:
                return
            current_params[page_param] = token

    @staticmethod
    def _raise_if_error(resp: requests.Response) -> None:
        if resp.status_code in (401, 403):
            raise DatabricksAuthError(
                f"Databricks API rejected the request ({resp.status_code}): {resp.text[:200]}"
            )
        if resp.status_code >= 400:
            raise DatabricksApiError(
                f"Databricks API error {resp.status_code} on {resp.url}: {resp.text[:200]}"
            )
