"""On-demand pip-installer for feature-gated dependencies.

A first-time ``pip install amx-cli`` ships:

- The CLI shell, LLM client (litellm), config, history store.
- AMX Studio (FastAPI + uvicorn + sse-starlette + python-multipart).
- A working DuckDB driver pair so a local profile is queryable
  without a second download.

Heavier clusters that not every user touches — chromadb + langchain
for RAG, ``boto3`` / ``google`` / ``msal`` for cloud document sources,
``openai`` / ``anthropic`` SDKs for Batch APIs, the niche DB drivers
(Snowflake / Databricks / BigQuery / Postgres / MySQL / …), and the
heavy ML bundles (sentence-transformers, bert-score) — are pulled
in transparently the first time the relevant feature runs.

Why this design:

- A fresh install completes quickly instead of pulling 500+ MB of
  wheels for features the user may never touch.
- Users never have to learn extras syntax (``amx-cli[rag,docs]``) to
  make their tool work — AMX is the one that knows what each feature
  needs.
- Already-installed packages are a single ``importlib.find_spec``
  no-op, so steady-state launches stay fast.
- Failure modes are surfaced explicitly: when a fetch fails (offline,
  read-only env, permission denied), the user sees one error line
  with the exact ``pip install`` command to run by hand. We never
  half-install and crash deeper in the import chain.

Two ways to call ``ensure()``:

1. **Bundle name (preferred for shared clusters).** The bundle
   registry below is the single place where "what does the RAG
   feature need" is recorded, so adding a package once propagates
   to every entry point::

       from amx.utils.optional_deps import ensure
       ensure("rag")  # docs / search / code RAG share this set
       import chromadb  # now safe

2. **Inline list (for one-off / per-backend triples).** Used by
   ``amx.db.drivers`` where each backend already has its own pair
   table, and by per-feature one-package needs like ``bert-score``::

       ensure(
           [("docx", "python-docx"), "chromadb"],
           feature="document RAG",
       )

The bare-string form inside the list is used when the importable
module name matches the pip distribution name (``chromadb`` →
``pip install chromadb``). The two-tuple form is used when they
differ (``import docx`` is shipped by ``python-docx``) or when
extras / version pins are needed (``("uvicorn", "uvicorn[standard]")``).
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import subprocess
import sys
import threading
from collections.abc import Iterable

#: Cache keys for module/pip pairs we have already verified or
#: installed in this process. Keeps hot lazy-import paths from
#: re-running ``find_spec`` once a feature has been used.
_VERIFIED: set[str] = set()

#: Single process-wide lock so two concurrent CLI flows (e.g. Studio's
#: HTTP worker + the REPL's foreground command) cannot race a pip
#: install for the same bundle. Without this, two parallel ``ensure``
#: calls for ``"rag"`` would both observe missing packages, both spawn
#: pip, and the second invocation could see an inconsistent on-disk
#: state mid-install.
_INSTALL_LOCK = threading.Lock()

PackageSpec = str | tuple[str, str]

#: Curated multi-package bundles. Adding a new feature cluster?
#: Update the dict in one place — every callsite that names the
#: bundle picks up the change automatically.
#:
#: Keys here intentionally line up with extras names in
#: ``pyproject.toml`` so a user who wants to skip the auto-install
#: prompt can still ``pip install amx-cli[<bundle>]``.
BUNDLES: dict[str, list[PackageSpec]] = {
    # Shared RAG core: chromadb + splitter + tiktoken. The /docs,
    # /search, and /code ingest entry points all touch this set, so
    # the first feature the user reaches pays the install once and
    # the others reuse the cache.
    "rag": [
        "chromadb",
        ("langchain_text_splitters", "langchain-text-splitters"),
        "tiktoken",
    ],
    # Document loaders on top of ``rag``. Only the /docs ingest path
    # needs these — search and code RAG do not.
    "docs-extended": [
        "chromadb",
        ("langchain_community", "langchain-community"),
        ("langchain_text_splitters", "langchain-text-splitters"),
        "tiktoken",
        "unstructured",
        "pypdf",
        ("docx", "python-docx"),
        "openpyxl",
    ],
}

#: Human-readable labels surfaced in the install banner when a bundle
#: is invoked by name. Falls back to the bundle key if absent.
BUNDLE_LABELS: dict[str, str] = {
    "rag": "shared RAG core (/docs, /search, /code)",
    "docs-extended": "document RAG (/docs ingest)",
}

#: Bundles whose total install footprint is large enough (sentence-
#: transformers / bert-score pull torch + transformers + roberta-large
#: weights — well over 1 GB) that we want a one-time interactive
#: confirmation before kicking off the download. Skipped automatically
#: when stdin is not a TTY (Studio worker, CI) or when the
#: ``AMX_AUTO_INSTALL=1`` opt-out is set.
_LARGE_PIP_TARGETS: frozenset[str] = frozenset(
    {
        "sentence-transformers",
        "bert-score",
    }
)


def _resolve(spec: PackageSpec) -> tuple[str, str]:
    """Return ``(importable_module_name, pip_install_target)``."""
    if isinstance(spec, str):
        return spec, spec
    return spec


def _confirm_large_install(missing_pip: list[str], feature: str) -> bool:
    """Prompt before fetching a known-large bundle.

    Returns ``True`` to proceed. Returns ``False`` only when the user
    explicitly declines at an interactive prompt; non-interactive
    callers (Studio HTTP worker, CI, AMX_AUTO_INSTALL=1) always
    proceed, since interrupting them mid-flow would leave the request
    half-handled with no recoverable surface.
    """
    if not any(pkg in _LARGE_PIP_TARGETS for pkg in missing_pip):
        return True
    if os.environ.get("AMX_AUTO_INSTALL", "").lower() in {"1", "true", "yes"}:
        return True
    if not sys.stdin.isatty():
        return True
    try:
        from amx.utils.console import confirm
    except Exception:
        return True
    return confirm(
        f"{feature} needs to download a large bundle (>500 MB of model + ML wheels). Continue?",
        default=True,
    )


def ensure(
    packages: str | Iterable[PackageSpec],
    *,
    feature: str | None = None,
) -> None:
    """Make sure each package the feature needs is importable.

    *packages* is either:

    - The name of a bundle in :data:`BUNDLES` (e.g. ``"rag"``), in
      which case the curated package list and a default *feature*
      label are looked up in the registry, OR
    - An iterable of :data:`PackageSpec` entries (the legacy
      per-callsite form, used by ``amx.db.drivers`` and the few
      one-package callsites).

    Missing packages are pip-installed in a child process targeting
    ``sys.executable`` so the install lands in the same interpreter
    that's running AMX (avoids the system-pip-vs-venv-pip footgun).
    The whole bundle goes through ONE pip subprocess — never one
    invocation per package — so the user sees a single download phase
    instead of N consecutive starts.

    Concurrent ``ensure`` calls (e.g. Studio's HTTP worker fielding a
    request while the REPL handles another command) are serialised
    by a process-wide lock so two threads cannot race the same pip
    install.

    Raises ``RuntimeError`` if pip install fails. The exception
    message includes the exact command for the user to run by hand,
    so the error is recoverable without grepping the source.
    """
    if isinstance(packages, str):
        bundle_name = packages
        try:
            specs: Iterable[PackageSpec] = BUNDLES[bundle_name]
        except KeyError as exc:
            raise KeyError(
                f"Unknown ensure() bundle: {bundle_name!r}. Known bundles: {sorted(BUNDLES)}"
            ) from exc
        if feature is None:
            feature = BUNDLE_LABELS.get(bundle_name, bundle_name)
    else:
        specs = packages

    if feature is None:
        raise TypeError("ensure() requires a feature label when called with a package list")

    missing_pip: list[str] = []
    seen_keys: list[str] = []
    for spec in specs:
        module_name, pip_name = _resolve(spec)
        cache_key = f"{module_name}|{pip_name}"
        seen_keys.append(cache_key)
        if cache_key in _VERIFIED:
            continue
        # Honour test fixtures that inject ``SimpleNamespace`` doubles
        # via ``sys.modules[name] = …``. ``find_spec`` would raise
        # ``ValueError: git.__spec__ is not set`` for those because
        # SimpleNamespace has no ``__spec__`` attribute. Treating the
        # presence of the name in ``sys.modules`` as "importable"
        # mirrors the behaviour of a real ``import`` statement, which
        # the production code paths actually rely on.
        if module_name in sys.modules:
            _VERIFIED.add(cache_key)
            continue
        try:
            spec_obj = importlib.util.find_spec(module_name)
        except (ValueError, ModuleNotFoundError):
            spec_obj = None
        if spec_obj is not None:
            _VERIFIED.add(cache_key)
            continue
        missing_pip.append(pip_name)

    if not missing_pip:
        return

    with _INSTALL_LOCK:
        # Re-check inside the lock: another thread may have installed
        # the bundle while we were waiting. Without this every queued
        # caller would still spawn its own pip subprocess for an
        # already-satisfied set.
        still_missing: list[str] = []
        for pip_name in missing_pip:
            module_name = next(
                (_resolve(s)[0] for s in specs if _resolve(s)[1] == pip_name),
                pip_name,
            )
            cache_key = f"{module_name}|{pip_name}"
            if cache_key in _VERIFIED:
                continue
            if module_name in sys.modules:
                _VERIFIED.add(cache_key)
                continue
            try:
                spec_obj = importlib.util.find_spec(module_name)
            except (ValueError, ModuleNotFoundError):
                spec_obj = None
            if spec_obj is not None:
                _VERIFIED.add(cache_key)
                continue
            still_missing.append(pip_name)

        if not still_missing:
            return

        if not _confirm_large_install(still_missing, feature):
            for key in seen_keys:
                _VERIFIED.discard(key)
            raise RuntimeError(
                f"User declined to install the {feature} bundle. "
                f"Run manually when ready: pip install {' '.join(still_missing)}"
            )

        # Lazy console import — ``optional_deps`` itself sits at the
        # top of feature modules, and we want it to stay cheap to load.
        from amx.utils.console import info, success

        info(
            f"First-time setup for {feature} — installing "
            f"{len(still_missing)} package{'s' if len(still_missing) > 1 else ''}: "
            f"{', '.join(still_missing)}"
        )
        info("This downloads from PyPI; pip output streams below so you see progress.")
        cmd = [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            *still_missing,
        ]
        # Deliberately NO ``capture_output=True`` and NO ``--quiet``: a
        # multi-package install (langchain-community + unstructured +
        # pypdf is ~80 MB / 30+ s on a fresh machine) with captured
        # output looks like the CLI froze. Streaming pip's native
        # progress bars to the user's terminal is the same UX they
        # already know from any other ``pip install`` and removes the
        # "did it crash?" doubt that captured output produces.
        try:
            proc = subprocess.run(cmd, check=False)
        except OSError as exc:
            manual = "pip install " + " ".join(still_missing)
            raise RuntimeError(f"Could not invoke pip ({exc}). Install manually: {manual}") from exc

        if proc.returncode != 0:
            for key in seen_keys:
                _VERIFIED.discard(key)
            manual = "pip install " + " ".join(still_missing)
            raise RuntimeError(
                f"pip install failed (exit code {proc.returncode}). Run manually: {manual}"
            )

        # Newly-installed packages weren't on sys.path at process start;
        # invalidate finder caches so the upcoming ``import`` picks them up.
        importlib.invalidate_caches()
        for key in seen_keys:
            _VERIFIED.add(key)
        success(f"Installed: {', '.join(still_missing)}.")
