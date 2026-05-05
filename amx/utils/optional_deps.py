"""On-demand pip-installer for feature-gated dependencies.

A first-time ``pip install amx-cli`` ships a thin core only — the CLI
shell, LLM client (litellm), config, and history store. Heavy
clusters that not every user will touch — chromadb + langchain for
RAG, fastapi/uvicorn for AMX Studio, boto3/google/msal for cloud
document sources, openai/anthropic SDKs for Batch APIs — are pulled
in transparently the first time the relevant feature runs.

Why this design:

- A fresh install completes in seconds instead of pulling 300+ MB of
  wheels for features the user may never touch.
- Users never have to learn extras syntax (``amx-cli[rag,docs]``) to
  make their tool work — AMX is the one that knows what each feature
  needs.
- Already-installed packages are a single ``importlib.find_spec``
  no-op, so steady-state launches stay fast.
- Failure modes are surfaced explicitly: when pip install fails
  (offline, read-only env, permission denied), the user sees one
  error line with the exact ``pip install`` command to run by hand.
  We never half-install and crash deeper in the import chain.

Usage from a feature module — ``ensure()`` runs BEFORE the heavy
imports execute, so the imports below it are guaranteed to resolve::

    # amx/docs/rag.py
    from amx.utils.optional_deps import ensure
    ensure(
        [
            "chromadb",
            ("langchain_community", "langchain-community"),
            ("langchain_text_splitters", "langchain-text-splitters"),
            ("docx", "python-docx"),
        ],
        feature="document RAG (/docs)",
    )

    import chromadb  # now safe
    from langchain_community.document_loaders import PyPDFLoader

The bare-string form is used when the importable module name matches
the pip distribution name (``chromadb`` → ``pip install chromadb``).
The two-tuple form is used when they differ (``import docx`` is
shipped by ``python-docx``) or when extras / version pins are needed
(``("uvicorn", "uvicorn[standard]")``).
"""

from __future__ import annotations

import importlib
import importlib.util
import subprocess
import sys
from collections.abc import Iterable

#: Cache keys for module/pip pairs we have already verified or
#: installed in this process. Keeps hot lazy-import paths from
#: re-running ``find_spec`` once a feature has been used.
_VERIFIED: set[str] = set()

PackageSpec = str | tuple[str, str]


def _resolve(spec: PackageSpec) -> tuple[str, str]:
    """Return ``(importable_module_name, pip_install_target)``."""
    if isinstance(spec, str):
        return spec, spec
    return spec


def ensure(packages: Iterable[PackageSpec], *, feature: str) -> None:
    """Make sure each package in *packages* is importable.

    Missing ones are pip-installed in a child process targeting
    ``sys.executable`` so the install lands in the same interpreter
    that's running AMX (avoids the system-pip-vs-venv-pip footgun).

    *feature* is a human-readable label for the message the user
    sees ("First-time setup for document RAG (/docs) — installing
    7 packages…").

    Raises ``RuntimeError`` if pip install fails. The exception
    message includes the exact command for the user to run by hand,
    so the error is recoverable without grepping the source.
    """
    missing_pip: list[str] = []
    seen_keys: list[str] = []
    for spec in packages:
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

    # Lazy console import — ``optional_deps`` itself sits at the top
    # of feature modules, and we want it to stay cheap to load.
    from amx.utils.console import info, success

    info(
        f"First-time setup for {feature} — installing "
        f"{len(missing_pip)} package{'s' if len(missing_pip) > 1 else ''}: "
        f"{', '.join(missing_pip)}"
    )
    info("This downloads from PyPI; pip output streams below so you see progress.")
    cmd = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        *missing_pip,
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
        manual = "pip install " + " ".join(missing_pip)
        raise RuntimeError(f"Could not invoke pip ({exc}). Install manually: {manual}") from exc

    if proc.returncode != 0:
        for key in seen_keys:
            _VERIFIED.discard(key)
        manual = "pip install " + " ".join(missing_pip)
        raise RuntimeError(
            f"pip install failed (exit code {proc.returncode}). Run manually: {manual}"
        )

    # Newly-installed packages weren't on sys.path at process start;
    # invalidate finder caches so the upcoming ``import`` picks them up.
    importlib.invalidate_caches()
    for key in seen_keys:
        _VERIFIED.add(key)
    success(f"Installed: {', '.join(missing_pip)}.")
