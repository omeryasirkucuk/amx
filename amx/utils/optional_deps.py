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
import re
import subprocess
import sys
import threading
import time
import uuid
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

        cmd = [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            *still_missing,
        ]
        try:
            returncode, captured = _run_pip_with_progress(cmd, feature, still_missing)
        except OSError as exc:
            manual = "pip install " + " ".join(still_missing)
            raise RuntimeError(f"Could not invoke pip ({exc}). Install manually: {manual}") from exc

        if returncode != 0:
            for key in seen_keys:
                _VERIFIED.discard(key)
            manual = "pip install " + " ".join(still_missing)
            tail = "\n".join(captured[-20:]) if captured else "(no pip output captured)"
            raise RuntimeError(
                f"pip install failed (exit code {returncode}). "
                f"Run manually: {manual}\n--- pip output (tail) ---\n{tail}"
            )

        # Newly-installed packages weren't on sys.path at process start;
        # invalidate finder caches so the upcoming ``import`` picks them up.
        importlib.invalidate_caches()
        for key in seen_keys:
            _VERIFIED.add(key)


# ── Progress-rendering pip wrapper ──────────────────────────────────────────

#: Regexes for the four pip milestones we surface as structured events.
#: Pip's wording is stable enough across versions to anchor on these
#: prefixes; anything we don't match still goes through as a generic
#: ``"tail"`` event so the live stream stays continuous.
_PIP_RE_COLLECTING = re.compile(r"^Collecting (\S+)")
_PIP_RE_DOWNLOADING = re.compile(r"^\s*Downloading (\S+)(?:\s+\(([^)]+)\))?")
_PIP_RE_INSTALLING = re.compile(r"^Installing collected packages:\s*(.+)")
_PIP_RE_SUCCESS = re.compile(r"^Successfully installed (.+)")


def _stdout_is_a_tty() -> bool:
    """Indirection seam so tests can force the headless code path
    without poking ``sys.stdout`` (which on most runtimes refuses
    arbitrary attribute assignment)."""
    try:
        return sys.stdout.isatty()
    except Exception:
        return False


def _run_pip_with_progress(
    cmd: list[str],
    feature: str,
    packages: list[str],
) -> tuple[int, list[str]]:
    """Run ``pip install`` while emitting structured progress.

    Pip's stdout/stderr are captured line-by-line (merged stream) so:

    1. The CLI shows a single Rich spinner + a one-line "tail" of the
       latest pip status instead of pages of raw output.
    2. Each parsed milestone is published to the process-global install
       bus so AMX Studio's banner can render the same progress in the
       browser.
    3. On failure the captured tail is returned to the caller and
       included in the ``RuntimeError`` so users still have something
       to grep.

    Returns ``(returncode, captured_lines)``. Does not raise on a
    non-zero exit — the caller decides how to surface it (matches the
    previous ``subprocess.run(..., check=False)`` contract).
    """
    # Lazy imports: the install bus pulls FastAPI deps transitively when
    # the routers import it, and we don't want to drag those into the
    # cold-start path of every feature module that imports ``ensure``.
    from amx.utils.console import console, success
    from amx.web import install_bus

    install_id = uuid.uuid4().hex[:12]
    captured: list[str] = []
    t_start = time.monotonic()

    install_bus.publish(
        "pip.install.begin",
        {"install_id": install_id, "feature": feature, "packages": list(packages)},
    )

    latest_tail = ""
    tail_lock = threading.Lock()

    def _on_line(line: str) -> None:
        nonlocal latest_tail
        captured.append(line)
        with tail_lock:
            latest_tail = line
        if m := _PIP_RE_COLLECTING.match(line):
            install_bus.publish(
                "pip.install.progress",
                {"install_id": install_id, "phase": "collecting", "package": m.group(1)},
            )
        elif m := _PIP_RE_DOWNLOADING.match(line):
            install_bus.publish(
                "pip.install.progress",
                {
                    "install_id": install_id,
                    "phase": "downloading",
                    "artifact": m.group(1),
                    "size": m.group(2),
                },
            )
        elif m := _PIP_RE_INSTALLING.match(line):
            install_bus.publish(
                "pip.install.progress",
                {"install_id": install_id, "phase": "installing", "packages": m.group(1)},
            )
        elif m := _PIP_RE_SUCCESS.match(line):
            install_bus.publish(
                "pip.install.progress",
                {"install_id": install_id, "phase": "installed", "installed": m.group(1)},
            )
        # Always emit a raw-tail event so live consumers see continuous
        # motion even between recognised milestones.
        install_bus.publish(
            "pip.install.progress",
            {"install_id": install_id, "phase": "tail", "line": line},
        )

    label_base = f"Installing libraries for {feature}"
    use_spinner = _stdout_is_a_tty()

    # If a Rich Live is already painting (orchestrator's display during
    # ``/run``), pause it for the duration of the install — nested Live
    # regions garble the terminal.
    paused_display = None
    try:
        from amx.utils.live_display import get_display

        display = get_display()
        if display is not None and getattr(display, "_live", None) is not None:
            display.pause()
            paused_display = display
    except Exception:
        paused_display = None

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        text=True,
    )
    assert proc.stdout is not None

    if use_spinner:
        from rich.console import Group
        from rich.live import Live
        from rich.spinner import Spinner
        from rich.text import Text

        stop_evt = threading.Event()

        def _renderable():
            elapsed = time.monotonic() - t_start
            with tail_lock:
                tail = latest_tail
            header = Spinner(
                "dots",
                text=Text(f"{label_base}…  {elapsed:.0f}s", style="cyan"),
            )
            if tail:
                truncated = tail if len(tail) <= 100 else tail[:97] + "…"
                return Group(header, Text(f"  └─ {truncated}", style="dim"))
            return header

        try:
            with Live(_renderable(), console=console, refresh_per_second=10, transient=True) as live:

                def _tick() -> None:
                    while not stop_evt.is_set():
                        try:
                            live.update(_renderable())
                        except Exception:
                            pass
                        stop_evt.wait(0.1)

                ticker = threading.Thread(target=_tick, daemon=True)
                ticker.start()
                try:
                    for raw in proc.stdout:
                        line = raw.rstrip("\r\n")
                        if not line:
                            continue
                        _on_line(line)
                    proc.wait()
                finally:
                    stop_evt.set()
                    ticker.join(timeout=0.5)
        finally:
            pass
    else:
        for raw in proc.stdout:
            line = raw.rstrip("\r\n")
            if not line:
                continue
            _on_line(line)
        proc.wait()

    elapsed_s = time.monotonic() - t_start

    if paused_display is not None:
        try:
            paused_display.resume()
        except Exception:
            pass

    if proc.returncode == 0:
        success(f"Installed libraries for {feature} ({elapsed_s:.1f}s)")
        install_bus.publish(
            "pip.install.done",
            {
                "install_id": install_id,
                "feature": feature,
                "packages": list(packages),
                "elapsed_s": elapsed_s,
            },
        )
    else:
        install_bus.publish(
            "pip.install.failed",
            {
                "install_id": install_id,
                "feature": feature,
                "packages": list(packages),
                "elapsed_s": elapsed_s,
                "returncode": proc.returncode,
                "tail": captured[-20:],
            },
        )

    return proc.returncode, captured
