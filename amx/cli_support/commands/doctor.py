"""`amx doctor` — diagnose installation, config, and connectivity issues.

The single most valuable diagnostic for the version-skew bug class
that hit on 2026-05-01: two ``amx`` binaries on PATH writing the same
``~/.amx/config.yml`` made profiles silently disappear. Doctor lists
every ``amx`` it can find on PATH so the user can spot a stale one
before debugging anything else.

Beyond PATH conflicts the command also checks: config file readability
+ schema version vs the running binary, optional dependency imports
(BigQuery / Snowflake / Databricks / docs deps), active DB profile
connectivity, active LLM profile reachability, and ``~/.amx``
directory permissions. Each check renders ✓/✗ with an actionable hint
on failure.
"""

from __future__ import annotations

import os
import shutil
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click

from amx import __version__
from amx.config import CONFIG_SCHEMA_VERSION, AMXConfig
from amx.utils.console import console

LogEvent = Callable[..., None]


# ── Result model ────────────────────────────────────────────────────────────


@dataclass
class CheckResult:
    """One diagnostic line. ``ok`` False means user attention required."""

    name: str
    ok: bool
    detail: str = ""
    hint: str = ""


# ── Individual checks ───────────────────────────────────────────────────────


def _check_amx_on_path() -> CheckResult:
    """Detect multiple ``amx`` binaries on PATH (the 0.3.1 ghost-profile bug).

    Cross-platform: on POSIX the file is named ``amx``; on Windows it's
    ``amx.exe`` (and pip also drops ``amx.cmd`` / ``amx-script.py``).
    The earlier loop only looked for the bare ``amx`` filename and
    therefore reported "(not found)" on a perfectly healthy Windows
    install. Use ``shutil.which`` for the canonical resolution and a
    cross-platform PATH walk to surface duplicates.
    """
    primary = shutil.which("amx")
    # Windows lookups via PATHEXT also resolve ``amx.exe``/``amx.cmd``;
    # ``shutil.which("amx")`` already does that for us.
    path_env = os.environ.get("PATH", "")
    candidates: list[str] = []
    is_windows = os.name == "nt"
    suffixes = (".exe", ".cmd", ".bat", "") if is_windows else ("",)
    for directory in path_env.split(os.pathsep):
        if not directory:
            continue
        for suffix in suffixes:
            candidate = Path(directory) / f"amx{suffix}"
            if candidate.is_file():
                # On POSIX additionally require the executable bit; on
                # Windows file extension already gates executability.
                if is_windows or os.access(candidate, os.X_OK):
                    candidates.append(str(candidate))
                    break
    # Dedup while preserving order — the first entry is what `which amx` returns.
    deduped: list[str] = []
    seen_set: set[str] = set()
    for entry in candidates:
        # Resolve to absolute physical path to detect symlink shadowing.
        try:
            real = str(Path(entry).resolve())
        except OSError:
            real = entry
        if real not in seen_set:
            seen_set.add(real)
            deduped.append(entry)
    if not deduped and primary:
        # ``shutil.which`` succeeded but our manual scan missed it
        # (e.g. PATH entry contains an env var the loop didn't expand).
        # Trust ``shutil.which`` over our walk so the user doesn't see
        # a false "(not found)" while the binary they just ran works.
        deduped = [primary]
    if len(deduped) <= 1:
        location = deduped[0] if deduped else "(not found)"
        return CheckResult(
            name="amx on PATH",
            ok=bool(deduped),
            detail=location,
            hint=""
            if deduped
            else "Reinstall AMX (`pip install amx-cli` or `pipx install amx-cli`).",
        )
    return CheckResult(
        name="amx on PATH",
        ok=False,
        detail=" + ".join(deduped),
        hint=(
            "Multiple `amx` installs detected. Each writes to the same "
            "~/.amx/config.yml; an older binary can silently strip keys it "
            "doesn't recognise (the ghost-profile bug). Uninstall all but "
            f"one — recommended: keep `{deduped[0]}` and remove the rest."
        ),
    )


def _check_python_version() -> CheckResult:
    py = sys.version_info
    ok = py >= (3, 10)
    return CheckResult(
        name="Python runtime",
        ok=ok,
        detail=f"{py.major}.{py.minor}.{py.micro} ({sys.executable})",
        hint="" if ok else "AMX requires Python 3.10 or newer.",
    )


def _check_amx_version() -> CheckResult:
    return CheckResult(
        name="AMX version",
        ok=True,
        detail=f"{__version__} (config schema v{CONFIG_SCHEMA_VERSION})",
    )


def _check_config_dir(cfg: AMXConfig) -> CheckResult:
    config_dir = Path(cfg.CONFIG_DIR)
    if not config_dir.exists():
        return CheckResult(
            name="Config directory",
            ok=True,
            detail=f"{config_dir} (will be created on first save)",
        )
    if not os.access(config_dir, os.W_OK):
        return CheckResult(
            name="Config directory",
            ok=False,
            detail=str(config_dir),
            hint=(
                f"AMX cannot write to {config_dir}. Check permissions: "
                f"`chmod u+rwx {config_dir}` or move it via $AMX_CONFIG_DIR."
            ),
        )
    return CheckResult(name="Config directory", ok=True, detail=str(config_dir))


def _check_config_file(cfg: AMXConfig) -> CheckResult:
    """Doctor only runs after ``AMXConfig.load`` already passed the schema gate.

    So we just report what was loaded and where. The schema-too-new
    case is caught earlier in ``cli.py`` before doctor is reachable.
    """
    if not cfg.config_path:
        return CheckResult(
            name="Config file",
            ok=True,
            detail="(none — fresh install, will be created on first save)",
        )
    p = Path(cfg.config_path)
    if not p.exists():
        return CheckResult(
            name="Config file",
            ok=True,
            detail=f"{p} (none yet)",
        )
    return CheckResult(
        name="Config file",
        ok=True,
        detail=f"{p} (schema v{CONFIG_SCHEMA_VERSION})",
    )


def _check_optional_deps(active_backend: str | None = None) -> list[CheckResult]:
    """Probe optional backend deps so users know what's available before they need it.

    When *active_backend* matches one of the database backends the check
    is promoted from "optional info" to "required" — the active profile
    cannot work without its driver, so reporting ``✓ not installed
    (optional)`` for the very thing the user is trying to use is
    actively misleading.
    """
    # (label, backend key, module to import, install hint)
    backends: tuple[tuple[str, str | None, str, str], ...] = (
        (
            "BigQuery driver",
            "bigquery",
            "google.cloud.bigquery",
            "pip install 'amx-cli[bigquery]'",
        ),
        (
            "Snowflake driver",
            "snowflake",
            "snowflake.connector",
            "pip install 'amx-cli[snowflake]'",
        ),
        (
            "Databricks driver",
            "databricks",
            "databricks.sql",
            "pip install 'amx-cli[databricks]'",
        ),
        (
            "OS keyring",
            None,
            "keyring",
            "pip install keyring (secrets fall back to plaintext YAML without it)",
        ),
    )
    results: list[CheckResult] = []
    for label, backend_key, mod, hint in backends:
        is_required = backend_key is not None and backend_key == active_backend
        try:
            __import__(mod)
            detail = "installed (required for active profile)" if is_required else "installed"
            results.append(CheckResult(name=label, ok=True, detail=detail))
        except ImportError:
            if is_required:
                results.append(
                    CheckResult(
                        name=label,
                        ok=False,
                        detail=f"not installed — required for active '{active_backend}' profile",
                        hint=hint,
                    )
                )
            else:
                results.append(
                    CheckResult(
                        name=label,
                        ok=True,  # Optional — surface as info, not error.
                        detail="not installed (optional)",
                        hint=hint,
                    )
                )
    return results


def _check_active_db_profile(cfg: AMXConfig) -> CheckResult:
    """Test the active DB connection if one is configured."""
    if not cfg.active_db_profile or not cfg.db_profiles:
        return CheckResult(
            name="Active DB profile",
            ok=True,
            detail="(none configured)",
            hint="Run /add-db-profile inside `amx` to add one.",
        )
    if not cfg.db.is_connection_configured():
        return CheckResult(
            name="Active DB profile",
            ok=False,
            detail=f"profile '{cfg.active_db_profile}' (incomplete)",
            hint="Run /add-db-profile to fill in connection details.",
        )
    # Lazy import — DatabaseConnector pulls in heavy backend deps.
    from amx.db.connector import DatabaseConnector

    try:
        db = DatabaseConnector(cfg.db)
        ok = bool(db.test_connection())
    except Exception as exc:
        return CheckResult(
            name="Active DB profile",
            ok=False,
            detail=f"profile '{cfg.active_db_profile}' — {exc.__class__.__name__}: {exc}",
            hint="Check credentials / network / firewall and retry.",
        )
    label = f"{cfg.db.backend} · {cfg.db.display_summary}"
    return CheckResult(
        name="Active DB profile",
        ok=ok,
        detail=f"profile '{cfg.active_db_profile}' → {label}",
        hint="" if ok else "Connection test returned False — check credentials.",
    )


def _check_active_llm_profile(cfg: AMXConfig) -> CheckResult:
    """Probe the active LLM endpoint if one is configured."""
    if not cfg.active_llm_profile or not cfg.llm_profiles or not cfg.llm.is_configured():
        return CheckResult(
            name="Active LLM profile",
            ok=True,
            detail="(none configured)",
            hint="Run /add-llm-profile inside `amx` to add one.",
        )
    from amx.llm.provider import LLMProvider

    try:
        provider = LLMProvider(cfg.llm)
        result = provider.test_result()
    except Exception as exc:
        return CheckResult(
            name="Active LLM profile",
            ok=False,
            detail=f"profile '{cfg.active_llm_profile}' — {exc.__class__.__name__}: {exc}",
            hint="Check API key / model name / base URL.",
        )
    label = f"{cfg.llm.provider}/{cfg.llm.model}"
    if result.ok:
        return CheckResult(
            name="Active LLM profile",
            ok=True,
            detail=f"profile '{cfg.active_llm_profile}' → {label}",
        )
    return CheckResult(
        name="Active LLM profile",
        ok=False,
        detail=f"profile '{cfg.active_llm_profile}' → {label}",
        hint=result.message or "LLM test returned not OK — check credentials.",
    )


# ── Rendering ───────────────────────────────────────────────────────────────


def _render_results(results: list[CheckResult]) -> int:
    """Render the report and return an exit code (0 = clean)."""
    fail_count = sum(1 for r in results if not r.ok)
    console.print()
    console.print("[heading]AMX doctor report[/heading]")
    console.print()
    for r in results:
        marker = "[bold green]✓[/]" if r.ok else "[bold red]✗[/]"
        line = f"{marker} [bold]{r.name}[/bold]"
        if r.detail:
            line += f" — {r.detail}"
        console.print(line)
        if r.hint and (not r.ok or "not installed" in r.detail):
            console.print(f"   [dim]{r.hint}[/dim]")
    console.print()
    if fail_count == 0:
        console.print("[bold green]All checks passed.[/bold green]")
    else:
        console.print(
            f"[bold red]{fail_count} check(s) failed.[/bold red] "
            "Address the items marked ✗ above and re-run `amx doctor`."
        )
    console.print()
    return 0 if fail_count == 0 else 1


# ── Command implementation ──────────────────────────────────────────────────


def collect_doctor_checks(
    cfg: AMXConfig,
    *,
    skip_network: bool = False,
) -> list[CheckResult]:
    """Build the diagnostic check list without rendering it.

    Shared between the CLI's ``run_doctor`` (which renders the list as
    a Rich report) and AMX Studio's ``GET /api/doctor`` endpoint
    (which JSON-encodes it). Splitting render from collect keeps the
    two surfaces in lock-step without each pulling in the other's
    Rich dependency.
    """
    results: list[CheckResult] = []
    results.append(_check_amx_version())
    results.append(_check_python_version())
    results.append(_check_amx_on_path())
    results.append(_check_config_dir(cfg))
    results.append(_check_config_file(cfg))
    active_backend: str | None = None
    if cfg.active_db_profile and cfg.db_profiles:
        active_backend = getattr(cfg.db, "backend", None) or None
    results.extend(_check_optional_deps(active_backend))
    if not skip_network:
        results.append(_check_active_db_profile(cfg))
        results.append(_check_active_llm_profile(cfg))
    return results


def run_doctor(cfg: AMXConfig, *, skip_network: bool = False) -> int:
    """Run every diagnostic and render the report. Returns the exit code."""
    return _render_results(collect_doctor_checks(cfg, skip_network=skip_network))


def register_doctor_command(
    main: click.Group,
    *,
    pass_config: Callable[[Callable[..., Any]], Callable[..., Any]],
    log_event: LogEvent,
) -> None:
    """Attach `amx doctor` (and `/doctor` inside the session) to the main group."""

    @main.command("doctor")
    @click.option(
        "--skip-network",
        is_flag=True,
        default=False,
        help="Skip DB and LLM connectivity tests (offline / quick mode).",
    )
    @pass_config
    def doctor(cfg: AMXConfig, skip_network: bool) -> None:
        """Diagnose installation, config, and connectivity issues."""
        exit_code = run_doctor(cfg, skip_network=skip_network)
        log_event(
            event_type="doctor",
            status="success" if exit_code == 0 else "failed",
            command="doctor",
            details={"skip_network": skip_network, "exit_code": exit_code},
        )
        if exit_code != 0:
            # Leave a non-zero exit so scripts can chain on it, but don't
            # raise a stack trace inside the interactive session.
            sys.exit(exit_code)


# Re-export for tests / direct invocation.
__all__ = [
    "CheckResult",
    "collect_doctor_checks",
    "register_doctor_command",
    "run_doctor",
]


# Backwards-compatible alias kept for symmetry with `shutil.which` usage
# in user docs/recipes — no production code depends on it.
def _which_amx() -> str | None:
    return shutil.which("amx")
