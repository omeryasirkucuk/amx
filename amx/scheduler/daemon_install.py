"""Scheduler daemon install / uninstall / detect (launchd + systemd).

This is the platform shim used by ``amx scheduler install-daemon`` and
friends. macOS uses a per-user launchd plist under
``~/Library/LaunchAgents/``; Linux uses a per-user systemd
``.service`` + ``.timer`` pair under ``~/.config/systemd/user/``;
other platforms (Windows etc) print a clear "not supported" message.

The unit name is suffixed with the ``AMX_CONFIG_DIR`` so prod (``~/.amx``)
and dev (``~/.amx-dev``) daemons coexist.
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
from pathlib import Path
from typing import Any

_PLIST_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{python_path}</string>
        <string>-m</string>
        <string>amx.scheduler</string>
    </array>
    <key>StartInterval</key><integer>60</integer>
    <key>RunAtLoad</key><true/>
    <key>EnvironmentVariables</key>
    <dict>
        <key>AMX_CONFIG_DIR</key><string>{config_dir}</string>
        <key>AMX_SKIP_BOOTSTRAP_TICK</key><string>1</string>
        <key>PYTHONUNBUFFERED</key><string>1</string>
        <key>PATH</key><string>/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin</string>
    </dict>
    <key>StandardOutPath</key><string>{log_path}</string>
    <key>StandardErrorPath</key><string>{log_path}</string>
</dict>
</plist>
"""

_SYSTEMD_SERVICE = """[Unit]
Description=AMX scheduler tick ({label})

[Service]
Type=oneshot
Environment=AMX_CONFIG_DIR={config_dir}
Environment=AMX_SKIP_BOOTSTRAP_TICK=1
Environment=PYTHONUNBUFFERED=1
ExecStart={python_path} -m amx.scheduler
"""

_SYSTEMD_TIMER = """[Unit]
Description=Run AMX scheduler tick every minute ({label})

[Timer]
OnBootSec=60
OnUnitActiveSec=60
Unit=amx-scheduler{suffix}.service

[Install]
WantedBy=timers.target
"""


def _config_dir() -> Path:
    override = os.environ.get("AMX_CONFIG_DIR")
    return Path(override).expanduser() if override else Path.home() / ".amx"


def _label_and_suffix() -> tuple[str, str]:
    """Compute the daemon label + filename suffix from AMX_CONFIG_DIR.

    Prod (``~/.amx``) -> label ``com.amx.scheduler``, suffix ``""``.
    Dev  (``~/.amx-dev``) -> label ``com.amx-dev.scheduler``, suffix ``"-dev"``.
    Custom paths derive a slug from the directory's basename.
    """
    cfg = _config_dir()
    base = cfg.name  # ".amx" or ".amx-dev" usually
    slug = base.lstrip(".") or "amx"
    if slug == "amx":
        return "com.amx.scheduler", ""
    return f"com.{slug}.scheduler", f"-{slug[len('amx-') :]}" if slug.startswith(
        "amx-"
    ) else f"-{slug}"


def _amx_path() -> str:
    found = shutil.which("amx")
    if found:
        return found
    return "amx"  # fall through; user can edit unit file if PATH is unusual


def _python_path() -> str:
    """Resolve the Python interpreter the daemon should use.

    The daemon invokes ``python -m amx.scheduler`` rather than the
    ``amx`` shell entry because AMX's CLI is REPL-only on this
    install -- top-level ``amx <subcommand>`` calls are blocked with
    "Direct subcommands are disabled". The Python module path
    bypasses that gate cleanly without breaking the user's
    interactive UX rule.

    We use ``sys.executable`` so the daemon honours whichever venv
    the install command was invoked from. That matters for the
    prod / dev split: prod and dev typically live in separate venvs
    and the daemon must use the one that actually has AMX installed.
    """
    import sys

    return sys.executable


def _macos_plist_path(label: str) -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"


def _systemd_service_path(suffix: str) -> Path:
    return Path.home() / ".config" / "systemd" / "user" / f"amx-scheduler{suffix}.service"


def _systemd_timer_path(suffix: str) -> Path:
    return Path.home() / ".config" / "systemd" / "user" / f"amx-scheduler{suffix}.timer"


def detect_daemon_state() -> dict[str, Any]:
    """Return ``{installed, loaded, path, last_tick_log, log_size_bytes,
    log_mtime}``.

    ``installed``: the plist / service file exists on disk.
    ``loaded``: the daemon is currently registered with the OS
    scheduler. Pre-PR the function only checked the file; the user's
    scheduler.log staying at 0 bytes after a successful "install"
    click was exactly because the plist was on disk but launchd
    never bootstrapped it. Now we distinguish the two so the SPA
    can render a clear "Installed but not loaded — click to fix"
    state instead of a misleading "Installed ✓" badge.

    Pure introspection — never starts or stops anything.
    """
    label, suffix = _label_and_suffix()
    cfg = _config_dir()
    log_path = cfg / "logs" / "scheduler.log"
    log_str = str(log_path) if log_path.exists() else None
    log_size = log_path.stat().st_size if log_path.exists() else 0
    log_mtime = log_path.stat().st_mtime if log_path.exists() else None
    system = platform.system()

    def _result(
        installed: bool, loaded: bool, path: str | None
    ) -> dict[str, Any]:
        return {
            "installed": installed,
            "loaded": loaded,
            "path": path,
            "last_tick_log": log_str,
            "log_size_bytes": log_size,
            "log_mtime": log_mtime,
        }

    if system == "Darwin":
        plist = _macos_plist_path(label)
        installed = plist.exists()
        loaded = False
        if installed:
            target = f"gui/{os.getuid()}/{label}"
            try:
                # ``launchctl print`` exits 0 when the service is
                # registered AND its plist is bootstrapped, non-zero
                # otherwise. ``capture_output=True`` swallows the
                # multi-line dump which we don't need.
                result = subprocess.run(
                    ["launchctl", "print", target],
                    check=False,
                    capture_output=True,
                )
                loaded = result.returncode == 0
            except FileNotFoundError:
                loaded = False
        return _result(installed, loaded, str(plist) if installed else None)
    if system == "Linux":
        service = _systemd_service_path(suffix)
        timer = _systemd_timer_path(suffix)
        installed = service.exists() and timer.exists()
        loaded = False
        if installed:
            try:
                # ``systemctl --user is-active <timer>`` returns 0 when
                # the timer is currently armed.
                result = subprocess.run(
                    ["systemctl", "--user", "is-active", timer.name],
                    check=False,
                    capture_output=True,
                )
                loaded = result.returncode == 0
            except FileNotFoundError:
                loaded = False
        return _result(installed, loaded, str(timer) if installed else None)
    if system == "Windows":
        try:
            result = subprocess.run(
                ["schtasks", "/query", "/tn", label],
                check=False,
                capture_output=True,
            )
            if result.returncode == 0:
                return _result(True, True, label)
        except FileNotFoundError:
            pass
        return _result(False, False, None)
    return _result(False, False, None)


def install_daemon() -> dict[str, Any]:
    """Install + activate the platform daemon."""
    label, suffix = _label_and_suffix()
    cfg = _config_dir()
    cfg.mkdir(parents=True, exist_ok=True)
    (cfg / "logs").mkdir(parents=True, exist_ok=True)
    log_path = cfg / "logs" / "scheduler.log"
    python_path = _python_path()
    system = platform.system()

    if system == "Darwin":
        plist_path = _macos_plist_path(label)
        plist_path.parent.mkdir(parents=True, exist_ok=True)
        plist_path.write_text(
            _PLIST_TEMPLATE.format(
                label=label,
                python_path=python_path,
                config_dir=str(cfg),
                log_path=str(log_path),
            )
        )
        # macOS Big Sur deprecated ``launchctl load`` — on modern
        # systems it returns 0 and silently does nothing, so the
        # plist sits on disk but the daemon never runs. The user's
        # scheduler.log staying at 0 bytes after a successful
        # "Install daemon" click is exactly that. Switch to the
        # modern ``bootstrap`` / ``enable`` / ``kickstart`` triple.
        target = f"gui/{os.getuid()}/{label}"
        domain = f"gui/{os.getuid()}"
        errors: list[str] = []

        def _run(cmd: list[str], *, ignore_fail: bool = False) -> int:
            try:
                result = subprocess.run(cmd, check=False, capture_output=True, text=True)
            except FileNotFoundError:
                return -1
            if result.returncode != 0 and not ignore_fail:
                stderr = (result.stderr or "").strip()
                if stderr:
                    errors.append(f"{' '.join(cmd)}: {stderr}")
            return result.returncode

        # 1. Unload any previous incarnation. ``bootout`` is the
        #    modern replacement; ``unload`` is the legacy fallback.
        #    Both no-op cleanly when the label isn't loaded.
        _run(["launchctl", "bootout", target], ignore_fail=True)
        _run(["launchctl", "unload", str(plist_path)], ignore_fail=True)
        # 2. Load + activate via the modern API. Capture stderr so
        #    a real failure (e.g. unsigned binary, missing python
        #    path) surfaces to the caller instead of being swallowed.
        rc_bootstrap = _run(["launchctl", "bootstrap", domain, str(plist_path)])
        if rc_bootstrap != 0:
            # Legacy fallback — older macOS (Catalina and earlier)
            # only understands ``load``. ``bootstrap`` returns
            # non-zero there; fall through silently and try the
            # old command, which is correct on those systems.
            _run(["launchctl", "load", str(plist_path)])
        # 3. Make sure the service isn't disabled by a prior
        #    ``launchctl disable`` (some user-config managers leave
        #    these stuck).
        _run(["launchctl", "enable", target], ignore_fail=True)
        # 4. Fire the first run immediately so the user doesn't wait
        #    up to 60s for the next interval tick after install.
        _run(["launchctl", "kickstart", "-k", target], ignore_fail=True)

        message = f"Installed launchd daemon {label}."
        if errors:
            message += " (warnings: " + "; ".join(errors) + ")"
        return {
            "message": message,
            "path": str(plist_path),
        }

    if system == "Linux":
        svc_path = _systemd_service_path(suffix)
        tmr_path = _systemd_timer_path(suffix)
        svc_path.parent.mkdir(parents=True, exist_ok=True)
        svc_path.write_text(
            _SYSTEMD_SERVICE.format(label=label, python_path=python_path, config_dir=str(cfg))
        )
        tmr_path.write_text(_SYSTEMD_TIMER.format(label=label, suffix=suffix))
        # ``loginctl enable-linger`` keeps user-level systemd units
        # running on headless / sshd-only servers where the user
        # doesn't have an interactive session pinned. Best-effort:
        # the call may fail without sudo on some distros, which is
        # fine -- the timer still works for users who stay logged
        # in interactively.
        username = os.environ.get("USER", "")
        for cmd in (
            ["systemctl", "--user", "daemon-reload"],
            ["systemctl", "--user", "enable", "--now", tmr_path.name],
            ["loginctl", "enable-linger", username] if username else None,
        ):
            if cmd is None:
                continue
            try:
                subprocess.run(cmd, check=False, capture_output=True)
            except FileNotFoundError:
                break
        return {
            "message": f"Installed systemd user timer {tmr_path.name}.",
            "path": str(tmr_path),
        }

    if system == "Windows":
        # Windows Task Scheduler equivalent: ``schtasks /create`` with
        # a minute-recurring trigger. ``/sc minute /mo 1`` runs every
        # 1 minute. The task runs as the current user (``/ru``)
        # because the LLM-credential lookup pulls from that user's
        # config dir; ``/rl LIMITED`` keeps it from auto-elevating.
        # /f forces overwrite of any older task with the same name.
        task_name = label  # e.g. "com.amx.scheduler"
        # Wrap the python invocation so the task captures stdout to
        # the same scheduler.log location as the other OSes.
        log_path_w = log_path
        username = os.environ.get("USERNAME", "")
        # cmd.exe redirection: pipe stdout+stderr to the log file,
        # append mode so consecutive ticks accumulate.
        task_action = (
            f'cmd.exe /c "set AMX_CONFIG_DIR={cfg} && '
            f"set AMX_SKIP_BOOTSTRAP_TICK=1 && "
            f"set PYTHONUNBUFFERED=1 && "
            f'"{python_path}" -m amx.scheduler >> "{log_path_w}" 2>&1"'
        )
        cmd = [
            "schtasks",
            "/create",
            "/sc",
            "minute",
            "/mo",
            "1",
            "/tn",
            task_name,
            "/tr",
            task_action,
            "/rl",
            "LIMITED",
            "/f",
        ]
        if username:
            cmd.extend(["/ru", username])
        try:
            result = subprocess.run(cmd, check=False, capture_output=True)
            ok = result.returncode == 0
        except FileNotFoundError:
            return {
                "message": (
                    "schtasks.exe not found. Daemon install requires "
                    "Windows Task Scheduler (built into Windows 10/11)."
                ),
                "path": None,
            }
        return {
            "message": (
                f"Installed Windows scheduled task '{task_name}'."
                if ok
                else f"schtasks /create failed: {result.stderr.decode(errors='replace').strip()}"
            ),
            "path": task_name if ok else None,
        }

    return {
        "message": (
            f"Daemon mode is not supported on {system}. Schedules will "
            "still fire whenever AMX or Studio is running (catch-up). "
            "Studio's in-process scheduler also fires due schedules "
            "on a 60s cadence whenever the Studio process is alive."
        ),
        "path": None,
    }


def uninstall_daemon() -> dict[str, Any]:
    """Remove the platform daemon. Idempotent."""
    label, suffix = _label_and_suffix()
    system = platform.system()

    if system == "Darwin":
        plist_path = _macos_plist_path(label)
        if plist_path.exists():
            try:
                subprocess.run(
                    ["launchctl", "unload", str(plist_path)],
                    check=False,
                    capture_output=True,
                )
            except FileNotFoundError:
                pass
            plist_path.unlink()
            return {"message": f"Uninstalled launchd daemon {label}."}
        return {"message": f"No launchd daemon for {label} was installed."}

    if system == "Linux":
        svc = _systemd_service_path(suffix)
        tmr = _systemd_timer_path(suffix)
        if tmr.exists():
            try:
                subprocess.run(
                    ["systemctl", "--user", "disable", "--now", tmr.name],
                    check=False,
                    capture_output=True,
                )
            except FileNotFoundError:
                pass
        for p in (tmr, svc):
            if p.exists():
                p.unlink()
        return {"message": f"Uninstalled systemd timer/service {tmr.name}."}

    if system == "Windows":
        try:
            result = subprocess.run(
                ["schtasks", "/delete", "/tn", label, "/f"],
                check=False,
                capture_output=True,
            )
            ok = result.returncode == 0
        except FileNotFoundError:
            return {"message": "schtasks.exe not found."}
        return {
            "message": (
                f"Uninstalled Windows scheduled task '{label}'."
                if ok
                else f"No Windows scheduled task '{label}' was installed."
            )
        }

    return {"message": "Daemon mode is not supported on this platform."}
