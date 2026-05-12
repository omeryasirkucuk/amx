"""``/restore-config`` slash command.

Recovery tool for the rare case where a save() corrupts
``~/.amx/config.yml`` (or its ``AMX_CONFIG_DIR`` override). Lists the
rotated backups, asks the user to pick one, and restores it. The
pre-restore state itself becomes the new ``.bak.1`` so the operation
is always reversible.

Backed by the rotation helpers shipped alongside the same PR:
:func:`amx.config.list_config_backups` and
:func:`amx.config.restore_config_from_backup`.
"""

from __future__ import annotations

import datetime
from pathlib import Path

from amx.config import (
    AMXConfig,
    list_config_backups,
    restore_config_from_backup,
)
from amx.utils.console import ask_choice, console, error, info, success, warn


def cmd_restore_config(cfg: AMXConfig, rest: list[str]) -> None:
    """Show available config backups and restore the user's pick.

    Usage forms:
      ``/restore-config``                 → interactive picker
      ``/restore-config --list``          → just list, no restore
      ``/restore-config --from <path>``   → restore an explicit backup file
                                            (also accepts ``.bak.N``)
    """
    config_path = Path(cfg._config_path) if getattr(cfg, "_config_path", "") else None

    if rest and rest[0] in ("--list", "-l"):
        _print_backup_table(config_path)
        return

    if len(rest) >= 2 and rest[0] in ("--from", "-f"):
        target = Path(rest[1]).expanduser()
        if not target.is_file():
            error(f"Backup file not found: {target}")
            return
        _perform_restore(target, config_path)
        return

    backups = list_config_backups(config_path)
    if not backups:
        warn(
            "No rotated config backups found. "
            "Backups are created on every save() — start a fresh AMX "
            "session and they'll begin accumulating."
        )
        return

    _print_backup_table(config_path)
    console.print()
    choices = [_format_backup_choice(b) for b in backups]
    picked = ask_choice("Restore which backup?", choices)
    idx = choices.index(picked)
    _perform_restore(backups[idx], config_path)


def _print_backup_table(config_path: Path | None) -> None:
    backups = list_config_backups(config_path)
    if not backups:
        info(
            "No backups yet. The first save() after this PR will create "
            "config.yml.bak.1; up to five generations are kept."
        )
        return
    info(f"Available backups ({len(backups)} of max 5):")
    for backup in backups:
        ts = _format_backup_choice(backup)
        console.print(f"  {ts}")


def _format_backup_choice(backup: Path) -> str:
    try:
        stat = backup.stat()
        mtime = datetime.datetime.fromtimestamp(stat.st_mtime)
        size_kb = stat.st_size / 1024
        return f"{backup.name} ({mtime:%Y-%m-%d %H:%M:%S}, {size_kb:.1f} KB)"
    except OSError:
        return backup.name


def _perform_restore(backup: Path, config_path: Path | None) -> None:
    try:
        restored = restore_config_from_backup(backup, config_path)
    except FileNotFoundError as exc:
        error(str(exc))
        return
    except Exception as exc:
        error(f"Restore failed: {exc}")
        return
    success(
        f"Restored {backup.name} -> {restored.name}. "
        "The pre-restore state was rotated to .bak.1 so the restore is "
        "reversible. Restart the CLI for the new config to take effect."
    )
