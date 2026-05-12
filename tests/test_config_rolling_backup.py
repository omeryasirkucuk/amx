"""Every :meth:`AMXConfig.save` rotates the previous on-disk YAML
into a backup slot (``config.yml.bak.1`` … ``.bak.N``) so that if a
future bug ever again silently truncates the live file, the user
has up to five generations of pre-incident data on disk for
recovery via ``/restore-config``.

These tests pin the rotation contract end-to-end and verify the
restore helper actually puts the backup contents back as the live
file (with the pre-restore state preserved as the new ``.bak.1``).
"""

from __future__ import annotations


def test_first_save_creates_no_backup_if_no_prior_file(tmp_path):
    """A fresh install: the very first save() has nothing to back up
    because there's no existing file on disk."""
    from amx.config import AMXConfig

    cfg_path = tmp_path / "config.yml"
    cfg = AMXConfig()
    cfg.save(str(cfg_path))
    assert cfg_path.exists()
    assert not (tmp_path / "config.yml.bak.1").exists()


def test_save_rotates_prior_file_into_bak1(tmp_path):
    """Second save: the previous on-disk content becomes .bak.1."""
    from amx.config import AMXConfig

    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "active_db_profile: pre-save\n"
        "db_profiles:\n"
        "  pre-save:\n"
        "    backend: postgresql\n"
    )

    cfg = AMXConfig.load(str(cfg_path))
    cfg.save(str(cfg_path))

    bak1 = tmp_path / "config.yml.bak.1"
    assert bak1.exists()
    assert "pre-save" in bak1.read_text()


def test_save_keeps_at_most_5_generations(tmp_path):
    """A sixth save rolls .bak.5 off the end."""
    from amx.config import AMXConfig

    cfg_path = tmp_path / "config.yml"
    # Seed with a base file so the first save() will rotate it.
    cfg_path.write_text("generation: 0\n")

    for i in range(1, 7):
        # Each save writes new content and rotates the prior file.
        cfg = AMXConfig.load(str(cfg_path))
        cfg_path.write_text(f"generation: {i}\n")
        cfg.save(str(cfg_path))

    # We should have .bak.1 through .bak.5; .bak.6 must not exist.
    for i in range(1, 6):
        assert (tmp_path / f"config.yml.bak.{i}").exists(), f"missing bak.{i}"
    assert not (tmp_path / "config.yml.bak.6").exists()


def test_list_config_backups_returns_newest_first(tmp_path):
    """``.bak.1`` is the newest backup; list_config_backups returns
    backups in numeric order so callers can show them most-recent-first."""
    from amx.config import list_config_backups

    cfg_path = tmp_path / "config.yml"
    for i in range(1, 4):
        (tmp_path / f"config.yml.bak.{i}").write_text(f"gen {i}\n")

    backups = list_config_backups(cfg_path)
    names = [b.name for b in backups]
    assert names == [
        "config.yml.bak.1",
        "config.yml.bak.2",
        "config.yml.bak.3",
    ]


def test_restore_from_backup_swaps_in_place(tmp_path):
    """Restore replaces the live config with the backup contents AND
    rotates the pre-restore state into the new .bak.1 so the user
    can undo the restore itself."""
    from amx.config import restore_config_from_backup

    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text("active_db_profile: live-broken\ndb_profiles: {}\n")
    backup = tmp_path / "config.yml.bak.2"
    backup.write_text("active_db_profile: known-good\ndb_profiles: {}\n")

    restore_config_from_backup(backup, cfg_path)

    live = cfg_path.read_text()
    assert "known-good" in live, "restore didn't take effect"

    new_bak1 = tmp_path / "config.yml.bak.1"
    assert new_bak1.exists(), "pre-restore state not rotated to bak.1"
    assert "live-broken" in new_bak1.read_text()


def test_restore_raises_for_missing_backup(tmp_path):
    """Restoring from a non-existent backup raises FileNotFoundError —
    the CLI handler surfaces this as an error() instead of crashing."""
    from amx.config import restore_config_from_backup

    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text("live\n")
    missing = tmp_path / "does-not-exist.bak.42"

    try:
        restore_config_from_backup(missing, cfg_path)
    except FileNotFoundError:
        return
    raise AssertionError("expected FileNotFoundError")


def test_rotation_is_best_effort_and_does_not_block_save(tmp_path, monkeypatch):
    """If the rotation step somehow fails (e.g. permission error),
    the save() must still succeed. The user's data on disk is more
    important than a perfect backup chain."""
    from amx import config as cfg_mod

    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text("active_db_profile: ''\ndb_profiles: {}\n")

    cfg = cfg_mod.AMXConfig.load(str(cfg_path))

    # Force rotation to raise — save() should still complete because
    # the helper wraps every step in suppress().
    def boom(*_a, **_kw):
        raise OSError("simulated rotation failure")

    monkeypatch.setattr(cfg_mod, "_rotate_config_backups", boom)
    try:
        cfg.save(str(cfg_path))
    except OSError:
        raise AssertionError(
            "rotation failure leaked through save() — would block user's "
            "ability to persist config when backups are misbehaving"
        )
    # Live file is still written; that's the contract.
    assert cfg_path.exists()
