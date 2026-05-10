"""Tests for rotating file handler + ``log_event`` structured emitter.

The handler change is invisible to log consumers (still ``amx.log``)
but caps disk usage. The ``log_event`` helper is a new additive API
that subsequent PRs (orchestrator cancellation, run lifecycle) will
emit through.
"""

from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest

from amx.utils import logging as amx_logging
from amx.utils.logging import LOG_BACKUP_COUNT, LOG_MAX_BYTES, get_logger, log_event


def _clear_amx_loggers() -> None:
    """Drop cached handlers so ``get_logger`` rebuilds against the
    test's patched ``LOG_DIR``. ``get_logger`` short-circuits when
    ``logger.handlers`` is non-empty, which would otherwise reuse the
    pre-patch file handler from a prior test."""
    for name in list(logging.Logger.manager.loggerDict):
        if name.startswith("amx."):
            logger = logging.getLogger(name)
            for handler in list(logger.handlers):
                logger.removeHandler(handler)
                try:
                    handler.close()
                except Exception:  # pragma: no cover - cleanup best-effort
                    pass


@pytest.fixture
def isolated_log_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect ``LOG_DIR`` to a per-test temp dir and reset cached
    handlers on entry and exit."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(amx_logging, "LOG_DIR", log_dir)
    _clear_amx_loggers()
    yield log_dir
    _clear_amx_loggers()


def test_get_logger_attaches_rotating_file_handler(isolated_log_dir: Path) -> None:
    logger = get_logger("rotation_smoke")

    rotating = [h for h in logger.handlers if isinstance(h, RotatingFileHandler)]
    assert len(rotating) == 1, "expected exactly one RotatingFileHandler"

    handler = rotating[0]
    assert handler.maxBytes == LOG_MAX_BYTES
    assert handler.backupCount == LOG_BACKUP_COUNT
    # Active log path is ``<LOG_DIR>/amx.log`` so existing log shippers
    # keep working after the rotation upgrade.
    assert Path(handler.baseFilename).name == "amx.log"
    assert Path(handler.baseFilename).parent == isolated_log_dir


def test_log_event_writes_json_line_with_event_payload(isolated_log_dir: Path) -> None:
    log_event(
        "run.start",
        run_id="abcd1234",
        profile="prod_pg",
        scope="public.transactions",
    )

    # Flush every amx.events handler so the on-disk file is up to date.
    for handler in logging.getLogger("amx.events").handlers:
        handler.flush()

    log_path = isolated_log_dir / "amx.log"
    raw = log_path.read_text(encoding="utf-8").splitlines()
    records = [json.loads(line) for line in raw if line.strip()]

    matching = [r for r in records if r.get("logger") == "amx.events"]
    assert len(matching) == 1, "log_event should emit exactly one record"

    record = matching[0]
    assert record["level"] == "INFO"
    payload = json.loads(record["message"])
    assert payload == {
        "event": "run.start",
        "run_id": "abcd1234",
        "profile": "prod_pg",
        "scope": "public.transactions",
    }


def test_log_event_serialises_non_string_fields(isolated_log_dir: Path) -> None:
    """Numeric / bool fields survive JSON round-trip — important so
    downstream analytics can do arithmetic on duration_ms et al."""
    log_event("run.cancellation.completed", drained_in_ms=1234, partial=True)

    for handler in logging.getLogger("amx.events").handlers:
        handler.flush()

    raw = (isolated_log_dir / "amx.log").read_text(encoding="utf-8").splitlines()
    matching = [
        json.loads(line)
        for line in raw
        if line.strip() and json.loads(line).get("logger") == "amx.events"
    ]
    payload = json.loads(matching[0]["message"])
    assert payload == {
        "event": "run.cancellation.completed",
        "drained_in_ms": 1234,
        "partial": True,
    }


def test_log_event_carries_request_id(isolated_log_dir: Path) -> None:
    """``request_id`` set via ``set_request_id`` propagates into the
    structured event line — keeps run-scoped events groupable."""
    amx_logging.set_request_id("rid-zzz")
    try:
        log_event("run.agent.failed", agent="rag", reason="timeout")
    finally:
        amx_logging.clear_request_id()

    for handler in logging.getLogger("amx.events").handlers:
        handler.flush()

    raw = (isolated_log_dir / "amx.log").read_text(encoding="utf-8").splitlines()
    matching = [
        json.loads(line)
        for line in raw
        if line.strip() and json.loads(line).get("logger") == "amx.events"
    ]
    assert matching[0]["request_id"] == "rid-zzz"


def test_int_env_helper_clamps_below_minimum(monkeypatch: pytest.MonkeyPatch) -> None:
    """``AMX_LOG_MAX_BYTES=0`` should not produce a zero-byte rotation
    threshold — the helper enforces a minimum so a misconfiguration
    can't disable rotation entirely."""
    monkeypatch.setenv("AMX_LOG_MAX_BYTES", "0")
    assert amx_logging._int_env("AMX_LOG_MAX_BYTES", 4096, minimum=1024) == 1024


def test_int_env_helper_falls_back_on_garbage(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AMX_LOG_BACKUP_COUNT", "not-an-int")
    assert amx_logging._int_env("AMX_LOG_BACKUP_COUNT", 5, minimum=0) == 5


def test_mute_root_logger_blocks_amx_info_through_root_basicconfig_handler(
    isolated_log_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: Studio terminal used to flood with
    ``INFO:amx.db.connector:...`` lines because a third-party
    ``logging.basicConfig`` handler attached to root accepts records
    propagated from amx.* (Python's ``callHandlers`` consults handler
    levels, not ancestor logger levels). The mute helper must raise
    that handler's level AND drop amx.* propagation so subsequent late
    ``basicConfig`` calls also stay silent.
    """
    root = logging.getLogger()
    saved_level = root.level
    saved_handlers = list(root.handlers)
    saved_studio_flag = amx_logging._studio_mode_active
    monkeypatch.setattr(amx_logging, "_studio_mode_active", False, raising=False)

    # Snapshot the ``propagate`` flag of every amx.* logger that
    # exists right now, BEFORE mute_root_logger_for_studio flips them
    # all to ``False``. Without this snapshot the teardown can only
    # restore the three names we know about by hand, and any other
    # amx.* logger that happens to have been imported by an earlier
    # test (e.g. ``amx.agents._orchestrator.rerun``) stays muted for
    # the rest of the session — pytest's ``caplog`` relies on
    # propagation to the root logger, so the next test that asserts
    # against a record from that logger silently fails.
    saved_propagate: dict[str, bool] = {
        name: existing.propagate
        for name, existing in list(logging.Logger.manager.loggerDict.items())
        if isinstance(existing, logging.Logger) and (name == "amx" or name.startswith("amx."))
    }

    try:
        for handler in list(root.handlers):
            root.removeHandler(handler)

        sink = logging.StreamHandler()
        sink.setLevel(logging.NOTSET)
        root.addHandler(sink)

        db_log = get_logger("db.connector")
        llm_log = get_logger("llm.provider")

        amx_logging.mute_root_logger_for_studio()

        assert sink.level >= logging.WARNING
        assert db_log.propagate is False
        assert llm_log.propagate is False

        # New amx.* loggers created after mute must inherit the silence.
        new_log = get_logger("agents.rag_after_mute")
        assert new_log.propagate is False

        # Late basicConfig (lazy litellm import) installs a fresh
        # NOTSET handler on root — the propagate=False guard means
        # amx.* records still don't reach it.
        logging.basicConfig(force=True)
        late_handler = next(h for h in root.handlers if h is not sink)
        try:
            assert late_handler.level == logging.NOTSET
            assert db_log.propagate is False
        finally:
            root.removeHandler(late_handler)
    finally:
        for handler in list(root.handlers):
            root.removeHandler(handler)
        for handler in saved_handlers:
            root.addHandler(handler)
        root.setLevel(saved_level)
        amx_logging._studio_mode_active = saved_studio_flag
        # Restore the propagate flag for every amx.* logger we
        # observed before muting. Loggers created during the test
        # (e.g. ``amx.agents.rag_after_mute`` and ``amx.db.connector``
        # / ``amx.llm.provider`` if they did not pre-exist) inherit
        # the default ``propagate = True`` after the next session
        # so we restore them explicitly as well.
        for name, prev in saved_propagate.items():
            logging.getLogger(name).propagate = prev
        for name in ("amx.db.connector", "amx.llm.provider", "amx.agents.rag_after_mute"):
            if name not in saved_propagate:
                logging.getLogger(name).propagate = True
