"""Tests for the module-level skeleton-sync cancel registry."""

from __future__ import annotations

from amx.search import _skeleton_jobs


def setup_function() -> None:
    _skeleton_jobs._jobs.clear()


def test_register_returns_unset_event() -> None:
    event = _skeleton_jobs.register("prof")
    assert event.is_set() is False
    assert _skeleton_jobs.is_cancelled("prof") is False


def test_cancel_sets_event_and_returns_true() -> None:
    event = _skeleton_jobs.register("prof")
    assert _skeleton_jobs.cancel("prof") is True
    assert event.is_set() is True
    assert _skeleton_jobs.is_cancelled("prof") is True


def test_cancel_with_no_job_returns_false() -> None:
    assert _skeleton_jobs.cancel("missing") is False


def test_double_register_returns_same_event() -> None:
    first = _skeleton_jobs.register("prof")
    second = _skeleton_jobs.register("prof")
    assert first is second


def test_unregister_clears_job() -> None:
    _skeleton_jobs.register("prof")
    _skeleton_jobs.unregister("prof")
    assert _skeleton_jobs.is_cancelled("prof") is False
    assert _skeleton_jobs.cancel("prof") is False


def test_running_profiles_lists_registered() -> None:
    _skeleton_jobs.register("a")
    _skeleton_jobs.register("b")
    assert sorted(_skeleton_jobs.running_profiles()) == ["a", "b"]
