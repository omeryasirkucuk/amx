"""Smoke test: concurrent Chroma writes from two threads in one process.

Two angles:

* Distinct collections — should be fully parallel.
* Same collection — Chroma's own per-collection lock may serialise, but
  the goal is "no corruption, both writes land".

If ``chromadb`` is not importable in the current environment the test is
skipped so we don't fail under stripped CI installs.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

chromadb = pytest.importorskip("chromadb")


def test_two_threads_write_distinct_collections(tmp_path: Path) -> None:
    client = chromadb.PersistentClient(path=str(tmp_path))
    errors: list[BaseException] = []
    err_lock = threading.Lock()

    def worker(name: str) -> None:
        try:
            col = client.get_or_create_collection(name)
            col.add(
                ids=[f"{name}-{i}" for i in range(20)],
                documents=[f"doc {i}" for i in range(20)],
            )
        except BaseException as exc:  # noqa: BLE001
            with err_lock:
                errors.append(exc)

    t1 = threading.Thread(target=worker, args=("col_a",))
    t2 = threading.Thread(target=worker, args=("col_b",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert errors == [], f"distinct-collection writes failed: {errors!r}"
    assert client.get_collection("col_a").count() == 20
    assert client.get_collection("col_b").count() == 20


def test_two_threads_write_same_collection_no_corruption(tmp_path: Path) -> None:
    client = chromadb.PersistentClient(path=str(tmp_path))
    col = client.get_or_create_collection("shared")
    errors: list[BaseException] = []
    err_lock = threading.Lock()

    def worker(offset: int) -> None:
        try:
            col.add(
                ids=[f"x-{offset}-{i}" for i in range(20)],
                documents=[f"d {offset}-{i}" for i in range(20)],
            )
        except BaseException as exc:  # noqa: BLE001
            with err_lock:
                errors.append(exc)

    t1 = threading.Thread(target=worker, args=(0,))
    t2 = threading.Thread(target=worker, args=(1000,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert errors == [], f"same-collection writes failed: {errors!r}"
    assert col.count() == 40
