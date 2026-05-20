"""Tests for the per-backend default-container helper."""

from __future__ import annotations

from types import SimpleNamespace

from amx.db._default_scope import profile_default_container


def test_databricks_catalog_wins() -> None:
    db = SimpleNamespace(backend="databricks", catalog="prod", database="", dataset="")
    assert profile_default_container(db) == "prod"


def test_bigquery_dataset() -> None:
    db = SimpleNamespace(backend="bigquery", catalog="", dataset="analytics", database="")
    assert profile_default_container(db) == "analytics"


def test_snowflake_database() -> None:
    db = SimpleNamespace(backend="snowflake", catalog="", dataset="", database="DW")
    assert profile_default_container(db) == "DW"


def test_postgres_database() -> None:
    db = SimpleNamespace(backend="postgres", catalog="", dataset="", database="app")
    assert profile_default_container(db) == "app"


def test_empty_profile_returns_none() -> None:
    db = SimpleNamespace(backend="postgres", catalog="", dataset="", database="")
    assert profile_default_container(db) is None


def test_none_input_returns_none() -> None:
    assert profile_default_container(None) is None


def test_catalog_beats_database_when_both_set() -> None:
    db = SimpleNamespace(backend="trino", catalog="hive", database="default", dataset="")
    assert profile_default_container(db) == "hive"
