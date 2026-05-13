"""Shared runtime utilities for AMX run workers.

This package holds primitives consumed by both the Studio FastAPI workers
and the upcoming scheduler engine — currently just the file-backed
advisory lock used to serialise concurrent run workers on a per-table
basis.
"""
