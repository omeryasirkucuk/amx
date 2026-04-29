"""Retrieval evaluation harness for AMX search.

This package collects light-weight scoring metrics and a smoke harness
that exercises the retrieval surface without requiring a live database
or a populated Chroma collection. Real evals — comparing MiniLM vs
OpenAI vs SentenceTransformers on an actual corpus — drop fixture files
into ``tests/eval/fixtures/`` and run::

    pytest tests/eval/

The metrics live in :mod:`tests.eval.metrics` so they can be re-used
by ad-hoc scripts and downstream notebooks without re-imports of the
full test suite.
"""
