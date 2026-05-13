"""LiteLLM start-up chatter is silenced before the user sees it.

Reported: on a corporate network with a MITM TLS proxy, LiteLLM's
remote model-cost-map fetch fails the SSL handshake and emits a
WARNING line every time the agent dispatches:

    LiteLLM:WARNING: Failed to fetch remote model cost map from
    https://raw.githubusercontent.com/.../model_prices_and_context_window.json:
    [SSL: CERTIFICATE_VERIFY_FAILED] ... Falling back to local backup.

The warning is informational — LiteLLM ships a local backup and uses
it — but it clutters the otherwise-clean ``ask>`` prompt. Two fixes,
both inside ``_litellm()``:

1. Set ``LITELLM_LOCAL_MODEL_COST_MAP=True`` *before* importing the
   library, so it skips the network call entirely.
2. Pin the LiteLLM and litellm loggers to NullHandler with level
   ``CRITICAL+1`` BEFORE the import (and again after, defensively)
   so any other start-up chatter the library might add gets dropped.
"""

from __future__ import annotations

import logging
import os
from unittest.mock import patch

from amx.llm import provider as llm_provider


def test_local_model_cost_map_env_set_before_import() -> None:
    """``_litellm()`` must set ``LITELLM_LOCAL_MODEL_COST_MAP=True``
    so litellm skips the GitHub fetch (which fires the SSL warning
    on corp networks with TLS proxies).
    """
    # Reset both the cached module ref *and* the env var so we exercise
    # the first-import code path even when an earlier test already
    # warmed up the litellm module. Without the ``patch.object`` the
    # bootstrap returns the cached module and never touches the env
    # var, leaving the assertion at the mercy of pytest's collection
    # order — historically this test happened to run before any other
    # test imported litellm, but adding more test files can swap that
    # ordering and trip the assertion.
    saved = os.environ.pop("LITELLM_LOCAL_MODEL_COST_MAP", None)
    try:
        with patch.object(llm_provider, "_litellm_module", None):
            llm_provider._litellm()
        assert os.environ.get("LITELLM_LOCAL_MODEL_COST_MAP") == "True"
    finally:
        if saved is None:
            os.environ.pop("LITELLM_LOCAL_MODEL_COST_MAP", None)
        else:
            os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = saved


def test_litellm_loggers_silenced() -> None:
    """The ``LiteLLM`` and ``litellm`` Python loggers should reach a
    state where any WARNING-level record they emit is dropped — no
    handler attached, no propagation to the root logger, level set
    above WARNING.
    """
    llm_provider._litellm()
    for name in ("LiteLLM", "litellm"):
        ext = logging.getLogger(name)
        # Level above WARNING — filter drops the cost-map warning.
        assert ext.level >= logging.CRITICAL
        # propagate disabled so the root handler doesn't print it.
        assert ext.propagate is False
        # Only NullHandler attached (or none).
        non_null = [h for h in ext.handlers if not isinstance(h, logging.NullHandler)]
        assert non_null == []


def test_existing_env_var_is_respected() -> None:
    """If the user has explicitly set ``LITELLM_LOCAL_MODEL_COST_MAP``
    to a non-default value (e.g. False to opt back into remote
    fetching for some reason), our setdefault must not clobber it.
    """
    saved = os.environ.get("LITELLM_LOCAL_MODEL_COST_MAP")
    try:
        os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "False"
        # Force re-init by stomping the cached module reference.
        with patch.object(llm_provider, "_litellm_module", None):
            llm_provider._litellm()
        assert os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] == "False"
    finally:
        if saved is None:
            os.environ.pop("LITELLM_LOCAL_MODEL_COST_MAP", None)
        else:
            os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = saved
