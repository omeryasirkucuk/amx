"""Pre-call output-token estimation.

``tiktoken`` measures the prompt exactly; the *output* — which dominates
cost on most modern models — has to be estimated. We learn a per-(agent,
model) ratio from the last ~30 days of recorded runs:

    ratio = AVG(completion_tokens / prompt_tokens)

and apply it to the freshly-counted prompt to project an output budget
the user can see *before* the LLM call goes out. When there is no
history (fresh install, brand-new agent/model combination) we fall
back to a per-agent default that matches AMX's structured-output
shape (Profile / RAG / Code agents emit ~50% the prompt size on
average; the merge agent is much terser).

The result feeds :func:`amx.utils.live_display.LiveDisplay.add_session_tokens`
so the header rendering can display "↓ 4.2k tokens · ~$0.0008" before
the call returns. Both surfaces label the projected number with a
leading ``~`` so the user knows it is a learned estimate, not an
exact figure.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("utils.cost_estimate")

# Per-agent fallback ratios used when no history exists. Tuned from
# observed completion/prompt averages across AMX's typical workloads
# (English metadata generation against medium-cardinality DBs).
_DEFAULT_RATIOS: dict[str, float] = {
    "profile_agent": 0.60,
    "profile_agent(batch)": 0.50,
    "rag_agent": 0.40,
    "rag_agent(batch)": 0.40,
    "code_agent": 0.50,
    "code_agent(batch)": 0.50,
    "merge": 0.30,
    "equivalence_agent": 0.45,
}
_GLOBAL_DEFAULT_RATIO = 0.55

# Sample-size floor: fewer than this many historical records and we do
# not trust the empirical mean enough to override the per-agent default.
# Keeps the very first run on a new model from anchoring on a single
# outlier.
_MIN_SAMPLES = 3
# History window for the running average. 30 days * 86400 sec.
_LEARNING_WINDOW_SEC = 30 * 86_400.0
# Cache invalidation TTL: refresh the learned table every 5 minutes
# during a long session so a long-running process picks up new data
# from completed runs without restarting.
_CACHE_REFRESH_SEC = 300.0


_LOCK = threading.Lock()
_RATIO_CACHE: dict[tuple[str, str], float] = {}
_CACHE_LOADED_AT: float = 0.0


def _default_ratio(agent_name: str) -> float:
    return _DEFAULT_RATIOS.get(agent_name, _GLOBAL_DEFAULT_RATIO)


def _query_history_ratios(history_store: Any) -> dict[tuple[str, str], tuple[float, int]]:
    """Aggregate ``completion / prompt`` ratios per (agent, model) over the window.

    Returns ``{(step, model_lower): (mean_ratio, sample_count)}``. We
    walk every ``analysis_runs.tokens_json.records`` entry within
    ``_LEARNING_WINDOW_SEC`` of now; old runs are excluded so a tariff
    change or model swap does not keep biasing the estimate forever.

    Robust to malformed payloads — any individual record that fails to
    parse is silently skipped. The function never raises (returns ``{}``
    on a wholesale read failure) so a corrupt history database cannot
    break a live run.
    """
    if history_store is None:
        return {}
    cutoff = time.time() - _LEARNING_WINDOW_SEC
    aggregates: dict[tuple[str, str], list[float]] = {}
    try:
        with history_store._connect() as conn:  # noqa: SLF001 - read-only
            rows = conn.execute(
                """
                SELECT tokens_json
                FROM analysis_runs
                WHERE started_at >= ?
                  AND tokens_json IS NOT NULL
                  AND tokens_json != ''
                """,
                (cutoff,),
            ).fetchall()
    except Exception as exc:  # noqa: BLE001 - cost estimate is opportunistic
        log.debug("learned-ratio history read failed: %s", exc)
        return {}
    for row in rows:
        raw = row[0] if not isinstance(row, dict) else row.get("tokens_json")
        if not raw:
            continue
        try:
            payload = json.loads(raw) if isinstance(raw, str) else dict(raw)
        except Exception:
            continue
        for record in payload.get("records") or []:
            if not isinstance(record, dict):
                continue
            prompt = float(record.get("prompt_tokens") or 0.0)
            completion = float(record.get("completion_tokens") or 0.0)
            if prompt <= 0 or completion <= 0:
                continue
            step = str(record.get("step") or "").strip()
            model = str(record.get("model") or "").strip().lower()
            if not step:
                continue
            key = (step, model)
            ratio = completion / prompt
            # Clamp pathological ratios. Reasoning models that burn
            # 8k thinking tokens on a 100-token prompt produce ratios
            # like 80x — useful as a "this run was huge" signal, but
            # poisonous when fed into an *average* used for pre-call
            # estimates.
            if 0.05 <= ratio <= 8.0:
                aggregates.setdefault(key, []).append(ratio)
    return {key: (sum(v) / len(v), len(v)) for key, v in aggregates.items() if v}


def _refresh_cache_if_due(history_store: Any) -> None:
    global _CACHE_LOADED_AT
    now = time.time()
    if now - _CACHE_LOADED_AT < _CACHE_REFRESH_SEC and _CACHE_LOADED_AT > 0:
        return
    learned = _query_history_ratios(history_store)
    with _LOCK:
        _RATIO_CACHE.clear()
        for key, (mean_ratio, samples) in learned.items():
            if samples >= _MIN_SAMPLES:
                _RATIO_CACHE[key] = mean_ratio
        _CACHE_LOADED_AT = now


def estimate_completion_tokens(
    *,
    agent_name: str,
    model: str,
    prompt_tokens: int,
    history_store: Any = None,
) -> int:
    """Project the completion-token count for the next LLM call.

    ``history_store`` is an :class:`amx.storage.sqlite_store.SQLiteHistoryStore`
    (typed ``Any`` to avoid the cyclic import). When ``None`` we skip
    the learning step and return the per-agent default * ``prompt_tokens``.
    """
    if prompt_tokens <= 0:
        return 0
    if history_store is not None:
        _refresh_cache_if_due(history_store)
    key = (agent_name.strip(), (model or "").strip().lower())
    ratio = _RATIO_CACHE.get(key)
    if ratio is None:
        ratio = _default_ratio(agent_name.strip())
    return max(1, int(prompt_tokens * ratio))


def estimate_cost_usd(
    *,
    agent_name: str,
    model: str,
    prompt_tokens: int,
    price: Any,  # ModelPrice; typed Any to avoid the cyclic import
    history_store: Any = None,
) -> float:
    """Convenience: combine :func:`estimate_completion_tokens` with a price."""
    completion_estimate = estimate_completion_tokens(
        agent_name=agent_name,
        model=model,
        prompt_tokens=prompt_tokens,
        history_store=history_store,
    )
    in_rate = float(getattr(price, "input_per_mtok", 0.0) or 0.0)
    out_rate = float(getattr(price, "output_per_mtok", 0.0) or 0.0)
    return (prompt_tokens / 1_000_000.0) * in_rate + (completion_estimate / 1_000_000.0) * out_rate


def reset_cache() -> None:
    """Drop the learned-ratio cache. Tests + ``/refresh-prices`` invoke this."""
    global _CACHE_LOADED_AT
    with _LOCK:
        _RATIO_CACHE.clear()
        _CACHE_LOADED_AT = 0.0


__all__ = [
    "estimate_completion_tokens",
    "estimate_cost_usd",
    "reset_cache",
]
