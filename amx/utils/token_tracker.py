"""Token counting (tiktoken) and per-session usage + cost tracking.

``tiktoken`` is intentionally NOT imported at module top: the tracker
module sits on the boot path (every ``cli_support/commands/*`` pulls
it in), and tiktoken's ~10 MB BPE-table load was previously charged
to every CLI invocation — including ``amx /db-profiles`` which
doesn't count tokens. The lazy-import inside ``_get_encoding`` keeps
the tracker cheap to import and defers the cost to the first
``estimate_tokens`` call. The same lazy boundary doubles as the
on-demand pip-install hook for users who never run a token-counting
flow at all (see ``amx.utils.optional_deps``).

Cost integration: every :meth:`TokenTracker.record` call now resolves
a price (via :func:`amx.llm.pricing.lookup_price`) and stores per-call
USD figures alongside the raw token counts. Run summaries, the
``/usage`` command, and the Studio run-detail page read these fields
directly so cost is always visible without a second SQL query.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import tiktoken


@lru_cache(maxsize=1)
def _get_encoding() -> tiktoken.Encoding:
    from amx.utils.optional_deps import ensure

    ensure(["tiktoken"], feature="token counting")
    import tiktoken

    return tiktoken.get_encoding("cl100k_base")


def estimate_tokens(messages: list[dict[str, str]]) -> int:
    enc = _get_encoding()
    total = 0
    for msg in messages:
        total += 4  # role/name/separator framing
        for value in msg.values():
            total += len(enc.encode(value, disallowed_special=()))
    total += 2  # reply priming
    return max(1, total)


@dataclass
class _UsageRecord:
    step: str
    input_estimate: int
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    model_processing_sec: float
    # Cost fields populated when the caller passes provider/model/cfg
    # (or uses ``record_for``). Frozen at the moment of the call so a
    # later price refresh does not retro-rewrite the audit trail.
    input_cost_usd: float = 0.0
    output_cost_usd: float = 0.0
    price_source: str = ""  # user_override | litellm | openrouter | fallback | unknown | ""
    provider: str = ""
    model: str = ""

    @property
    def total_cost_usd(self) -> float:
        return float(self.input_cost_usd) + float(self.output_cost_usd)


class TokenTracker:
    """Accumulates token usage + cost across a session (singleton via module-level instance)."""

    def __init__(self) -> None:
        self._records: list[_UsageRecord] = []

    def reset(self) -> None:
        self._records.clear()

    def record(
        self,
        step: str,
        input_estimate: int,
        usage: dict | None = None,
        *,
        provider: str | None = None,
        model: str | None = None,
        cfg: Any = None,
        profile_name: str | None = None,
    ) -> None:
        """Record one LLM call's token + cost footprint.

        Backwards compatible: callers that do not pass ``provider`` /
        ``model`` / ``cfg`` simply skip the cost computation and the
        record's cost fields stay zero. Existing call sites continue to
        work; new agent integrations should prefer :meth:`record_for`.
        """
        prompt = 0
        completion = 0
        total = 0
        model_processing_sec = 0.0
        if usage:
            prompt = int(getattr(usage, "prompt_tokens", 0) or usage.get("prompt_tokens", 0) or 0)
            completion = int(
                getattr(usage, "completion_tokens", 0) or usage.get("completion_tokens", 0) or 0
            )
            total = int(getattr(usage, "total_tokens", 0) or usage.get("total_tokens", 0) or 0)
            if not total:
                total = prompt + completion
            model_processing_sec = float(
                getattr(usage, "model_processing_sec", 0.0)
                or usage.get("model_processing_sec", 0.0)
                or 0.0
            )

        in_cost = 0.0
        out_cost = 0.0
        price_source = ""
        if provider and model:
            try:
                from amx.llm.pricing import compute_cost, lookup_price

                price = lookup_price(cfg, provider=provider, model=model, profile_name=profile_name)
                in_cost, out_cost, _ = compute_cost(
                    prompt_tokens=prompt or input_estimate,
                    completion_tokens=completion,
                    price=price,
                )
                price_source = price.source
            except Exception as exc:  # noqa: BLE001 - cost is opportunistic
                from amx.utils.logging import get_logger

                get_logger("utils.token_tracker").debug(
                    "cost lookup failed for %s/%s: %s", provider, model, exc
                )

        self._records.append(
            _UsageRecord(
                step=step,
                input_estimate=input_estimate,
                prompt_tokens=prompt,
                completion_tokens=completion,
                total_tokens=total,
                model_processing_sec=model_processing_sec,
                input_cost_usd=in_cost,
                output_cost_usd=out_cost,
                price_source=price_source,
                provider=str(provider or ""),
                model=str(model or ""),
            )
        )
        try:
            from amx.utils.live_display import get_display

            display = get_display()
            if display.is_active:
                display.add_session_tokens(
                    input_tokens=prompt,
                    output_tokens=completion,
                    cost_delta_usd=in_cost + out_cost,
                )
        except Exception:
            pass

    def record_for(
        self,
        step: str,
        input_estimate: int,
        llm: Any,
        usage: dict | None = None,
    ) -> None:
        """Convenience wrapper that pulls provider/model/cfg off an
        :class:`amx.llm.provider.LLMProvider`-shaped object.

        Lets call sites stay one-line: ``tracker.record_for("profile",
        est, self.llm, result.usage)``. The agent's ``self.llm.cfg``
        carries the active provider, model, and any custom cost
        overrides — exactly what :func:`amx.llm.pricing.lookup_price`
        needs.
        """
        cfg = getattr(llm, "cfg", None)
        provider = getattr(cfg, "provider", "") if cfg is not None else ""
        model = getattr(cfg, "model", "") if cfg is not None else ""
        self.record(step, input_estimate, usage, provider=provider, model=model, cfg=cfg)

    def summary(self) -> list[tuple[str, int, int, int, float]]:
        """Aggregate records by step name -> ``(step, input, output, total, cost_usd)``.

        The new fifth field carries the summed USD cost for that step.
        Callers that only consume the first four (legacy 4-tuple) keep
        working via tuple unpacking.
        """
        agg: dict[str, list[float]] = {}
        for r in self._records:
            if r.step not in agg:
                agg[r.step] = [0, 0, 0, 0.0]
            agg[r.step][0] += r.prompt_tokens or r.input_estimate
            agg[r.step][1] += r.completion_tokens
            agg[r.step][2] += r.total_tokens or (r.prompt_tokens + r.completion_tokens)
            agg[r.step][3] += r.input_cost_usd + r.output_cost_usd
        return [
            (step, int(vals[0]), int(vals[1]), int(vals[2]), float(vals[3]))
            for step, vals in agg.items()
        ]

    @property
    def total_tokens(self) -> int:
        return sum(r.total_tokens or (r.prompt_tokens + r.completion_tokens) for r in self._records)

    @property
    def total_cost_usd(self) -> float:
        return float(sum(r.input_cost_usd + r.output_cost_usd for r in self._records))

    @property
    def has_records(self) -> bool:
        return bool(self._records)

    @property
    def total_model_processing_sec(self) -> float:
        return float(sum(max(0.0, r.model_processing_sec) for r in self._records))

    def records(self) -> list[dict[str, Any]]:
        """Return raw token + cost records for persistence/analytics."""
        return [
            {
                "step": r.step,
                "input_estimate": r.input_estimate,
                "prompt_tokens": r.prompt_tokens,
                "completion_tokens": r.completion_tokens,
                "total_tokens": r.total_tokens,
                "model_processing_sec": round(float(r.model_processing_sec), 6),
                "input_cost_usd": round(float(r.input_cost_usd), 8),
                "output_cost_usd": round(float(r.output_cost_usd), 8),
                "price_source": r.price_source,
                "provider": r.provider,
                "model": r.model,
            }
            for r in self._records
        ]

    def drop_steps(self, blocked_steps: set[str]) -> None:
        """Remove records for exact step names from the tracker."""
        if not blocked_steps:
            return
        self._records = [r for r in self._records if r.step not in blocked_steps]


tracker = TokenTracker()
