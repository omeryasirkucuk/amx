"""Live LLM cost tracking — unit tests.

Covers:

* Fetcher parsing (LiteLLM JSON ``cost_per_token`` -> ``$/Mtok`` scale).
* OpenRouter fallback for routes the LiteLLM JSON misses.
* User-override resolution: wins over fetched, half-override = no
  override.
* Bundled fallback hit when both network sources fail.
* Cache TTL + freshness flags.
* Refresh idempotency (skip when fresh, force-fetch when forced).
* ``compute_cost`` math.
* ``TokenTracker`` cost integration round-trip.
* Pre-call ``estimate_completion_tokens`` learned-ratio + fallback.
"""

from __future__ import annotations

import json
import ssl
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from amx.llm import pricing as pricing_mod
from amx.llm.pricing import (
    ModelPrice,
    _build_ssl_context,
    _http_get_json,
    cache_age_seconds,
    cache_info,
    compute_cost,
    fetch_litellm_prices,
    fetch_openrouter_prices,
    lookup_price,
    refresh_prices,
    reset_state_for_tests,
)


@pytest.fixture(autouse=True)
def _reset_pricing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Each test starts with a fresh in-memory + disk-cache state."""
    reset_state_for_tests()
    monkeypatch.setattr(pricing_mod, "_cache_path", lambda: tmp_path / "pricing-cache.json")
    yield
    reset_state_for_tests()


# ── Fetcher parsing ────────────────────────────────────────────────────────


def test_fetch_litellm_parses_cost_per_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """``cost_per_token`` is per-token; we must scale it to $/Mtok."""
    fake_payload = {
        "sample_spec": {"input_cost_per_token": 0.0},  # excluded sentinel
        "gpt-mock": {
            "input_cost_per_token": 0.0000015,  # = $1.50 / Mtok
            "output_cost_per_token": 0.000006,  # = $6.00 / Mtok
        },
        "no-cost-listed": {"max_tokens": 4096},
    }
    monkeypatch.setattr(pricing_mod, "_http_get_json", lambda url, headers=None: fake_payload)
    out = fetch_litellm_prices()
    assert "sample_spec" not in out
    assert "no-cost-listed" not in out
    assert out["gpt-mock"].input_per_mtok == pytest.approx(1.50)
    assert out["gpt-mock"].output_per_mtok == pytest.approx(6.00)
    assert out["gpt-mock"].source == "litellm"
    assert out["gpt-mock"].fetched_at is not None


# ── SSL context for stdlib urlopen ─────────────────────────────────────────


def _clear_pricing_ssl_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip the SSL env vars the helper consults so each test starts
    from a known floor regardless of the developer's shell."""
    for var in ("AMX_INSECURE_SSL", "AMX_CA_BUNDLE", "SSL_CERT_FILE"):
        monkeypatch.delenv(var, raising=False)


def test_build_ssl_context_uses_certifi_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression: plain Windows Python ships no default CA bundle, so a
    bare ``urlopen`` raises CERTIFICATE_VERIFY_FAILED. The helper must
    fall back to ``certifi.where()`` so the price fetch succeeds out of
    the box without any user env-var configuration."""
    pytest.importorskip("certifi")
    import certifi

    _clear_pricing_ssl_env(monkeypatch)
    ctx = _build_ssl_context()

    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True
    # Loaded a non-empty CA list — the certifi branch only "works" if
    # the bundled bundle actually populates the context.
    assert ctx.get_ca_certs(), "expected certifi CA bundle to populate context"


def test_build_ssl_context_honours_amx_ca_bundle(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Corporate networks set ``AMX_CA_BUNDLE`` to a re-signed root.
    The helper must read that file, not silently fall through to
    certifi (which would not contain the corporate root and would
    fail TLS verification against the proxy)."""
    # Use certifi's bundle as a real, parseable PEM under a controlled
    # path so we can assert the helper picked OUR file by name.
    pytest.importorskip("certifi")
    import certifi

    bundle_pem = Path(certifi.where()).read_bytes()
    custom_bundle = tmp_path / "corp-root.pem"
    custom_bundle.write_bytes(bundle_pem)

    _clear_pricing_ssl_env(monkeypatch)
    monkeypatch.setenv("AMX_CA_BUNDLE", str(custom_bundle))

    ctx = _build_ssl_context()
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.get_ca_certs(), "expected AMX_CA_BUNDLE PEM to populate context"


def test_build_ssl_context_unverified_when_insecure_ssl_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``AMX_INSECURE_SSL=1`` is the documented escape hatch for hostile
    networks. The helper must mirror the LLM provider's contract — an
    unverified context with hostname check disabled — so the price
    fetch behaves the same way the LLM call does in that mode."""
    _clear_pricing_ssl_env(monkeypatch)
    monkeypatch.setenv("AMX_INSECURE_SSL", "1")

    ctx = _build_ssl_context()
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.check_hostname is False
    assert ctx.verify_mode == ssl.CERT_NONE


def test_http_get_json_passes_context_to_urlopen(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wiring guard: a future refactor must not silently drop the
    ``context=`` kwarg. ``urlopen`` without a context is exactly the
    bug the Windows user hit — locking this in keeps the fix from
    regressing through an innocent-looking edit."""
    captured: dict[str, object] = {}

    class _FakeResponse:
        def read(self) -> bytes:
            return b'{"ok": true}'

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, *exc: object) -> None:
            return None

    def _fake_urlopen(req: object, *args: object, **kwargs: object) -> _FakeResponse:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return _FakeResponse()

    monkeypatch.setattr(pricing_mod.urllib.request, "urlopen", _fake_urlopen)
    out = _http_get_json("https://example.invalid/pricing.json")

    assert out == {"ok": True}
    ctx = captured["kwargs"].get("context")
    assert isinstance(ctx, ssl.SSLContext), (
        "_http_get_json must pass context=SSLContext to urlopen — without it, "
        "Windows Python raises CERTIFICATE_VERIFY_FAILED on every HTTPS host"
    )


def test_fetch_openrouter_parses_pricing_block(monkeypatch: pytest.MonkeyPatch) -> None:
    """OpenRouter wraps the rates as strings under ``pricing.{prompt,completion}``."""
    fake_payload = {
        "data": [
            {
                "id": "openai/gpt-route",
                "pricing": {"prompt": "0.0000005", "completion": "0.000002"},
            },
            {"id": "skipme", "pricing": "broken-not-a-dict"},
        ]
    }
    monkeypatch.setattr(pricing_mod, "_http_get_json", lambda url, headers=None: fake_payload)
    out = fetch_openrouter_prices()
    assert "skipme" not in out
    hit = out["openai/gpt-route"]
    assert hit.input_per_mtok == pytest.approx(0.50)
    assert hit.output_per_mtok == pytest.approx(2.00)
    assert hit.source == "openrouter"


# ── Lookup resolution order ────────────────────────────────────────────────


def test_user_override_wins_over_fetched(monkeypatch: pytest.MonkeyPatch) -> None:
    """A profile's custom rates outrank LiteLLM / OpenRouter / fallback."""
    cfg = SimpleNamespace(
        llm_profiles={
            "p": SimpleNamespace(
                custom_input_cost_per_mtok=0.50,
                custom_output_cost_per_mtok=2.50,
            )
        },
        llm=SimpleNamespace(
            custom_input_cost_per_mtok=None,
            custom_output_cost_per_mtok=None,
        ),
    )
    price = lookup_price(cfg, provider="openai", model="gpt-4o-mini", profile_name="p")
    assert price.source == "user_override"
    assert price.input_per_mtok == 0.50
    assert price.output_per_mtok == 2.50


def test_half_override_falls_through_to_fetched() -> None:
    """Setting only one of the two rates is treated as no override at all."""
    cfg = SimpleNamespace(
        llm_profiles={
            "p": SimpleNamespace(
                custom_input_cost_per_mtok=0.50,
                custom_output_cost_per_mtok=None,  # half-override
            )
        },
        llm=SimpleNamespace(
            custom_input_cost_per_mtok=None,
            custom_output_cost_per_mtok=None,
        ),
    )
    price = lookup_price(cfg, provider="openai", model="gpt-4o-mini", profile_name="p")
    assert price.source != "user_override"
    # Bundled fallback should hit gpt-4o-mini.
    assert price.is_known


def test_bundled_fallback_hit_when_no_network() -> None:
    """Without network fetches, the bundled snapshot must still answer."""
    price = lookup_price(None, provider="anthropic", model="claude-haiku-4.5")
    assert price.source == "fallback"
    assert price.input_per_mtok > 0


def test_unknown_model_returns_zero_with_unknown_source() -> None:
    price = lookup_price(None, provider="alien", model="totally-not-a-model")
    assert price.source == "unknown"
    assert price.input_per_mtok == 0.0
    assert price.output_per_mtok == 0.0


# ── Cache + refresh ────────────────────────────────────────────────────────


def test_refresh_skips_when_cache_fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """A fresh cache must short-circuit ``refresh_prices(force=False)``."""

    fake_payload = {"data": []}

    def fake_litellm() -> dict[str, ModelPrice]:
        return {"x": ModelPrice(1, 1, "litellm", time.time())}

    def fake_openrouter() -> dict[str, ModelPrice]:
        return {"y": ModelPrice(2, 2, "openrouter", time.time())}

    monkeypatch.setattr(pricing_mod, "fetch_litellm_prices", fake_litellm)
    monkeypatch.setattr(pricing_mod, "fetch_openrouter_prices", fake_openrouter)
    monkeypatch.setattr(pricing_mod, "_http_get_json", lambda *a, **k: fake_payload)

    refresh_prices(force=True)  # populate
    second = refresh_prices(force=False)
    assert second.get("skipped") is True


def test_refresh_force_writes_cache(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Force-refresh must hit the wire and persist a new cache file."""
    monkeypatch.setattr(
        pricing_mod,
        "fetch_litellm_prices",
        lambda: {"a": ModelPrice(1.0, 2.0, "litellm", time.time())},
    )
    monkeypatch.setattr(pricing_mod, "fetch_openrouter_prices", lambda: {})
    out = refresh_prices(force=True)
    assert out["litellm"] == 1
    assert out.get("skipped") is False
    cache_file = pricing_mod._cache_path()
    assert cache_file.exists()
    payload = json.loads(cache_file.read_text())
    assert "a" in payload["litellm"]


def test_refresh_records_errors_without_raising(monkeypatch: pytest.MonkeyPatch) -> None:
    """A single source failure must not poison the other or raise."""
    import urllib.error

    def boom() -> dict[str, ModelPrice]:
        raise urllib.error.URLError("simulated offline")

    monkeypatch.setattr(pricing_mod, "fetch_litellm_prices", boom)
    monkeypatch.setattr(
        pricing_mod,
        "fetch_openrouter_prices",
        lambda: {"good": ModelPrice(1, 1, "openrouter", time.time())},
    )
    out = refresh_prices(force=True)
    assert any("litellm" in e for e in out["errors"])
    assert out["openrouter"] == 1


def test_cache_age_reports_none_before_first_fetch() -> None:
    assert cache_age_seconds() is None
    assert cache_info()["is_stale"] is True


# ── compute_cost ───────────────────────────────────────────────────────────


def test_compute_cost_math() -> None:
    """Spot-check the arithmetic so a future refactor cannot drift."""
    price = ModelPrice(input_per_mtok=2.0, output_per_mtok=10.0, source="test")
    in_usd, out_usd, total = compute_cost(
        prompt_tokens=500_000, completion_tokens=100_000, price=price
    )
    assert in_usd == pytest.approx(1.0)
    assert out_usd == pytest.approx(1.0)
    assert total == pytest.approx(2.0)


def test_compute_cost_clamps_negative_token_counts() -> None:
    price = ModelPrice(input_per_mtok=10.0, output_per_mtok=10.0, source="test")
    _i, _o, total = compute_cost(prompt_tokens=-5, completion_tokens=-9, price=price)
    assert total == 0.0


# ── TokenTracker cost integration ──────────────────────────────────────────


def test_token_tracker_records_cost_when_provider_supplied() -> None:
    from amx.utils.token_tracker import TokenTracker

    cfg = SimpleNamespace(
        llm_profiles={},
        llm=SimpleNamespace(
            custom_input_cost_per_mtok=2.0,
            custom_output_cost_per_mtok=10.0,
        ),
    )
    t = TokenTracker()
    t.record(
        "profile_agent",
        100,
        {"prompt_tokens": 500_000, "completion_tokens": 100_000},
        provider="anthropic",
        model="claude-sonnet-4",
        cfg=cfg,
    )
    summary = t.summary()
    assert len(summary) == 1
    step, inp, out, total, cost = summary[0]
    assert step == "profile_agent"
    assert inp == 500_000
    assert out == 100_000
    # Custom override -> $1.0 input + $1.0 output = $2.0
    assert cost == pytest.approx(2.0)
    assert t.total_cost_usd == pytest.approx(2.0)


def test_token_tracker_records_zero_cost_when_no_provider_passed() -> None:
    """Backwards-compat path: existing callers that omit provider/model
    still get zero cost without crashing."""
    from amx.utils.token_tracker import TokenTracker

    t = TokenTracker()
    t.record(
        "profile_agent",
        100,
        {"prompt_tokens": 1000, "completion_tokens": 500},
    )
    assert t.total_cost_usd == 0.0


def test_token_tracker_records_method_round_trip_includes_cost_fields() -> None:
    from amx.utils.token_tracker import TokenTracker

    cfg = SimpleNamespace(
        llm_profiles={},
        llm=SimpleNamespace(
            custom_input_cost_per_mtok=1.0,
            custom_output_cost_per_mtok=4.0,
        ),
    )
    t = TokenTracker()
    t.record(
        "merge",
        50,
        {"prompt_tokens": 10_000, "completion_tokens": 2_000},
        provider="openai",
        model="gpt-4o",
        cfg=cfg,
    )
    rec = t.records()[0]
    assert rec["price_source"] == "user_override"
    assert rec["input_cost_usd"] == pytest.approx(0.01)
    assert rec["output_cost_usd"] == pytest.approx(0.008)
    assert rec["provider"] == "openai"
    assert rec["model"] == "gpt-4o"


# ── Pre-call estimator ─────────────────────────────────────────────────────


def test_estimate_completion_tokens_uses_per_agent_default_with_no_history() -> None:
    from amx.utils.cost_estimate import estimate_completion_tokens, reset_cache

    reset_cache()
    # profile_agent default ratio is 0.60 -> 100 prompt -> ~60 output.
    out = estimate_completion_tokens(
        agent_name="profile_agent",
        model="gpt-4o-mini",
        prompt_tokens=100,
        history_store=None,
    )
    assert 50 <= out <= 70


def test_estimate_completion_tokens_prefers_history_over_default(
    tmp_path: Path,
) -> None:
    """Once enough samples exist for (step, model), the learned ratio
    should override the per-agent default."""
    from amx.storage.sqlite_store import SQLiteHistoryStore
    from amx.utils.cost_estimate import estimate_completion_tokens, reset_cache

    reset_cache()
    store = SQLiteHistoryStore(tmp_path / "h.db")
    store.init()
    # Seed three runs with a 0.10 completion/prompt ratio for one
    # (agent, model) — well below the per-agent default of 0.60.
    for _ in range(3):
        run_id = store.create_run(
            command="analyze.run",
            mode="chat",
            db_backend="postgresql",
            db_profile="local",
            llm_provider="openai",
            llm_model="gpt-test",
            scope={"public": ["t"]},
        )
        store.finish_run(
            run_id,
            status="success",
            metrics={},
            tokens={
                "records": [
                    {
                        "step": "profile_agent",
                        "prompt_tokens": 1000,
                        "completion_tokens": 100,
                        "model": "gpt-test",
                    }
                ]
            },
            results={},
        )
    out = estimate_completion_tokens(
        agent_name="profile_agent",
        model="gpt-test",
        prompt_tokens=1000,
        history_store=store,
    )
    # Learned 0.10 ratio -> ~100 (not the 0.60 default that would give 600).
    assert 80 <= out <= 120


# ── End-to-end ``lookup_price`` + ``cache_info`` after a refresh ───────────


def test_cache_info_after_refresh_marks_fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pricing_mod,
        "fetch_litellm_prices",
        lambda: {"a": ModelPrice(1, 1, "litellm", time.time())},
    )
    monkeypatch.setattr(pricing_mod, "fetch_openrouter_prices", lambda: {})
    refresh_prices(force=True)
    info = cache_info()
    assert info["is_stale"] is False
    assert info["litellm_count"] >= 1
    assert info["age_seconds"] is not None
    assert info["age_seconds"] >= 0
