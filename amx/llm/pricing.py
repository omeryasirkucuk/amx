"""Live LLM cost tracking.

Resolve a per-million-token price for any (provider, model) pair so AMX
can render real USD cost alongside its existing token totals.

Resolution order:

    1. **User override** — ``LLMConfig.custom_input_cost_per_mtok`` /
       ``custom_output_cost_per_mtok``. Both must be set; a half-override
       is treated as no override (avoids the "set output, forgot input"
       footgun where output rate * every token would overstate cost).
    2. **LiteLLM hosted JSON** —
       ``raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json``.
       Single source of truth for ~300+ models, updated frequently by
       Berri AI; covers OpenAI, Anthropic, Gemini, OpenRouter routes,
       Bedrock, etc. Public, no auth.
    3. **OpenRouter ``/v1/models``** — only consulted for ``openrouter/*``
       routes the LiteLLM JSON misses (newly-launched routes typically
       appear here days before the LiteLLM file). Public, no auth.
    4. **Bundled fallback snapshot** (``pricing_fallback.json`` next to
       this module) — last-known-good values for ~30 popular models.
       Used when both network sources fail (offline, fresh install,
       enterprise behind a strict proxy).
    5. **Zero / unknown** — ``ModelPrice(0, 0, source="unknown")`` with
       a once-per-session WARN log so the user knows cost is undercounted.

Costs are persisted into ``analysis_runs.tokens_json`` at run time
(audit trail / "what did this actually cost when prices were X"). The
``/usage --live`` flag and the Studio "Recompute with current prices"
toggle re-run :func:`compute_cost` against the fresh prices for the
same recorded token totals so users can see "with today's prices, the
same workload would cost Y".

All disk + network access is contained here. Other modules import
:func:`lookup_price` and :func:`compute_cost` only.
"""

from __future__ import annotations

import json
import os
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from importlib.resources import files as _resource_files
from pathlib import Path
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("llm.pricing")

# ── Public types ───────────────────────────────────────────────────────────


@dataclass
class ModelPrice:
    """Per-million-token rates for one model.

    ``input_per_mtok`` and ``output_per_mtok`` are in USD per 1M tokens
    (the same scale users see on provider pricing pages). ``source``
    identifies which resolution layer produced the price so the UI can
    render "OpenRouter says $0.30/$2.50" vs "User override $1.00/$5.00".
    ``fetched_at`` is the epoch when the source data was downloaded;
    ``None`` for user overrides + bundled fallback (those aren't network
    fetches).
    """

    input_per_mtok: float
    output_per_mtok: float
    source: str  # "user_override" | "litellm" | "openrouter" | "fallback" | "unknown"
    fetched_at: float | None = None

    @property
    def is_known(self) -> bool:
        return self.source != "unknown"


@dataclass
class ModelCatalogEntry:
    """One row of the cross-source pricing catalog.

    Returned by :func:`list_all_models` so Studio's price-browser dialog
    and the CLI ``/cost`` picker can render every model AMX has price
    data for, without each surface having to walk ``_PRICES`` itself.
    ``provider_hint`` is a best-effort split of the canonical key —
    display only, never used for resolution (callers still hand
    ``(provider, model)`` to :func:`lookup_price`).
    """

    model_id: str
    provider_hint: str
    input_per_mtok: float
    output_per_mtok: float
    source: str  # "litellm" | "openrouter" | "fallback"
    fetched_at: float | None = None


# ── Cache + fetch internals ────────────────────────────────────────────────

_CACHE_TTL_SEC: float = 24 * 60 * 60  # 24h
_LITELLM_URL = (
    "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"
)
_OPENROUTER_URL = "https://openrouter.ai/api/v1/models"
_HTTP_TIMEOUT_SEC = 8.0

# Logged once per (provider, model) per session so a 200-table run does
# not produce 200 identical "unknown price" lines.
_UNKNOWN_PRICE_WARNED: set[str] = set()
_PRICING_LOCK = threading.Lock()


def _cache_path() -> Path:
    return Path.home() / ".amx" / "pricing-cache.json"


def _load_cache() -> dict[str, Any]:
    """Read the on-disk cache; return ``{}`` when missing or unreadable.

    Never raises — a malformed cache must not stop AMX from running; the
    next refresh writes a fresh copy.
    """
    path = _cache_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - cache is best-effort
        log.debug("pricing cache unreadable, ignoring: %s", exc)
        return {}


def _write_cache(payload: dict[str, Any]) -> None:
    path = _cache_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except OSError as exc:  # pragma: no cover - cache write is best-effort
        log.debug("could not persist pricing cache: %s", exc)


def _build_ssl_context() -> ssl.SSLContext:
    """SSL context for the price-fetch ``urlopen`` calls.

    Resolution order:

    1. ``AMX_INSECURE_SSL`` truthy — return an unverified context.
       Diagnostic-only escape hatch for hostile networks; mirrors
       :func:`amx.llm.provider._configure_ssl_environment`'s contract.
    2. ``AMX_CA_BUNDLE`` or ``SSL_CERT_FILE`` set to an existing file
       — corporate CA bundle override; the typical fix for
       Zscaler / Netskope / on-prem MITM proxies when the OS trust
       store does NOT contain the corporate CA.
    3. Otherwise — plain ``ssl.create_default_context()``. Python's
       :func:`SSLContext.load_default_certs` (called by the default
       context constructor) pulls in the OS trust store on every
       supported platform: ``enum_certificates("ROOT")`` on Windows,
       ``SecTrustCopyAnchorCertificates`` on macOS, the system
       ``ca-certificates`` packages on Linux/BSD. The startup helper
       ``amx.utils.network_trust.configure_trust_store`` additionally
       injects ``truststore`` (when available) so the OS store is
       consulted through first-class OS APIs instead of OpenSSL's
       built-in resolution, which catches edge-cases like partial
       chains and unusual Linux store layouts.

    Earlier revisions of this function forced
    ``ssl.create_default_context(cafile=certifi.where())`` whenever no
    env var was set. That branch was intended to paper over Python
    <= 3.3 on Windows (which shipped no default CA bundle), but on
    Python 3.10+ — AMX's minimum — passing ``cafile=`` actively
    *replaces* the default chain and prevents
    ``load_default_certs()`` from reading the OS trust store, so the
    corporate CA that browsers / curl already trust never reaches
    the verifier. Removing the certifi forcing is the entire fix for
    "Refresh prices returns CERTIFICATE_VERIFY_FAILED behind a
    corporate proxy where curl works fine".
    """
    insecure = os.getenv("AMX_INSECURE_SSL", "").strip().lower()
    if insecure in ("1", "true", "yes", "on"):
        return ssl._create_unverified_context()

    for env_var in ("AMX_CA_BUNDLE", "SSL_CERT_FILE"):
        candidate = os.getenv(env_var, "").strip()
        if candidate and os.path.isfile(candidate):
            return ssl.create_default_context(cafile=candidate)

    return ssl.create_default_context()


def _is_cert_verify_error(exc: BaseException) -> bool:
    """True when *exc* is (or wraps) a TLS certificate-verification failure.

    Walks ``__cause__`` and ``URLError.reason`` so the
    ``CERTIFICATE_VERIFY_FAILED`` signature is detected whether
    Python surfaced it as ``ssl.SSLCertVerificationError`` directly,
    wrapped it inside ``urllib.error.URLError``, or stacked it via
    ``__cause__`` (the chain seen on Python 3.10–3.14).
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, ssl.SSLCertVerificationError):
            return True
        reason = getattr(current, "reason", None)
        if isinstance(reason, BaseException) and id(reason) not in seen:
            current = reason
            continue
        current = current.__cause__
    return False


def _format_fetch_error(source: str, url: str, exc: BaseException) -> str:
    """Render a fetch failure for the Studio toast / CLI log.

    Specialises the message for the two failure modes that confuse
    first-time users the most:

    1. **TLS verification failure** (``ssl.SSLCertVerificationError``
       — typical behind a corporate TLS-inspecting proxy). The raw
       ``URLError: <urlopen error [SSL: CERTIFICATE_VERIFY_FAILED]
       ... self-signed certificate in certificate chain ...>`` blob
       reads like a crash; the hint names the
       ``AMX_CA_BUNDLE`` override.
    2. **HTTP 4xx Forbidden / blocked** (``urllib.error.HTTPError``
       with a 4xx status). On corporate networks ``openrouter.ai``
       is commonly classed as an LLM endpoint and blanket-blocked by
       the proxy, surfacing as ``HTTP Error 403: Forbidden``. The
       hint reassures the user that LiteLLM kept loading and the
       block is on their network, not AMX.

    Every other failure (genuine outage, JSON parse error, file
    system error) keeps the existing class-name + message format so
    real problems still surface plainly.
    """
    try:
        host = urllib.parse.urlparse(url).hostname or url
    except ValueError:
        host = url

    if _is_cert_verify_error(exc):
        return (
            f"{source}: TLS verification failed against {host} — usually a corporate proxy. "
            f"AMX consults the OS trust store automatically; if your company CA is still "
            f"missing, set AMX_CA_BUNDLE=/path/to/ca.pem and retry."
        )

    if isinstance(exc, urllib.error.HTTPError) and 400 <= exc.code < 500:
        return (
            f"{source}: {host} returned HTTP {exc.code} — likely blocked by a network policy "
            f"(corporate proxy, firewall, or rate limit). AMX kept the cached / fallback "
            f"catalog for this source; ask your network admin to allow {host} if you need "
            f"its model list."
        )

    return f"{source}: {exc.__class__.__name__}: {exc}"


def _http_get_json(url: str, *, headers: dict[str, str] | None = None) -> Any:
    """Minimal stdlib HTTP GET -> JSON. Stays out of httpx/urllib3 dep tree.

    Raises ``urllib.error.URLError`` / ``json.JSONDecodeError`` so callers
    can swallow the failure and fall back to the next resolution layer.
    """
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(
        req, timeout=_HTTP_TIMEOUT_SEC, context=_build_ssl_context()
    ) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw)


def fetch_litellm_prices() -> dict[str, ModelPrice]:
    """Download Berri AI's hosted price table.

    The JSON keys are model identifiers as LiteLLM understands them
    (e.g. ``"gpt-4o-mini"``, ``"claude-3-5-haiku"``,
    ``"openrouter/openai/gpt-4o-mini"``). Each entry has
    ``input_cost_per_token`` / ``output_cost_per_token`` in *USD per
    token*; we multiply by 1e6 to land in $/Mtok.

    Returns model_id -> ModelPrice. Models without both rates are skipped.
    """
    raw = _http_get_json(_LITELLM_URL)
    out: dict[str, ModelPrice] = {}
    now = time.time()
    for model_id, payload in (raw or {}).items():
        if not isinstance(payload, dict) or model_id == "sample_spec":
            continue
        in_per_token = payload.get("input_cost_per_token")
        out_per_token = payload.get("output_cost_per_token")
        if in_per_token is None and out_per_token is None:
            continue
        try:
            in_rate = float(in_per_token or 0.0) * 1_000_000.0
            out_rate = float(out_per_token or 0.0) * 1_000_000.0
        except (TypeError, ValueError):
            continue
        out[str(model_id).lower()] = ModelPrice(
            input_per_mtok=in_rate,
            output_per_mtok=out_rate,
            source="litellm",
            fetched_at=now,
        )
    return out


def fetch_openrouter_prices() -> dict[str, ModelPrice]:
    """Download OpenRouter's public model list.

    Each entry has ``id`` (e.g. ``"openai/gpt-4o-mini"``) and ``pricing``
    with ``prompt`` / ``completion`` strings expressed in *USD per token*
    (parsed via ``float()``). We apply the same x1e6 conversion as the
    LiteLLM source so callers get a consistent $/Mtok scale.
    """
    raw = _http_get_json(_OPENROUTER_URL)
    out: dict[str, ModelPrice] = {}
    now = time.time()
    items = raw.get("data") if isinstance(raw, dict) else raw
    if not isinstance(items, list):
        return out
    for entry in items:
        if not isinstance(entry, dict):
            continue
        model_id = str(entry.get("id") or "").lower()
        pricing = entry.get("pricing") or {}
        if not model_id or not isinstance(pricing, dict):
            continue
        try:
            in_rate = float(pricing.get("prompt", 0) or 0) * 1_000_000.0
            out_rate = float(pricing.get("completion", 0) or 0) * 1_000_000.0
        except (TypeError, ValueError):
            continue
        out[model_id] = ModelPrice(
            input_per_mtok=in_rate,
            output_per_mtok=out_rate,
            source="openrouter",
            fetched_at=now,
        )
    return out


def _load_bundled_fallback() -> dict[str, ModelPrice]:
    """Read the snapshot shipped inside the wheel as last-resort."""
    try:
        text = (
            _resource_files("amx.llm").joinpath("pricing_fallback.json").read_text(encoding="utf-8")
        )
    except Exception as exc:  # pragma: no cover - bundled file always present
        log.warning("bundled pricing fallback unreadable: %s", exc)
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    out: dict[str, ModelPrice] = {}
    for model_id, rates in (payload.get("models") or {}).items():
        try:
            out[str(model_id).lower()] = ModelPrice(
                input_per_mtok=float(rates.get("input", 0.0)),
                output_per_mtok=float(rates.get("output", 0.0)),
                source="fallback",
                fetched_at=None,
            )
        except (TypeError, ValueError, AttributeError):
            continue
    return out


# In-memory state. Module-level so a fresh ``lookup_price`` after
# a refresh sees the new prices without re-reading from disk.
_PRICES: dict[str, dict[str, ModelPrice]] = {
    "litellm": {},
    "openrouter": {},
    "fallback": {},
}
_FETCHED_AT: float | None = None
_BUNDLED_LOADED = False


def _ensure_loaded() -> None:
    """Lazily populate the in-memory price tables.

    On first call: load the on-disk cache (if any) and supplement from
    the bundled fallback for offline/fresh-install paths. We do NOT
    fetch from the network here — that would block the first LLM call
    on a slow connection. Refresh is explicit (``/refresh-prices``) or
    via the ``cache_info`` "is_stale" flag the UI surfaces.
    """
    global _FETCHED_AT, _BUNDLED_LOADED
    with _PRICING_LOCK:
        if not _BUNDLED_LOADED:
            _PRICES["fallback"] = _load_bundled_fallback()
            _BUNDLED_LOADED = True
        cache = _load_cache()
        cache_fetched = float(cache.get("fetched_at") or 0.0) or None
        if cache_fetched and not _FETCHED_AT:
            _FETCHED_AT = cache_fetched
            litellm_raw = cache.get("litellm") or {}
            for model_id, rates in litellm_raw.items():
                _PRICES["litellm"][str(model_id).lower()] = ModelPrice(
                    input_per_mtok=float(rates.get("input_per_mtok", 0.0)),
                    output_per_mtok=float(rates.get("output_per_mtok", 0.0)),
                    source="litellm",
                    fetched_at=cache_fetched,
                )
            openrouter_raw = cache.get("openrouter") or {}
            for model_id, rates in openrouter_raw.items():
                _PRICES["openrouter"][str(model_id).lower()] = ModelPrice(
                    input_per_mtok=float(rates.get("input_per_mtok", 0.0)),
                    output_per_mtok=float(rates.get("output_per_mtok", 0.0)),
                    source="openrouter",
                    fetched_at=cache_fetched,
                )


def cache_age_seconds() -> float | None:
    """How long ago was the cache fetched? ``None`` when never fetched."""
    _ensure_loaded()
    if _FETCHED_AT is None:
        return None
    return max(0.0, time.time() - _FETCHED_AT)


def cache_info() -> dict[str, Any]:
    """Snapshot for the Studio ``GET /api/pricing/cache-info`` endpoint."""
    _ensure_loaded()
    return {
        "fetched_at": _FETCHED_AT,
        "age_seconds": cache_age_seconds(),
        "ttl_seconds": _CACHE_TTL_SEC,
        "is_stale": _FETCHED_AT is None or (time.time() - _FETCHED_AT > _CACHE_TTL_SEC),
        "litellm_count": len(_PRICES.get("litellm") or {}),
        "openrouter_count": len(_PRICES.get("openrouter") or {}),
        "fallback_count": len(_PRICES.get("fallback") or {}),
    }


def refresh_prices(*, force: bool = False) -> dict[str, Any]:
    """Fetch fresh prices from both network sources + persist to cache.

    Idempotent: calling twice in quick succession both succeed and write
    the same cache. Returns
    ``{"litellm": N, "openrouter": M, "errors": [..], "skipped": bool}``
    so callers can render a "Updated 312 LiteLLM + 287 OpenRouter
    models" confirmation. With ``force=False`` and a fresh cache
    (< TTL), this is a no-op that returns the existing counts.
    """
    global _FETCHED_AT
    _ensure_loaded()
    if not force and _FETCHED_AT and (time.time() - _FETCHED_AT) < _CACHE_TTL_SEC:
        return {
            "litellm": len(_PRICES["litellm"]),
            "openrouter": len(_PRICES["openrouter"]),
            "errors": [],
            "skipped": True,
        }

    errors: list[str] = []
    try:
        new_litellm = fetch_litellm_prices()
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        new_litellm = {}
        errors.append(_format_fetch_error("litellm", _LITELLM_URL, exc))
    try:
        new_openrouter = fetch_openrouter_prices()
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        new_openrouter = {}
        errors.append(_format_fetch_error("openrouter", _OPENROUTER_URL, exc))

    with _PRICING_LOCK:
        if new_litellm:
            _PRICES["litellm"] = new_litellm
        if new_openrouter:
            _PRICES["openrouter"] = new_openrouter
        if new_litellm or new_openrouter:
            _FETCHED_AT = time.time()
            _write_cache(
                {
                    "fetched_at": _FETCHED_AT,
                    "litellm": {k: asdict(v) for k, v in _PRICES["litellm"].items()},
                    "openrouter": {k: asdict(v) for k, v in _PRICES["openrouter"].items()},
                }
            )

    return {
        "litellm": len(_PRICES["litellm"]),
        "openrouter": len(_PRICES["openrouter"]),
        "errors": errors,
        "skipped": False,
    }


# ── Lookup ─────────────────────────────────────────────────────────────────


def _normalize_model_id(provider: str, model: str) -> list[str]:
    """Generate the candidate keys to probe in each price source.

    LiteLLM uses bare model ids for OpenAI/Anthropic but
    ``openrouter/<vendor>/<model>`` for OpenRouter routes. OpenRouter
    uses ``<vendor>/<model>``. The bundled fallback is keyed by
    ``<vendor>/<model>``. We generate a richer candidate list so all
    three keying conventions hit:

    1. The raw model id as typed (``"claude-sonnet-4-20250514"``).
    2. Strip provider prefixes one segment at a time (``"openai/gpt-4o"``
       -> ``"gpt-4o"``).
    3. Strip dated / version suffixes (``"-20250514"``, ``"-v2"``,
       ``"-latest"``, etc).
    4. Re-attach a ``<provider>/<model>`` form so the fallback table —
       which uses provider-prefixed keys — gets hit when the caller
       passed only the bare model id.
    5. For OpenRouter, also try the ``openrouter/<full>`` form which
       LiteLLM uses for those routes.
    """
    name = (model or "").strip().lower()
    candidates: list[str] = []
    if not name:
        return candidates
    candidates.append(name)
    while "/" in name:
        name = name.split("/", 1)[1]
        candidates.append(name)
    full = (model or "").strip().lower()
    prov = (provider or "").strip().lower()
    base = candidates[0]
    for sep in ("-2024", "-2025", "-2026", "-v", "-latest", "-beta", "-preview"):
        if sep in base:
            head = base.split(sep, 1)[0]
            if head not in candidates:
                candidates.append(head)
    # Re-attach ``<provider>/<X>`` for every bare candidate so the
    # bundled fallback (provider-prefixed keys) and LiteLLM
    # (occasionally provider-prefixed keys) both have a chance to hit.
    if prov:
        for cand in list(candidates):
            if "/" not in cand:
                candidates.append(f"{prov}/{cand}")
    if prov == "openrouter" and "/" in full:
        candidates.append(f"openrouter/{full}")
    seen: set[str] = set()
    deduped: list[str] = []
    for cand in candidates:
        if cand and cand not in seen:
            seen.add(cand)
            deduped.append(cand)
    return deduped


def _user_override(cfg: Any, profile_name: str | None) -> ModelPrice | None:
    """Pull custom rates off the named LLMConfig profile.

    Half-override (only one of input/output set) -> ``None`` so the
    user does not accidentally bill themselves at "free input + market
    output" or vice versa.

    Accepts either an ``AMXConfig`` (looks up
    ``cfg.llm_profiles[profile_name]`` then falls back to ``cfg.llm``)
    or a bare ``LLMConfig`` (reads its custom fields directly).
    """
    if cfg is None:
        return None
    target = None
    if profile_name and getattr(cfg, "llm_profiles", None):
        target = cfg.llm_profiles.get(profile_name)
    if target is None:
        target = getattr(cfg, "llm", None)
    if target is None and hasattr(cfg, "custom_input_cost_per_mtok"):
        target = cfg
    if target is None:
        return None
    in_rate = getattr(target, "custom_input_cost_per_mtok", None)
    out_rate = getattr(target, "custom_output_cost_per_mtok", None)
    if in_rate is None or out_rate is None:
        return None
    try:
        return ModelPrice(
            input_per_mtok=float(in_rate),
            output_per_mtok=float(out_rate),
            source="user_override",
            fetched_at=None,
        )
    except (TypeError, ValueError):
        return None


def lookup_price(
    cfg: Any,
    *,
    provider: str,
    model: str,
    profile_name: str | None = None,
) -> ModelPrice:
    """Resolve a price using the documented order.

    ``cfg`` is an :class:`AMXConfig` (typed ``Any`` to avoid a cyclic
    import). Pass ``profile_name`` to target a specific LLM profile —
    the wizard / Settings hint reads this when the user has not
    activated the profile yet.
    """
    _ensure_loaded()
    override = _user_override(cfg, profile_name)
    if override is not None:
        return override
    candidates = _normalize_model_id(provider, model)
    for source_key in ("litellm", "openrouter", "fallback"):
        table = _PRICES.get(source_key) or {}
        for cand in candidates:
            hit = table.get(cand)
            if hit is not None:
                return hit
    key = f"{(provider or '').lower()}|{(model or '').lower()}"
    if key not in _UNKNOWN_PRICE_WARNED:
        _UNKNOWN_PRICE_WARNED.add(key)
        log.info(
            "No price entry for provider=%s model=%s -- cost will display as $0.00. "
            "Run /refresh-prices or set a custom override via /cost.",
            provider,
            model,
        )
    return ModelPrice(0.0, 0.0, source="unknown", fetched_at=None)


# ── Cost compute ───────────────────────────────────────────────────────────


def compute_cost(
    *,
    prompt_tokens: int,
    completion_tokens: int,
    price: ModelPrice,
) -> tuple[float, float, float]:
    """Return ``(input_usd, output_usd, total_usd)``.

    ``ModelPrice`` rates are USD per 1M tokens; we divide token counts
    by 1e6 before multiplying. Negative or non-int token counts are
    coerced to zero so a single bad usage payload does not poison the
    aggregate.
    """
    p_tok = max(0, int(prompt_tokens or 0))
    c_tok = max(0, int(completion_tokens or 0))
    in_usd = (p_tok / 1_000_000.0) * float(price.input_per_mtok or 0.0)
    out_usd = (c_tok / 1_000_000.0) * float(price.output_per_mtok or 0.0)
    return in_usd, out_usd, in_usd + out_usd


def _provider_hint_for_key(model_id: str) -> str:
    """Best-effort provider split for a canonical price key.

    LiteLLM keys come in three shapes: bare (``"gpt-4o-mini"``),
    ``"<vendor>/<model>"`` (``"openai/gpt-4o-mini"``), and
    ``"openrouter/<vendor>/<model>"`` (``"openrouter/openai/gpt-4o-mini"``).
    Bundled fallback uses ``"<vendor>/<model>"``. We surface the first
    segment as ``provider_hint`` so the UI can render a "Provider"
    column without inventing one. Bare keys keep an empty hint —
    truthful, since we genuinely don't know the provider from the key
    alone.
    """
    if "/" not in model_id:
        return ""
    head = model_id.split("/", 1)[0]
    return head if head else ""


def list_all_models() -> list[ModelCatalogEntry]:
    """Flat, deduped catalog of every model the price layer knows.

    Walks ``_PRICES`` in priority order — litellm > openrouter >
    fallback — matching :func:`lookup_price`'s resolution chain. The
    first source to claim a given key wins; later sources for the same
    key are skipped. Sorted alphabetically by ``model_id`` so the
    result is stable across processes (the in-memory dicts are dict-
    insertion-ordered, which would leak through to the UI otherwise).

    Empty in-memory cache is fine — :func:`_ensure_loaded` runs first
    and seeds the bundled fallback (~30 popular models) so a fresh /
    offline install still has a non-empty list to browse.
    """
    _ensure_loaded()
    seen: set[str] = set()
    entries: list[ModelCatalogEntry] = []
    with _PRICING_LOCK:
        for source_key in ("litellm", "openrouter", "fallback"):
            table = _PRICES.get(source_key) or {}
            for model_id, price in table.items():
                if model_id in seen:
                    continue
                seen.add(model_id)
                entries.append(
                    ModelCatalogEntry(
                        model_id=model_id,
                        provider_hint=_provider_hint_for_key(model_id),
                        input_per_mtok=price.input_per_mtok,
                        output_per_mtok=price.output_per_mtok,
                        source=price.source,
                        fetched_at=price.fetched_at,
                    )
                )
    entries.sort(key=lambda e: e.model_id)
    return entries


def reset_state_for_tests() -> None:
    """Drop in-memory state so unit tests start from a clean slate."""
    global _FETCHED_AT, _BUNDLED_LOADED
    with _PRICING_LOCK:
        for table in _PRICES.values():
            table.clear()
        _FETCHED_AT = None
        _BUNDLED_LOADED = False
        _UNKNOWN_PRICE_WARNED.clear()


__all__ = [
    "ModelCatalogEntry",
    "ModelPrice",
    "cache_age_seconds",
    "cache_info",
    "compute_cost",
    "fetch_litellm_prices",
    "fetch_openrouter_prices",
    "list_all_models",
    "lookup_price",
    "refresh_prices",
    "reset_state_for_tests",
]
