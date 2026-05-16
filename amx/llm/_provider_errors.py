"""Error classification for LLM provider calls.

Extracted from :mod:`amx.llm.provider` so the exception types, the
pattern tables, and the three classifier functions live in one
focused module independent from the rest of ``LLMProvider``'s call
machinery. ``provider.py`` re-exports the public names so historical
imports (``from amx.llm.provider import FatalLLMError``) continue to
work unchanged.

The three classifiers are pure:

* :func:`_classify_fatal_llm_error` — HTTP status + message inspection
  that maps non-retryable provider errors to a user-friendly
  :class:`FatalLLMError`.
* :func:`_is_logprobs_unsupported_error` — detects providers that
  reject the ``logprobs=True`` request flag.
* :func:`_is_transient_llm_error` — picks the rate-limit / timeout /
  upstream 5xx errors worth retrying once.
"""

from __future__ import annotations


class LLMTruncationError(RuntimeError):
    """Raised when model output is truncated by max_tokens."""


class FatalLLMError(RuntimeError):
    """Raised for non-recoverable LLM errors that should abort the entire run.

    Examples: out-of-credits (HTTP 402), invalid API key (401), permission
    denied (403), model not found (404). Retrying these just wastes time and
    money — every queued batch fails the same way. Catching this at the
    orchestrator / analyze_flow level lets us show the user one clear,
    actionable message and exit instead of generating 200+ identical
    warnings while iterating through every table.

    The ``user_message`` should be short, specific, and tell the user what
    to do (e.g. "Your OpenRouter account is out of credits. Visit
    https://openrouter.ai/settings/credits to top up.").
    """

    def __init__(self, user_message: str, *, original_message: str = "") -> None:
        super().__init__(user_message)
        self.user_message = user_message
        self.original_message = original_message or user_message


_FATAL_HTTP_STATUS_CODES: frozenset[int] = frozenset({401, 402, 403, 404})


_FATAL_MESSAGE_PATTERNS: tuple[tuple[str, str], ...] = (
    # Pattern → user-facing summary. Provider names are intentionally stripped
    # so the same patterns work across OpenAI, OpenRouter, Anthropic, etc.
    ("more credits", "Your account is out of credits — top up to continue."),
    (
        "insufficient_quota",
        "Your account has hit its quota — increase the limit or wait for the reset.",
    ),
    (
        "insufficient quota",
        "Your account has hit its quota — increase the limit or wait for the reset.",
    ),
    ("requires more credits", "Your account is out of credits — top up to continue."),
    ("can only afford", "Your account is out of credits — top up to continue."),
    (
        "invalid api key",
        "The API key configured for this LLM profile is invalid. Run /llm to fix it.",
    ),
    (
        "invalid_api_key",
        "The API key configured for this LLM profile is invalid. Run /llm to fix it.",
    ),
    (
        "incorrect api key",
        "The API key configured for this LLM profile is invalid. Run /llm to fix it.",
    ),
    ("authentication", "LLM authentication failed — check the API key under /llm."),
    (
        "model not found",
        "The configured model does not exist for this provider. Run /llm to pick another.",
    ),
    (
        "model_not_found",
        "The configured model does not exist for this provider. Run /llm to pick another.",
    ),
    (
        "does not exist",
        "The configured model does not exist for this provider. Run /llm to pick another.",
    ),
)


_LOGPROBS_UNSUPPORTED_PATTERNS: tuple[str, ...] = (
    "logprobs is not enabled for this model",  # Gemini Flash exact text
    "logprobs is not supported",
    "logprobs not supported",
    "logprobs are not supported",
    "logprob is not supported",
    "does not support logprobs",
    "logprobs parameter is not supported",
    # Databricks Foundation Model Serving's OpenAI shim 400s with this
    # exact phrase on its Anthropic-backed Claude endpoints. We disable
    # logprobs pre-emptively for ``databricks_serving`` (see chat()
    # below), but the runtime fallback covers anyone calling LiteLLM
    # directly with the same provider shape.
    "top_logprobs: extra inputs are not permitted",
    "logprobs: extra inputs are not permitted",
)


_TRANSIENT_LLM_EXCEPTION_NAMES: frozenset[str] = frozenset(
    {
        "APITimeoutError",
        "APIConnectionError",
        "InternalServerError",
        "ServiceUnavailableError",
        "RateLimitError",
        "Timeout",
        "ReadTimeout",
        "ConnectTimeout",
        "ConnectionError",
        "ConnectionResetError",
    }
)


_TRANSIENT_LLM_MESSAGE_TOKENS: tuple[str, ...] = (
    "rate limit",
    "rate_limit",
    "429",
    "timed out",
    "timeout",
    "connection reset",
    "connection aborted",
    "broken pipe",
    "service unavailable",
    "502 bad gateway",
    "503 service",
    "504 gateway",
    "temporary failure",
)


def _is_logprobs_unsupported_error(exc: BaseException) -> bool:
    """True when the exception indicates the model rejected ``logprobs=True``.

    Detection is message-based because providers wrap the rejection
    inside provider-specific exception types (``GeminiException``,
    ``BadRequestError``, etc.) — the only reliable signal is the
    inner JSON body.
    """
    msg = str(exc).lower()
    return any(pat in msg for pat in _LOGPROBS_UNSUPPORTED_PATTERNS)


def _classify_fatal_llm_error(exc: BaseException) -> FatalLLMError | None:
    """Return ``FatalLLMError`` when ``exc`` is non-retryable, else None.

    Detection strategy: inspect the exception's ``status_code`` (LiteLLM /
    httpx attach this) AND the lowercased error message. We need both
    because some providers wrap responses with status_code=200 and put the
    real error in the body.
    """
    msg = str(exc)
    msg_lower = msg.lower()

    # Corporate SSL inspection / self-signed CA bundles cause every retry to
    # fail the same way — short-circuit so the user gets the fix-it message
    # immediately instead of three repeated "certificate verify failed"
    # warnings followed by a generic stack trace.
    if (
        "certificate verify failed" in msg_lower
        or "self-signed certificate" in msg_lower
        or "ssl: certificate" in msg_lower
    ):
        return FatalLLMError(
            "SSL certificate verification failed — your network is using a "
            "TLS-inspecting proxy whose root CA Python doesn't trust. "
            "Fix: set AMX_CA_BUNDLE=/path/to/corp_root.pem (preferred), or "
            "AMX_INSECURE_SSL=1 for diagnostics only.",
            original_message=msg,
        )

    status: int | None = None
    for attr in ("status_code", "code"):
        candidate = getattr(exc, attr, None)
        if isinstance(candidate, int):
            status = candidate
            break
        if isinstance(candidate, str) and candidate.isdigit():
            status = int(candidate)
            break
    # Many LiteLLM error strings include the HTTP code inline ("APIError: 402").
    if status is None:
        for code in _FATAL_HTTP_STATUS_CODES:
            if f"{code}" in msg and (
                f" {code} " in msg
                or f'":{code}' in msg
                or f'"code":{code}' in msg
                or f'"code": {code}' in msg
                or f"({code})" in msg
            ):
                status = code
                break
    for pattern, user_msg in _FATAL_MESSAGE_PATTERNS:
        if pattern in msg_lower:
            return FatalLLMError(user_msg, original_message=msg)
    if status in _FATAL_HTTP_STATUS_CODES:
        # Last-resort generic message when the body didn't match a pattern.
        if status == 402:
            return FatalLLMError(
                "Your LLM provider returned 402 Payment Required — usually out of credits.",
                original_message=msg,
            )
        if status == 401:
            return FatalLLMError(
                "LLM authentication failed (HTTP 401). Re-check the API key under /llm.",
                original_message=msg,
            )
        if status == 403:
            return FatalLLMError(
                "LLM access denied (HTTP 403). Your key may lack permission for this model.",
                original_message=msg,
            )
        if status == 404:
            return FatalLLMError(
                "LLM returned 404 — the configured model name is unknown to the provider.",
                original_message=msg,
            )
    return None


def _is_transient_llm_error(exc: BaseException) -> bool:
    """Return True for LLM errors worth retrying once with backoff.

    Covers rate-limit (HTTP 429), timeouts, connection-reset, and the common
    upstream 5xx classes. Authentication / bad-request errors are NOT
    transient and propagate to the caller after one attempt so the user
    sees a themed connector-categorised message instead of a 30-second
    silent retry storm.
    """
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return True
    cls_name = exc.__class__.__name__
    if cls_name in _TRANSIENT_LLM_EXCEPTION_NAMES:
        return True
    msg = str(exc).lower()
    return any(token in msg for token in _TRANSIENT_LLM_MESSAGE_TOKENS)
