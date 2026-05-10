"""Process-wide trust-store wiring for AMX's HTTPS calls.

Why this module exists
----------------------

Every AMX HTTPS call — the pricing fetcher in ``amx/llm/pricing.py``,
the document downloads in ``amx/docs/scanner.py``, the litellm /
openai / anthropic SDKs called from ``amx/llm/provider.py`` — has to
agree on how to verify TLS certificates. On a corporate or
school-managed laptop, the certificate chain is rewritten by a
TLS-intercepting middlebox (Zscaler, Netskope, Cloudflare WARP, an
on-prem inspection appliance). The middlebox's CA is installed in
the OS trust store so browsers, ``curl``, and Edge accept it; Python's
``ssl`` module can reach it via ``ssl.SSLContext.load_default_certs()``,
but only when no explicit ``cafile=`` is forced — and the underlying
OpenSSL resolution does fall over in edge cases (partial chains,
unusual Linux store layouts, certain WSL configs).

This module solves both halves by:

1. **Injecting ``truststore`` once per process.** ``truststore`` is a
   small, PyPA-endorsed package (used by ``pip`` 24.2+) that replaces
   the underlying CA resolution with first-class OS APIs
   (``enum_certificates`` on Windows, ``SecTrustEvaluateWithError`` on
   macOS, the system ``ca-certificates`` packages on Linux). After
   ``truststore.inject_into_ssl()``, every later
   ``ssl.create_default_context()`` — used by ``urllib`` AND
   ``requests`` via urllib3 AND ``httpx`` — is routed through the OS
   store. The injection is best-effort: ``ImportError`` /
   ``NotImplementedError`` / ``OSError`` are swallowed so an
   unsupported interpreter cannot crash the CLI.

2. **Fanning ``AMX_CA_BUNDLE`` out to the env vars third-party HTTP
   clients read.** Users who already set ``AMX_CA_BUNDLE`` for the
   LLM provider get the same override for ``requests``,
   ``urllib3``, ``httpx``, and curl-via-subprocess without having to
   set four env vars themselves.

This helper is idempotent — every call after the first is a no-op,
so it can safely run at every CLI entrypoint and every Studio request
worker.
"""

from __future__ import annotations

import os
import threading

_INJECTED_LOCK = threading.Lock()
_INJECTED = False


def configure_trust_store() -> None:
    """Wire AMX's HTTPS calls through the OS trust store.

    Safe to call multiple times — the actual ``truststore`` inject
    runs once and subsequent calls are O(1).
    """
    global _INJECTED
    if _INJECTED:
        # Still fan out AMX_CA_BUNDLE on every call: the env var can
        # be set late (e.g. by ``set AMX_CA_BUNDLE=...`` between two
        # /studio refreshes in the same process).
        _fan_out_ca_bundle_env()
        return

    with _INJECTED_LOCK:
        if _INJECTED:
            _fan_out_ca_bundle_env()
            return

        try:
            import truststore  # type: ignore[import-not-found]

            truststore.inject_into_ssl()
        except (ImportError, NotImplementedError, OSError):
            # Leaving the default ``ssl`` resolution in place is a
            # fully working fallback — Python 3.10+ already pulls the
            # OS trust store via ``load_default_certs()`` whenever
            # ``ssl.create_default_context()`` is called without a
            # ``cafile=`` argument. The injection just widens the
            # success surface for edge-case Linux / WSL layouts.
            pass

        _fan_out_ca_bundle_env()
        _INJECTED = True


def _fan_out_ca_bundle_env() -> None:
    """Mirror ``AMX_CA_BUNDLE`` into the third-party-recognised env vars.

    ``requests`` reads ``REQUESTS_CA_BUNDLE``; ``urllib3`` /
    ``httpx`` / Python's ``ssl`` module read ``SSL_CERT_FILE``; ``curl``
    invoked from a subprocess reads ``CURL_CA_BUNDLE``. Setting them
    here is a strict no-op when ``AMX_CA_BUNDLE`` is unset, and
    ``setdefault`` semantics mean a user who already pinned one of
    these vars explicitly keeps their value.
    """
    ca_bundle = os.environ.get("AMX_CA_BUNDLE", "").strip()
    if not ca_bundle or not os.path.exists(ca_bundle):
        return
    for var in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE", "CURL_CA_BUNDLE"):
        os.environ.setdefault(var, ca_bundle)
