/** TanStack Query hook + client-side fallbacks for LLM capability lookup.
 *
 *  The Studio's "Advanced LLM settings" panel gates ``thinking_budget``
 *  and the ``logprob_*`` rows on whether the active (provider, model)
 *  supports those knobs. The authoritative answer lives on the
 *  backend (``amx.llm.capabilities``); this module wraps the
 *  ``GET /api/llm/capabilities`` endpoint with a TanStack Query so:
 *
 *  - capabilities are fetched once per (provider, model) and held for
 *    the whole session (``staleTime: Infinity`` / ``gcTime: Infinity``);
 *  - saving an LLM profile invalidates ``["capabilities"]`` so a
 *    re-keyed model is re-fetched on next mount;
 *  - while the first request is in flight the UI uses a client-side
 *    substring fallback so the gating never blocks rendering.
 */

import { useQuery, useQueryClient } from "@tanstack/react-query";

import { api, type LLMCapabilities } from "./api";

const CAPABILITIES_KEY = ["capabilities"] as const;

/** Client-side fallback for ``supports_thinking`` — mirrors the
 *  backend's :func:`amx.llm.provider._supports_thinking` table. */
export function supportsThinkingFallback(provider: string, model: string): boolean {
  const p = (provider || "").toLowerCase();
  const m = (model || "").toLowerCase();
  if (p === "anthropic") {
    return (
      m.includes("claude-sonnet-4") ||
      m.includes("claude-opus-4") ||
      m.includes("claude-3-7-sonnet") ||
      m.includes("claude-3.7-sonnet")
    );
  }
  if (p === "deepseek") return m.includes("reasoner");
  if (p === "openai") {
    return (
      /(^|\W)o\d/.test(m) ||
      m.includes("gpt-5") ||
      m.includes("o1") ||
      m.includes("o3") ||
      m.includes("o4")
    );
  }
  if (p === "openrouter") {
    return (
      m.includes("thinking") ||
      m.includes("reasoner") ||
      m.includes("reasoning") ||
      m.includes("claude-sonnet-4") ||
      m.includes("claude-opus-4") ||
      m.includes("claude-3-7-sonnet") ||
      m.includes("kimi-k2") ||
      m.includes("kimi-2") ||
      m.includes("/o1") ||
      m.includes("/o3") ||
      m.includes("/o4") ||
      m.includes("gpt-5")
    );
  }
  return false;
}

/** Client-side fallback for ``supports_logprobs``. */
export function supportsLogprobsFallback(provider: string, model: string): boolean {
  const p = (provider || "").toLowerCase();
  const m = (model || "").toLowerCase();
  if (p === "anthropic" || p === "databricks_serving") return false;
  if (p === "gemini") return !m.includes("flash");
  if (p === "openai") {
    return !(
      m.includes("o1") ||
      m.includes("o3") ||
      m.includes("o4") ||
      m.includes("gpt-5")
    );
  }
  if (p === "openrouter") {
    return !(
      m.includes("anthropic/") ||
      m.includes("claude-") ||
      m.includes("/flash") ||
      m.includes("-flash") ||
      m.includes("/o1") ||
      m.includes("/o3") ||
      m.includes("/o4") ||
      m.includes("gpt-5") ||
      m.includes("thinking") ||
      m.includes("reasoner") ||
      m.includes("reasoning")
    );
  }
  return true;
}

/** Fetch capabilities for one ``(provider, model)`` pair, cached for
 *  the lifetime of the session.
 *
 *  Returns ``{ data, supportsThinking, supportsLogprobs }``; the
 *  ``supports*`` helpers fall back to the client-side substring
 *  tables while the network request is in flight so the gating UI
 *  never has a flash of un-gated state. */
export function useLLMCapabilities(provider: string | null, model: string | null) {
  const enabled = Boolean(provider && model);
  const query = useQuery<LLMCapabilities>({
    queryKey: [...CAPABILITIES_KEY, provider, model],
    queryFn: () => api.llmCapabilities(provider as string, model as string),
    enabled,
    staleTime: Infinity,
    gcTime: Infinity,
  });
  const supportsThinking = query.data?.supports_thinking
    ?? (enabled ? supportsThinkingFallback(provider as string, model as string) : false);
  const supportsLogprobs = query.data?.supports_logprobs
    ?? (enabled ? supportsLogprobsFallback(provider as string, model as string) : true);
  return {
    data: query.data ?? null,
    isLoading: query.isLoading,
    supportsThinking,
    supportsLogprobs,
  };
}

/** Invalidate every cached capability lookup. Wire this into the
 *  Settings page's ``PUT /api/profiles/llm/{name}`` mutation's
 *  ``onSuccess`` so a re-keyed model is re-checked on next mount. */
export function useInvalidateLLMCapabilities() {
  const qc = useQueryClient();
  return () => qc.invalidateQueries({ queryKey: CAPABILITIES_KEY });
}
