/**
 * useCurrentUserRole — resolves the current user's role in the shared
 * team workspace via GET /api/admin/me.
 *
 * Returns "viewer" when the shared store is not configured, unavailable,
 * or the request fails — the admin panel should never crash the UI.
 */

import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "../lib/api";

interface MeResponse {
  username: string;
  hostname: string;
  role: string;
}

export function useCurrentUserRole(): {
  role: string;
  username: string;
  hostname: string;
  isLoading: boolean;
} {
  const query = useQuery<MeResponse>({
    queryKey: ["admin-me"],
    queryFn: () => apiFetch<MeResponse>("/api/admin/me"),
    staleTime: 120_000,
    retry: false,
    // Silence errors — store may not be initialised yet
    meta: { silentError: true },
  });

  return {
    role: query.data?.role ?? "viewer",
    username: query.data?.username ?? "",
    hostname: query.data?.hostname ?? "",
    isLoading: query.isLoading,
  };
}
