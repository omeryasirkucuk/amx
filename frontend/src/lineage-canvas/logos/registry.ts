/**
 * Logo registry — TanStack hook + REST mutations + key/id lookup.
 *
 * The list endpoint is cached under the dedicated key
 * ``["lineage-canvas-logos"]`` so it never collides with any other
 * Studio query.
 */

import { useMemo } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { apiFetch } from "../../lib/api";

export type LogoCategory =
  | "cloud"
  | "warehouse"
  | "bi"
  | "tooling"
  | "custom";

export type LogoSource = "default" | "custom";

export interface LogoRow {
  id: number;
  key: string;
  label: string;
  category: LogoCategory | string;
  source: LogoSource;
  data_url: string;
  url: string;
  created_at: number;
}

export interface LogosResponse {
  logos: LogoRow[];
}

const LOGOS_KEY = ["lineage-canvas-logos"] as const;

export function useLogosQuery() {
  return useQuery({
    queryKey: LOGOS_KEY,
    queryFn: () => apiFetch<LogosResponse>("/api/lineage/logos"),
    staleTime: 60_000,
  });
}

export function useAddLogoMutation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (input: {
      key: string;
      label: string;
      category?: LogoCategory | string;
      data_url?: string;
      url?: string;
    }) =>
      apiFetch<LogoRow>("/api/lineage/logos", {
        method: "POST",
        body: JSON.stringify(input),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: LOGOS_KEY });
    },
  });
}

export function useDeleteLogoMutation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (logoId: number) => {
      const res = await fetch(`/api/lineage/logos/${logoId}`, { method: "DELETE" });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `delete failed (${res.status})`);
      }
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: LOGOS_KEY });
    },
  });
}

/** Resolve a logo row by key OR id. Returns undefined when not present. */
export function resolveLogo(
  logos: LogoRow[] | undefined,
  keyOrId: string | number | undefined | null,
): LogoRow | undefined {
  if (!logos || keyOrId === undefined || keyOrId === null || keyOrId === "") {
    return undefined;
  }
  if (typeof keyOrId === "number") {
    return logos.find((l) => l.id === keyOrId);
  }
  // Custom rows shadow defaults under the same key.
  const candidates = logos.filter((l) => l.key === keyOrId);
  return (
    candidates.find((l) => l.source === "custom") ??
    candidates.find((l) => l.source === "default") ??
    candidates[0]
  );
}

/** Memoised helper used by node components: turns the registry into a key→row map. */
export function useLogoIndex(): Map<string, LogoRow> {
  const q = useLogosQuery();
  return useMemo(() => {
    const out = new Map<string, LogoRow>();
    for (const l of q.data?.logos ?? []) {
      const existing = out.get(l.key);
      // Custom rows shadow defaults.
      if (!existing || (l.source === "custom" && existing.source !== "custom")) {
        out.set(l.key, l);
      }
    }
    return out;
  }, [q.data]);
}

/** Used by the persistence layer to read a logo's data URL on demand. */
export function pickLogoSrc(row: LogoRow | undefined): string {
  if (!row) return "";
  return row.data_url || row.url || "";
}
