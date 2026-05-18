// React Query hooks for the Documentation Pages feature.
// Wraps the /api/pages endpoints so route components can read/write
// without re-implementing query keys or invalidation rules.

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { apiFetch } from "../lib/api";

export type PageStatus = "draft" | "published" | "deleted";

export interface PageAssetRef {
  kind: string;
  ref: string;
  included?: boolean;
}

export interface PageSource {
  kind: "upload" | "email" | "excel";
  source_path: string;
  original_name: string;
}

export interface PageVersion {
  page_id: string;
  version_no: number;
  markdown_body: string;
  saved_at: string;
  saved_by: string | null;
  note: string | null;
}

export interface Page {
  id: string;
  title: string;
  slug: string;
  status: PageStatus;
  markdown_body: string;
  rendered_html: string | null;
  created_at: string;
  updated_at: string;
  created_by: string | null;
  generation_prompt: string | null;
  model_used: string | null;
}

export interface PageDetail extends Page {
  assets: PageAssetRef[];
  sources: PageSource[];
  versions: PageVersion[];
}

interface CreatePagePayload {
  title: string;
  intent: string;
  assets: PageAssetRef[];
}

interface SavePagePayload {
  title?: string;
  markdown_body?: string;
  status?: PageStatus;
  note?: string;
}

const pagesKey = ["pages", "list"] as const;
const pageKey = (id: string) => ["pages", "detail", id] as const;

export function usePagesList() {
  return useQuery({
    queryKey: pagesKey,
    queryFn: () => apiFetch<Page[]>("/api/pages"),
  });
}

export function usePage(id: string | undefined) {
  return useQuery({
    queryKey: pageKey(id ?? ""),
    queryFn: () => apiFetch<PageDetail>(`/api/pages/${encodeURIComponent(id ?? "")}`),
    enabled: !!id,
  });
}

export function useCreatePage() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: CreatePagePayload) =>
      apiFetch<{ id: string }>("/api/pages", {
        method: "POST",
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: pagesKey });
    },
  });
}

export function useGeneratePage(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () =>
      apiFetch<PageDetail>(`/api/pages/${encodeURIComponent(id)}/generate`, {
        method: "POST",
      }),
    onSuccess: (updated) => {
      qc.setQueryData(pageKey(id), updated);
      qc.invalidateQueries({ queryKey: pagesKey });
    },
  });
}

export function useSavePage(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: SavePagePayload) =>
      apiFetch<PageDetail>(`/api/pages/${encodeURIComponent(id)}`, {
        method: "PATCH",
        body: JSON.stringify(body),
      }),
    onSuccess: (updated) => {
      qc.setQueryData(pageKey(id), updated);
      qc.invalidateQueries({ queryKey: pagesKey });
    },
  });
}

export function useDeletePage(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () =>
      apiFetch<void>(`/api/pages/${encodeURIComponent(id)}`, {
        method: "DELETE",
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: pagesKey });
      qc.removeQueries({ queryKey: pageKey(id) });
    },
  });
}

export function useUploadPageSource(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (file: File) => {
      const form = new FormData();
      form.append("file", file);
      const res = await fetch(
        `/api/pages/${encodeURIComponent(id)}/sources`,
        { method: "POST", body: form },
      );
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || res.statusText);
      }
      return (await res.json()) as PageSource;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: pageKey(id) });
    },
  });
}
