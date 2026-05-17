/**
 * Lineage detail route — renders the canvas for one anchor.
 *
 * URL shape: ``/lineage/:profile/:anchor`` where ``:anchor`` is the
 * URL-encoded artifact slug (matches the row links in Lineage.tsx).
 * We resolve the slug → artifact → anchor path via the artifact list
 * so the deep-link from a fresh visit works without extra round-trips
 * once the SPA already loaded the list.
 *
 * Three top-row actions:
 *   • Refresh   — POSTs /refresh (cache-only)
 *   • Force fresh — POSTs /refresh?no_cache=true (DB hit)
 *   • AI suggest  — POSTs /suggest, persists, refetches
 */

import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { ArrowLeft, RefreshCcw, Sparkles, Zap } from "lucide-react";

import {
  lineageFetch,
  lineageList,
  lineageRefresh,
  lineageSuggest,
  type LineageArtifact,
  type LineageEdge,
} from "../lib/api";
import { LineageCanvas } from "../components/LineageCanvas";
import { EdgePanel } from "../components/EdgePanel";
import { Badge, Button } from "../components/ui";

export default function LineageDetail() {
  const { profile = "", anchor = "" } = useParams<{ profile: string; anchor: string }>();
  const profileName = decodeURIComponent(profile);
  const slug = decodeURIComponent(anchor);
  const qc = useQueryClient();
  const [selectedEdge, setSelectedEdge] = useState<LineageEdge | null>(null);

  // Resolve the artifact slug to a concrete anchor path. List endpoint
  // is cheap (small SQLite query); UI shows a "Loading anchor" spinner
  // while it lands.
  const artifacts = useQuery({
    queryKey: ["lineage-artifacts", profileName],
    queryFn: () => lineageList(profileName),
  });
  const artifact: LineageArtifact | undefined = useMemo(
    () => artifacts.data?.artifacts.find((a) => a.name === slug),
    [artifacts.data, slug],
  );
  const anchorPath = useMemo(() => buildAnchorPath(artifact), [artifact]);

  const lineage = useQuery({
    queryKey: ["lineage-payload", profileName, anchorPath],
    queryFn: () => lineageFetch(anchorPath, { profile: profileName }),
    enabled: !!anchorPath,
  });

  const refresh = useMutation({
    mutationFn: (noCache: boolean) =>
      lineageRefresh(anchorPath, { profile: profileName, noCache }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["lineage-payload", profileName, anchorPath] });
      qc.invalidateQueries({ queryKey: ["lineage-artifacts", profileName] });
    },
  });

  const suggest = useMutation({
    mutationFn: () => lineageSuggest(anchorPath, { profile: profileName }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["lineage-payload", profileName, anchorPath] });
    },
  });

  if (artifacts.isLoading) {
    return <div className="p-6 text-sm text-fg-muted">Loading lineage artifact…</div>;
  }
  if (!artifact) {
    return (
      <div className="flex flex-col gap-3 p-6 text-sm">
        <p>Artifact <code className="font-mono">{slug}</code> not found.</p>
        <Link className="text-accent-default" to="/lineage">
          ← Back to lineage list
        </Link>
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col gap-3">
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-2 text-sm">
          <Link
            to="/lineage"
            className="inline-flex items-center gap-1 text-fg-muted hover:text-fg-default"
          >
            <ArrowLeft className="h-4 w-4" /> Lineage
          </Link>
          <span className="text-fg-muted">/</span>
          <span className="font-mono text-xs">{anchorPath}</span>
          <Badge tone="neutral" className="ml-2">
            {profileName}
          </Badge>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="secondary"
            size="sm"
            onClick={() => refresh.mutate(false)}
            disabled={refresh.isPending}
          >
            <RefreshCcw className="h-4 w-4" />
            Refresh
          </Button>
          <Button
            variant="secondary"
            size="sm"
            onClick={() => refresh.mutate(true)}
            disabled={refresh.isPending}
          >
            <Zap className="h-4 w-4" />
            Force fresh
          </Button>
          <Button
            variant="primary"
            size="sm"
            onClick={() => {
              if (
                window.confirm(
                  "Run a single LLM call to suggest lineage edges for this anchor? Spends tokens on your active LLM profile.",
                )
              ) {
                suggest.mutate();
              }
            }}
            disabled={suggest.isPending}
          >
            <Sparkles className="h-4 w-4" />
            AI suggest
          </Button>
        </div>
      </div>

      {lineage.data?.partial && (
        <div className="rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-800">
          Partial render — some extractors had cache misses. Click <strong>Force fresh</strong> to
          refill the view-DDL cache.
        </div>
      )}

      <div className="grid h-[calc(100vh-220px)] grid-cols-[minmax(0,1fr)_320px] gap-3">
        <div className="overflow-hidden rounded-xl border border-surface-border bg-surface-raised">
          {lineage.isLoading && (
            <div className="flex h-full items-center justify-center text-sm text-fg-muted">
              Loading lineage payload…
            </div>
          )}
          {lineage.data && (
            <LineageCanvas payload={lineage.data} onSelectEdge={setSelectedEdge} />
          )}
        </div>
        <EdgePanel edge={selectedEdge} />
      </div>
    </div>
  );
}

function buildAnchorPath(artifact: LineageArtifact | undefined): string {
  // The artifact rows in /api/lineage carry just the entity_id, not
  // the FQN — the slug is human-readable but not directly usable as
  // an API path. We piggy-back on the slug shape that
  // `_default_slug` in amx/cli_support/commands/lineage.py uses:
  // ``schema-table[-column]`` with dashes for non-alphanumerics. We
  // convert dashes back to dots so the FastAPI route sees the
  // canonical "schema.table" path.
  if (!artifact) return "";
  // Slug → dots heuristic (good-enough for v1; S4 could persist the
  // canonical path on the artifact row to avoid this round-trip).
  return artifact.name.replace(/-/g, ".");
}
