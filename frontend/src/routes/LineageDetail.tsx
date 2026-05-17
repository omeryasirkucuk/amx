/**
 * Lineage detail route — renders the canvas for one anchor.
 *
 * URL shape: ``/lineage/:profile/:anchor`` where ``:anchor`` is the
 * URL-encoded artifact slug (matches the row links in Lineage.tsx).
 * We resolve the slug → artifact → anchor path via the artifact list
 * so the deep-link from a fresh visit works without extra round-trips
 * once the SPA already loaded the list.
 *
 * v3 additions:
 * - Multi-tab support via URL hash `#tabs=schema.table_a,schema.table_b`.
 *   The active anchor is the path param; the hash holds the background
 *   tabs. A small tab bar above the canvas lets users swap + close.
 * - LineageSearchInput pinned top-left of the canvas; ⌘K opens it.
 * - Chain highlight is baked into the canvas (click any node → its
 *   upstream + downstream highlight, rest fades).
 *
 * Top-row actions stay: Refresh, Force fresh, AI suggest.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useLocation, useNavigate, useParams } from "react-router-dom";
import { ArrowLeft, Eye, EyeOff, RefreshCcw, Share2, Sparkles, Zap } from "lucide-react";

import { encodeLineageShare } from "../lib/lineageShare";

import {
  lineageCreateEdge,
  lineageDeleteEdge,
  lineageFetch,
  lineageList,
  lineageRefresh,
  lineageSetVerdict,
  lineageSuggest,
  type LineageArtifact,
  type LineageEdge,
} from "../lib/api";
import {
  LineageCanvas,
  type EdgeAction,
  type LineageCanvasHandle,
} from "../components/LineageCanvas";
import LineageSearchInput from "../components/LineageSearchInput";
import LineageTabBar from "../components/LineageTabBar";
import { EdgePanel } from "../components/EdgePanel";
import { LineageTracePanel } from "../components/lineage/LineageTracePanel";
import { Badge, Button, useToast } from "../components/ui";

export default function LineageDetail() {
  const { profile = "", anchor = "" } = useParams<{ profile: string; anchor: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const profileName = decodeURIComponent(profile);
  const slug = decodeURIComponent(anchor);
  const qc = useQueryClient();
  const canvasRef = useRef<LineageCanvasHandle | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<LineageEdge | null>(null);
  const [searchSignal, setSearchSignal] = useState(0);
  const [tracedColumn, setTracedColumn] = useState<{
    nodeId: string;
    column: string;
  } | null>(null);

  const tabs = useMemo(() => parseTabHash(location.hash), [location.hash]);
  const allTabs = useMemo(() => {
    const list = tabs.slice();
    if (!list.includes(slug)) list.unshift(slug);
    return list;
  }, [tabs, slug]);

  // ⌘K / Ctrl-K opens the canvas search input. Registered locally
  // (not via the global CommandPalette) so the shortcut is scoped to
  // this route — avoids stealing ⌘K when the user is in /ask or /runs.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && (e.key === "k" || e.key === "K")) {
        e.preventDefault();
        setSearchSignal((n) => n + 1);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

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
  // The artifact list is enriched with the catalog row's database so
  // the canvas can pass it on every subsequent call — without this
  // the backend can't find the anchor on profiles whose catalog
  // entries carry an explicit database (postgres with multi-db,
  // bigquery, etc.).
  const anchorDatabase = artifact?.anchor_database ?? "";

  const lineage = useQuery({
    queryKey: ["lineage-payload", profileName, anchorDatabase, anchorPath],
    queryFn: () =>
      lineageFetch(anchorPath, { profile: profileName, database: anchorDatabase }),
    enabled: !!anchorPath,
  });

  const refresh = useMutation({
    mutationFn: (noCache: boolean) =>
      lineageRefresh(anchorPath, {
        profile: profileName,
        database: anchorDatabase,
        noCache,
      }),
    onSuccess: () => {
      qc.invalidateQueries({
        queryKey: ["lineage-payload", profileName, anchorDatabase, anchorPath],
      });
      qc.invalidateQueries({ queryKey: ["lineage-artifacts", profileName] });
    },
  });

  const suggest = useMutation({
    mutationFn: () =>
      lineageSuggest(anchorPath, { profile: profileName, database: anchorDatabase }),
    onSuccess: () => {
      qc.invalidateQueries({
        queryKey: ["lineage-payload", profileName, anchorDatabase, anchorPath],
      });
    },
  });

  const toast = useToast();

  const invalidateCanvas = () => {
    qc.invalidateQueries({
      queryKey: ["lineage-payload", profileName, anchorDatabase, anchorPath],
    });
  };

  const createEdge = useMutation({
    mutationFn: ({
      source,
      target,
      sourceColumn,
      targetColumn,
    }: {
      source: string;
      target: string;
      sourceColumn: string | null;
      targetColumn: string | null;
    }) =>
      lineageCreateEdge({
        profile: profileName,
        source_fqn: source,
        target_fqn: target,
        source_column: sourceColumn,
        target_column: targetColumn,
      }),
    onSuccess: invalidateCanvas,
    onError: (e: Error) => {
      toast.push({
        title: "Could not save edge",
        description: e.message,
        tone: "error",
      });
    },
  });

  const verdictMut = useMutation({
    mutationFn: ({ id, verdict }: { id: number; verdict: "approved" | "rejected" }) =>
      lineageSetVerdict(id, verdict),
    onSuccess: invalidateCanvas,
    onError: (e: Error) => {
      toast.push({ title: "Verdict failed", description: e.message, tone: "error" });
    },
  });

  const deleteEdge = useMutation({
    mutationFn: (id: number) => lineageDeleteEdge(id),
    onSuccess: invalidateCanvas,
    onError: (e: Error) => {
      toast.push({ title: "Delete failed", description: e.message, tone: "error" });
    },
  });

  const handleEdgeAction = (edge: LineageEdge, action: EdgeAction) => {
    if (action === "delete") {
      if (edge.id == null) {
        toast.push({
          title: "Cannot delete this edge",
          description:
            "Ephemeral edges (heuristic / query log) are re-derived each refresh; mark them as rejected instead.",
          tone: "info",
        });
        return;
      }
      deleteEdge.mutate(edge.id);
      return;
    }
    if (edge.id == null) {
      toast.push({
        title: "Cannot tag this edge",
        description:
          "Ephemeral edges have no persisted row to attach a verdict to. Run /refresh first to materialise.",
        tone: "info",
      });
      return;
    }
    verdictMut.mutate({
      id: edge.id,
      verdict: action === "approve" ? "approved" : "rejected",
    });
  };

  const handleCreateEdge = (conn: {
    source: string;
    target: string;
    sourceColumn: string | null;
    targetColumn: string | null;
  }) => {
    // Source / target are node ids — i.e. "schema.table" or
    // "database.schema.table". sourceColumn / targetColumn come from
    // the React Flow port handles (column names on TableNode rows).
    createEdge.mutate(conn);
  };

  // v3 S5 — rejected-edge toggle. Default hides them so the canvas
  // stays clean; the toggle in the action row brings them back for
  // audit purposes.
  const [showRejected, setShowRejected] = useState(false);

  // v3 S5 — share link. Produces a /lineage/share#<encoded> URL,
  // copies to clipboard, surfaces a toast. The payload travels in
  // the URL hash so it never reaches the server.
  const copyShareLink = async () => {
    if (!lineage.data) return;
    const blob = encodeLineageShare(lineage.data);
    const url = `${window.location.origin}/lineage/share#${blob}`;
    try {
      await navigator.clipboard.writeText(url);
      toast.push({
        title: "Share link copied",
        description: "Anyone with the link can open the read-only canvas.",
        tone: "success",
      });
    } catch {
      toast.push({
        title: "Could not copy",
        description: "Browser blocked clipboard access; URL: " + url,
        tone: "error",
      });
    }
  };

  // Filter rejected edges out of the canvas payload unless the user
  // explicitly wants to see them. Pure client-side; the underlying
  // catalog row stays intact.
  const filteredPayload = useMemo(() => {
    if (!lineage.data) return lineage.data;
    if (showRejected) return lineage.data;
    const before = lineage.data.edges.length;
    const filtered = lineage.data.edges.filter((e) => e.verdict !== "rejected");
    if (filtered.length === before) return lineage.data;
    return { ...lineage.data, edges: filtered };
  }, [lineage.data, showRejected]);

  const goToTab = useCallback(
    (next: string) => {
      const others = allTabs.filter((t) => t !== next);
      const hash = others.length ? `#tabs=${others.join(",")}` : "";
      navigate(`/lineage/${encodeURIComponent(profileName)}/${encodeURIComponent(next)}${hash}`);
    },
    [allTabs, navigate, profileName],
  );

  const closeTab = useCallback(
    (target: string) => {
      const remaining = allTabs.filter((t) => t !== target);
      if (remaining.length === 0) {
        navigate("/lineage");
        return;
      }
      if (target === slug) {
        goToTab(remaining[0]);
      } else {
        const others = remaining.filter((t) => t !== slug);
        const hash = others.length ? `#tabs=${others.join(",")}` : "";
        navigate(
          `/lineage/${encodeURIComponent(profileName)}/${encodeURIComponent(slug)}${hash}`,
          { replace: true },
        );
      }
    },
    [allTabs, goToTab, navigate, profileName, slug],
  );

  if (artifacts.isLoading) {
    return <div className="p-6 text-sm text-fg-muted">Loading lineage artifact…</div>;
  }
  if (!artifact) {
    return (
      <div className="flex flex-col gap-3 p-6 text-sm">
        <p>
          Artifact <code className="font-mono">{slug}</code> not found.
        </p>
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
            variant="secondary"
            size="sm"
            onClick={() => setShowRejected((v) => !v)}
            title={showRejected ? "Hide rejected edges" : "Show rejected edges"}
          >
            {showRejected ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
            {showRejected ? "Hide rejected" : "Show rejected"}
          </Button>
          <Button
            variant="secondary"
            size="sm"
            onClick={copyShareLink}
            disabled={!lineage.data}
            title="Copy a read-only share link to the clipboard"
          >
            <Share2 className="h-4 w-4" />
            Share
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

      {allTabs.length > 1 && (
        <LineageTabBar
          tabs={allTabs}
          activeTab={slug}
          onPick={goToTab}
          onClose={closeTab}
        />
      )}

      {lineage.data?.partial && (
        <div className="rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-800">
          Partial render — some extractors had cache misses. Click <strong>Force fresh</strong> to
          refill the view-DDL cache.
        </div>
      )}

      <div
        className={
          "grid h-[calc(100vh-260px)] gap-3 " +
          (tracedColumn
            ? "grid-cols-[minmax(0,1fr)_288px_320px]"
            : "grid-cols-[minmax(0,1fr)_320px]")
        }
      >
        <div className="relative overflow-hidden rounded-xl border border-surface-border bg-surface-raised">
          {lineage.isLoading && (
            <div className="flex h-full items-center justify-center text-sm text-fg-muted">
              Loading lineage payload…
            </div>
          )}
          {filteredPayload && (
            <>
              <LineageSearchInput
                nodes={filteredPayload.nodes}
                onPick={(id) => canvasRef.current?.focusNode(id)}
                openSignal={searchSignal}
              />
              <LineageCanvas
                ref={canvasRef}
                payload={filteredPayload}
                onSelectEdge={setSelectedEdge}
                onCreateEdge={handleCreateEdge}
                onEdgeAction={handleEdgeAction}
                onColumnClick={(nodeId, column) => setTracedColumn({ nodeId, column })}
                tracedColumn={tracedColumn}
              />
            </>
          )}
        </div>
        {tracedColumn && (
          <LineageTracePanel
            profile={profileName}
            anchorPath={anchorPath}
            column={tracedColumn.column}
            onClose={() => setTracedColumn(null)}
            onStepClick={(step) => {
              canvasRef.current?.focusNode(
                step.kind === "operator"
                  ? step.fqn
                  : `${step.schema}.${step.table}`,
              );
              if (step.column && step.kind !== "operator") {
                setTracedColumn({
                  nodeId: `${step.schema}.${step.table}`,
                  column: step.column,
                });
              }
            }}
          />
        )}
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
  return artifact.name.replace(/-/g, ".");
}

function parseTabHash(hash: string): string[] {
  if (!hash) return [];
  const match = /[#&]tabs=([^&]+)/.exec(hash);
  if (!match) return [];
  return match[1]
    .split(",")
    .map((s) => decodeURIComponent(s.trim()))
    .filter(Boolean);
}
