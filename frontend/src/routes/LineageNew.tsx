/**
 * Lineage manual-draw workspace.
 *
 * Blank canvas + toolbar. Users:
 *   1. Pick a DB profile from the toolbar (auto-selected when there's
 *      only one).
 *   2. Click "Add table" to pull a node from the catalogue into the
 *      canvas. The picker is a small searchable list of cached tables
 *      for the active profile.
 *   3. Drag from a node's edge handle to another node to draw an edge.
 *   4. Right-click an edge → delete.
 *   5. Click "Save canvas" to persist as a lineage artifact (rendered
 *      via the backend's standard create_lineage path).
 *
 * State is entirely client-side until Save fires. No backend round-
 * trips for drag-to-connect during draft — keeps the canvas snappy
 * and lets the user iterate before committing.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import {
  ArrowLeft,
  CheckCircle2,
  Plus,
  Save,
  Trash2,
  X,
} from "lucide-react";
import ReactFlow, {
  Background,
  Controls,
  MarkerType,
  ReactFlowProvider,
  useReactFlow,
  type Connection,
  type Edge as RFEdge,
  type EdgeMouseHandler,
  type Node as RFNode,
} from "reactflow";
import "reactflow/dist/style.css";
import dagre from "dagre";

import { api, apiFetch } from "../lib/api";
import PageHeader from "../components/PageHeader";
import Modal from "../components/Modal";
import { Badge, Button, useToast } from "../components/ui";

interface DbProfileSummary {
  name: string;
  backend?: string;
  database?: string;
  catalog?: string;
}

interface DbProfilesResponse {
  profiles: DbProfileSummary[];
}

interface DraftNode {
  id: string; // 'schema.table'
  schema: string;
  table: string;
}

interface DraftEdge {
  id: string;
  source: string;
  target: string;
}

const NODE_W = 220;
const NODE_H = 56;
const CATALOG_BACKENDS = new Set(["databricks", "bigquery", "snowflake"]);

export default function LineageNew() {
  return (
    <ReactFlowProvider>
      <Workspace />
    </ReactFlowProvider>
  );
}

function Workspace() {
  const navigate = useNavigate();
  const toast = useToast();
  const flow = useReactFlow();
  const [profile, setProfile] = useState<string>("");
  const [database, setDatabase] = useState<string>("");
  const [catalog, setCatalog] = useState<string>("");
  const [draftNodes, setDraftNodes] = useState<DraftNode[]>([]);
  const [draftEdges, setDraftEdges] = useState<DraftEdge[]>([]);
  const [addOpen, setAddOpen] = useState(false);
  const [saveOpen, setSaveOpen] = useState(false);
  const [saveName, setSaveName] = useState("");
  const [contextEdge, setContextEdge] = useState<DraftEdge | null>(null);
  const canvasRef = useRef<HTMLDivElement | null>(null);

  // Profile picker.
  const profiles = useQuery({
    queryKey: ["db-profiles", "list"],
    queryFn: () => apiFetch<DbProfilesResponse>("/api/profiles/db"),
  });
  const profileMeta = useMemo(
    () => profiles.data?.profiles.find((p) => p.name === profile),
    [profiles.data, profile],
  );
  const supportsCatalogs = profileMeta
    ? CATALOG_BACKENDS.has(String(profileMeta.backend || "").toLowerCase())
    : false;
  useEffect(() => {
    if (!profile && profiles.data && profiles.data.profiles.length === 1) {
      setProfile(profiles.data.profiles[0].name);
    }
  }, [profile, profiles.data]);

  // Catalog / database resolution for the picker.
  const catalogs = useQuery({
    queryKey: ["live-catalogs", profile],
    queryFn: () => api.liveCatalogs({ profile }),
    enabled: !!profile && supportsCatalogs,
  });
  useEffect(() => {
    if (supportsCatalogs && !catalog) {
      const list = catalogs.data?.catalogs ?? [];
      const active =
        catalogs.data?.active_catalog ?? catalogs.data?.active_project ?? "";
      if (active && list.includes(active)) setCatalog(active);
      else if (list.length === 1) setCatalog(list[0]);
    }
  }, [supportsCatalogs, catalog, catalogs.data]);
  const databases = useQuery({
    queryKey: ["live-databases", profile],
    queryFn: () => api.liveDatabases({ profile }),
    enabled: !!profile && !supportsCatalogs,
  });
  useEffect(() => {
    if (!supportsCatalogs && !database) {
      const list = databases.data?.databases ?? [];
      const active = databases.data?.active_database ?? "";
      if (active && list.includes(active)) setDatabase(active);
      else if (list.length === 1) setDatabase(list[0]);
    }
  }, [supportsCatalogs, database, databases.data]);
  const effectiveDb = supportsCatalogs ? catalog : database;

  // Convert draft state into React Flow shape with dagre layout.
  const { rfNodes, rfEdges } = useMemo(() => {
    const g = new dagre.graphlib.Graph().setGraph({
      rankdir: "LR",
      nodesep: 40,
      ranksep: 90,
      marginx: 24,
      marginy: 24,
    });
    g.setDefaultEdgeLabel(() => ({}));
    draftNodes.forEach((n) => g.setNode(n.id, { width: NODE_W, height: NODE_H }));
    draftEdges.forEach((e) => g.setEdge(e.source, e.target));
    dagre.layout(g);
    const rfNodes: RFNode[] = draftNodes.map((n) => {
      const pos = g.node(n.id);
      return {
        id: n.id,
        type: "default",
        data: { label: `${n.schema}.${n.table}` },
        position: { x: (pos?.x ?? 0) - NODE_W / 2, y: (pos?.y ?? 0) - NODE_H / 2 },
        style: {
          borderColor: "#0f172a",
          borderWidth: 1.2,
          background: "#ffffff",
          padding: 8,
          borderRadius: 8,
          fontWeight: 500,
        },
      };
    });
    const rfEdges: RFEdge[] = draftEdges.map((e) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      type: "smoothstep",
      label: "manual",
      markerEnd: { type: MarkerType.ArrowClosed, color: "#0f172a" },
      style: { stroke: "#0f172a", strokeWidth: 1.4 },
      labelStyle: { fontSize: 10, fill: "#0f172a" },
      labelBgStyle: { fill: "#ffffffcc" },
      labelBgPadding: [4, 2] as [number, number],
      labelBgBorderRadius: 4,
    }));
    return { rfNodes, rfEdges };
  }, [draftNodes, draftEdges]);

  const onConnect = useCallback(
    (conn: Connection) => {
      if (!conn.source || !conn.target || conn.source === conn.target) return;
      setDraftEdges((prev) => {
        const id = `${conn.source}->${conn.target}`;
        if (prev.some((e) => e.id === id)) return prev;
        return [...prev, { id, source: conn.source!, target: conn.target! }];
      });
    },
    [],
  );

  const onEdgeContextMenu: EdgeMouseHandler = (e, edge) => {
    e.preventDefault();
    setContextEdge({
      id: String(edge.id),
      source: String(edge.source),
      target: String(edge.target),
    });
  };

  const removeEdge = (id: string) => {
    setDraftEdges((prev) => prev.filter((e) => e.id !== id));
    setContextEdge(null);
  };

  const removeNode = (id: string) => {
    setDraftNodes((prev) => prev.filter((n) => n.id !== id));
    setDraftEdges((prev) => prev.filter((e) => e.source !== id && e.target !== id));
  };

  const addNode = useCallback(
    (schema: string, table: string) => {
      const id = `${schema}.${table}`;
      setDraftNodes((prev) => (prev.some((n) => n.id === id) ? prev : [...prev, { id, schema, table }]));
      setAddOpen(false);
      requestAnimationFrame(() => {
        flow.fitView({ duration: 240, padding: 0.2 });
      });
    },
    [flow],
  );

  const save = useMutation({
    mutationFn: async () => {
      if (!profile) throw new Error("Pick a DB profile first.");
      if (draftNodes.length === 0) throw new Error("Add at least one node.");
      if (!saveName.trim()) throw new Error("Give the artifact a name.");
      const anchor = draftNodes[0];
      const body = {
        profile,
        name: saveName.trim(),
        anchor_fqn: effectiveDb
          ? `${effectiveDb}.${anchor.schema}.${anchor.table}`
          : `${anchor.schema}.${anchor.table}`,
        edges: draftEdges.map((e) => {
          const src = draftNodes.find((n) => n.id === e.source)!;
          const tgt = draftNodes.find((n) => n.id === e.target)!;
          const srcFqn = effectiveDb
            ? `${effectiveDb}.${src.schema}.${src.table}`
            : `${src.schema}.${src.table}`;
          const tgtFqn = effectiveDb
            ? `${effectiveDb}.${tgt.schema}.${tgt.table}`
            : `${tgt.schema}.${tgt.table}`;
          return { source_fqn: srcFqn, target_fqn: tgtFqn };
        }),
      };
      const res = await apiFetch<{ artifact_id: number; persisted_edges: number }>(
        "/api/lineage/manual",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
      );
      return { ...res, slug: saveName.trim().replace(/[^A-Za-z0-9_-]+/g, "_") };
    },
    onSuccess: (out) => {
      toast.push({
        title: "Lineage saved",
        description: `Persisted ${out.persisted_edges} edge(s). Opening the canvas…`,
        tone: "success",
      });
      setSaveOpen(false);
      navigate(`/lineage/${encodeURIComponent(profile)}/${encodeURIComponent(out.slug)}`);
    },
    onError: (e: Error) => {
      toast.push({ title: "Save failed", description: e.message, tone: "error" });
    },
  });

  return (
    <div className="flex h-full flex-col gap-3">
      <PageHeader
        title="Draw lineage"
        breadcrumbs={[
          { label: "Lineage", to: "/lineage" },
          { label: "New" },
        ]}
        description="Add tables from your catalogue, drag from one to another to create an edge, save when you're done."
        actions={
          <div className="flex items-center gap-2">
            <Button
              variant="secondary"
              size="md"
              leadingIcon={<Plus size={14} />}
              disabled={!profile}
              onClick={() => setAddOpen(true)}
            >
              Add table
            </Button>
            <Button
              variant="primary"
              size="md"
              leadingIcon={<Save size={14} />}
              disabled={draftNodes.length === 0}
              onClick={() => {
                if (!saveName) setSaveName(`manual-${Date.now()}`);
                setSaveOpen(true);
              }}
            >
              Save canvas
            </Button>
          </div>
        }
      />

      <div className="flex flex-wrap items-center gap-3 rounded-md border border-surface-border bg-surface-raised px-3 py-2 text-xs">
        <span className="text-fg-muted">Profile</span>
        <select
          className="rounded-md border border-surface-border bg-surface-raised px-2 py-1"
          value={profile}
          onChange={(e) => {
            setProfile(e.target.value);
            setDatabase("");
            setCatalog("");
            setDraftNodes([]);
            setDraftEdges([]);
          }}
        >
          <option value="">— pick profile —</option>
          {(profiles.data?.profiles ?? []).map((p) => (
            <option key={p.name} value={p.name}>
              {p.name}
            </option>
          ))}
        </select>
        {profile && supportsCatalogs && (
          <>
            <span className="text-fg-muted">Catalog</span>
            <select
              className="rounded-md border border-surface-border bg-surface-raised px-2 py-1"
              value={catalog}
              onChange={(e) => {
                setCatalog(e.target.value);
                setDraftNodes([]);
                setDraftEdges([]);
              }}
            >
              <option value="">— pick catalog —</option>
              {(catalogs.data?.catalogs ?? []).map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
          </>
        )}
        {profile && !supportsCatalogs && (
          <>
            <span className="text-fg-muted">Database</span>
            <select
              className="rounded-md border border-surface-border bg-surface-raised px-2 py-1"
              value={database}
              onChange={(e) => {
                setDatabase(e.target.value);
                setDraftNodes([]);
                setDraftEdges([]);
              }}
            >
              <option value="">— pick database —</option>
              {(databases.data?.databases ?? []).map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
          </>
        )}
        <span className="ml-auto text-fg-muted">
          {draftNodes.length} node{draftNodes.length === 1 ? "" : "s"} ·{" "}
          {draftEdges.length} edge{draftEdges.length === 1 ? "" : "s"}
        </span>
      </div>

      <div
        ref={canvasRef}
        className="relative h-[calc(100vh-280px)] overflow-hidden rounded-xl border border-surface-border bg-surface-raised"
      >
        {draftNodes.length === 0 ? (
          <div className="absolute inset-0 flex flex-col items-center justify-center gap-3 text-sm text-fg-muted">
            <p>Blank canvas — add tables to start drawing.</p>
            <Button
              variant="secondary"
              size="sm"
              leadingIcon={<Plus size={14} />}
              disabled={!profile}
              onClick={() => setAddOpen(true)}
            >
              Add table
            </Button>
          </div>
        ) : (
          <ReactFlow
            nodes={rfNodes}
            edges={rfEdges}
            onConnect={onConnect}
            onEdgeContextMenu={onEdgeContextMenu}
            onNodeContextMenu={(e, node) => {
              e.preventDefault();
              if (window.confirm(`Remove node ${node.id}?`)) {
                removeNode(String(node.id));
              }
            }}
            nodesDraggable={false}
            nodesConnectable
            elementsSelectable
            fitView
            fitViewOptions={{ padding: 0.2 }}
            proOptions={{ hideAttribution: true }}
          >
            <Background gap={20} color="#e2e8f0" />
            <Controls showInteractive={false} position="bottom-right" />
          </ReactFlow>
        )}
        {contextEdge && (
          <div className="pointer-events-auto absolute bottom-4 left-4 flex items-center gap-2 rounded-md border border-surface-border bg-surface-raised px-3 py-2 text-xs shadow">
            <span className="font-mono">{contextEdge.source} → {contextEdge.target}</span>
            <Button
              variant="secondary"
              size="sm"
              leadingIcon={<Trash2 size={12} />}
              onClick={() => removeEdge(contextEdge.id)}
            >
              Delete
            </Button>
            <button
              type="button"
              onClick={() => setContextEdge(null)}
              className="rounded p-1 text-fg-muted hover:bg-surface-muted"
              aria-label="Dismiss"
            >
              <X className="h-3 w-3" />
            </button>
          </div>
        )}
      </div>

      <div className="text-xs text-fg-muted">
        <Link to="/lineage" className="inline-flex items-center gap-1 hover:text-fg-default">
          <ArrowLeft className="h-3 w-3" /> Back to Lineage hub
        </Link>
      </div>

      <AddTableModal
        open={addOpen}
        onClose={() => setAddOpen(false)}
        profile={profile}
        database={!supportsCatalogs ? database : ""}
        catalog={supportsCatalogs ? catalog : ""}
        onPick={addNode}
        alreadyAdded={new Set(draftNodes.map((n) => n.id))}
      />

      <Modal
        open={saveOpen}
        onClose={() => setSaveOpen(false)}
        title={
          <span className="inline-flex items-center gap-2">
            <CheckCircle2 className="h-4 w-4" /> Save lineage canvas
          </span>
        }
        description="Persist this canvas as a lineage artifact. The first node becomes the anchor; all hand-drawn edges land in catalog_relationships as lineage_manual rows."
        footer={
          <div className="flex justify-end gap-2">
            <Button variant="secondary" size="md" onClick={() => setSaveOpen(false)}>
              Cancel
            </Button>
            <Button
              variant="primary"
              size="md"
              loading={save.isPending}
              disabled={!saveName.trim() || save.isPending}
              onClick={() => save.mutate()}
            >
              Save
            </Button>
          </div>
        }
      >
        <label className="block space-y-1 text-sm">
          <span className="text-xs uppercase tracking-wide text-fg-muted">Artifact name</span>
          <input
            type="text"
            value={saveName}
            onChange={(e) => setSaveName(e.target.value)}
            className="block w-full rounded-md border border-surface-border bg-surface-raised px-3 py-2 text-sm focus:border-accent-default focus:outline-none focus:ring-1 focus:ring-accent-default"
            placeholder="my-custom-flow"
          />
          {save.error && (
            <p className="mt-2 text-xs text-critical">
              {(save.error as Error).message}
            </p>
          )}
          <p className="text-xs text-fg-muted">
            Anchor: <span className="font-mono">{draftNodes[0]?.schema}.{draftNodes[0]?.table}</span>
          </p>
        </label>
      </Modal>
    </div>
  );
}

interface AddTableModalProps {
  open: boolean;
  onClose: () => void;
  profile: string;
  database: string;
  catalog: string;
  onPick: (schema: string, table: string) => void;
  alreadyAdded: Set<string>;
}

function AddTableModal({
  open,
  onClose,
  profile,
  database,
  catalog,
  onPick,
  alreadyAdded,
}: AddTableModalProps) {
  const [schema, setSchema] = useState<string>("");
  const [query, setQuery] = useState<string>("");

  useEffect(() => {
    if (open) {
      setSchema("");
      setQuery("");
    }
  }, [open]);

  const scopeKind: "catalog" | "database" = catalog ? "catalog" : "database";
  const schemas = useQuery({
    queryKey: ["live-schemas", profile, database, catalog],
    queryFn: () =>
      api.liveSchemas({
        profile,
        database,
        catalog,
        kind: scopeKind,
      }),
    enabled: open && !!profile && (!!catalog || !!database),
  });
  const schemaList = schemas.data?.schemas ?? [];

  const assets = useQuery({
    queryKey: ["live-assets", profile, database, catalog, schema],
    queryFn: () =>
      api.liveAssets(
        { profile, database, catalog, kind: scopeKind },
        schema,
      ),
    enabled: open && !!profile && !!schema,
  });
  const tableList = useMemo(
    () =>
      (assets.data?.assets ?? []).filter((a) => a.kind === "table" || a.kind === "view"),
    [assets.data],
  );

  const matches = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return tableList;
    return tableList.filter((a) => a.name.toLowerCase().includes(q));
  }, [tableList, query]);

  return (
    <Modal
      open={open}
      onClose={onClose}
      size="md"
      title={
        <span className="inline-flex items-center gap-2">
          <Plus className="h-4 w-4" /> Add table to canvas
        </span>
      }
      description="Pick a cached table; it joins the canvas as a draggable node. Drag from its border handles to draw edges."
    >
      <div className="space-y-3 text-sm">
        <label className="block space-y-1">
          <span className="text-xs uppercase tracking-wide text-fg-muted">Schema</span>
          {schemas.isLoading ? (
            <div className="rounded-md border border-surface-border bg-surface-muted px-3 py-2 text-xs text-fg-muted">
              Loading…
            </div>
          ) : schemaList.length === 0 ? (
            <div className="rounded-md border border-dashed border-surface-border bg-surface-muted px-3 py-2 text-xs text-fg-muted">
              No schemas — run Sync first.
            </div>
          ) : (
            <select
              className="block w-full rounded-md border border-surface-border bg-surface-raised px-3 py-2 text-sm"
              value={schema}
              onChange={(e) => setSchema(e.target.value)}
            >
              <option value="">— pick schema —</option>
              {schemaList.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          )}
        </label>

        {schema && (
          <label className="block space-y-1">
            <span className="text-xs uppercase tracking-wide text-fg-muted">
              Filter
            </span>
            <input
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Type to filter…"
              className="block w-full rounded-md border border-surface-border bg-surface-raised px-3 py-2 text-sm"
            />
          </label>
        )}

        {schema && (
          <div className="max-h-72 overflow-y-auto rounded-md border border-surface-border bg-surface-raised">
            {assets.isLoading ? (
              <div className="p-3 text-xs text-fg-muted">Loading tables…</div>
            ) : matches.length === 0 ? (
              <div className="p-3 text-xs text-fg-muted">No matching tables.</div>
            ) : (
              <ul className="divide-y divide-surface-border text-xs">
                {matches.map((a) => {
                  const id = `${schema}.${a.name}`;
                  const added = alreadyAdded.has(id);
                  return (
                    <li key={a.name} className="flex items-center justify-between px-3 py-2">
                      <span className="font-mono">
                        {schema}.{a.name}
                        {a.kind === "view" && (
                          <Badge tone="neutral" className="ml-2">
                            view
                          </Badge>
                        )}
                      </span>
                      <button
                        type="button"
                        disabled={added}
                        onClick={() => onPick(schema, a.name)}
                        className={
                          "rounded px-2 py-1 " +
                          (added
                            ? "text-fg-muted"
                            : "text-accent-default hover:bg-accent-default/10")
                        }
                      >
                        {added ? "added" : "+ Add"}
                      </button>
                    </li>
                  );
                })}
              </ul>
            )}
          </div>
        )}
      </div>
    </Modal>
  );
}
