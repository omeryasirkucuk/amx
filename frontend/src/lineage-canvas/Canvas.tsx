/**
 * Lineage Canvas — root component for the rebuilt /lineage experience.
 *
 * Responsibilities:
 *   - Hold the canonical ReactFlow nodes + edges
 *   - Wire toolbar actions to canvas + bridge mutations
 *   - Subscribe to the streaming AI hook and merge each batch in
 *   - Orchestrate Save / Open by artifact id (never by name slug —
 *     that was the source of the original save-canvas mis-resolve bug)
 *
 * URL surface (all served by this single component):
 *   /lineage                  blank canvas
 *   /lineage?artifact=<id>    re-open a saved canvas
 *
 * Anchor selection for AI generate flows through an inline modal so
 * the user can pick any table on the canvas as the focal node.
 */

import {
  KeyboardEvent,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  Background,
  Controls,
  ReactFlow,
  ReactFlowProvider,
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
  useReactFlow,
  type Connection,
  type EdgeChange,
  type NodeChange,
} from "reactflow";
import "reactflow/dist/style.css";
import { useSearchParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Loader2 } from "lucide-react";

import PageHeader from "../components/PageHeader";
import Modal from "../components/Modal";
import { Button, useToast } from "../components/ui";
import "reactflow/dist/style.css";

import { AddTableModal } from "./components/AddTableModal";
import { AttributeTrackerPanel } from "./components/AttributeTrackerPanel";
import { ColumnEdgeMarkerDefs, edgeTypes } from "./components/ColumnEdge";
import { EdgeLegendChip } from "./components/EdgeLegendChip";
import { SearchModal } from "./components/SearchModal";
import { Toolbar } from "./components/Toolbar";
import { buildEdgeHoverLabel } from "./edgeLabels";
import { useAutoLayout } from "./hooks/useAutoLayout";
import { usePngExport } from "./hooks/usePngExport";
import { useStreamingAI, type StreamBatch } from "./hooks/useStreamingAI";
import { nodeTypes, nodeTypeForOperator } from "./nodes/registry";
import { EDGE_COLORS, OPERATOR_COLORS } from "./constants";
import "./styles/theme.css";
import {
  buildSavePayload,
  loadCanvas,
  saveManualCanvas,
} from "./amx-bridge/persistence";
import {
  convertLoadedCanvas,
  loadedEdgeToCanvasEdge,
  makeTableNode,
} from "./amx-bridge/payload";
import { fetchTableColumns } from "./amx-bridge/catalog";
import { lineageEdgesAmong } from "../lib/api";
import { proposeNameMatchEdges } from "./heuristics/nameMatch";
import { parseSql, renderSql } from "./amx-bridge/sqlIo";
import { logoKeyForBackend } from "./logos/backendMap";
import { LogoPicker } from "./logos/LogoPicker";
import type { LogoRow } from "./logos/registry";
import type {
  AddTablePick,
  CanvasEdge,
  CanvasNode,
  OperatorKind,
  TableNodeData,
} from "./types";

export default function LineageCanvasRoute() {
  return (
    <ReactFlowProvider>
      <CanvasInner />
    </ReactFlowProvider>
  );
}

function CanvasInner() {
  const toast = useToast();
  const qc = useQueryClient();
  const [params, setParams] = useSearchParams();
  const artifactId = params.get("artifact")
    ? Number(params.get("artifact"))
    : null;

  const [nodes, setNodes] = useState<CanvasNode[]>([]);
  const [edges, setEdges] = useState<CanvasEdge[]>([]);
  const [primaryProfile, setPrimaryProfile] = useState<string>("");
  const [artifactName, setArtifactName] = useState<string>("");
  const [activeArtifactId, setActiveArtifactId] = useState<number | null>(
    artifactId,
  );
  const [generating, setGenerating] = useState(false);

  // Modals
  const [addOpen, setAddOpen] = useState(false);
  const [searchOpen, setSearchOpen] = useState(false);
  const [trackerOpen, setTrackerOpen] = useState(false);
  const [saveOpen, setSaveOpen] = useState(false);
  const [generateOpen, setGenerateOpen] = useState(false);
  const [sqlImportOpen, setSqlImportOpen] = useState(false);
  const [sqlExportOpen, setSqlExportOpen] = useState(false);
  const [logoPickerOpen, setLogoPickerOpen] = useState(false);

  const [saveName, setSaveName] = useState("");
  const [aiAnchor, setAiAnchor] = useState("");
  const [sqlInput, setSqlInput] = useState("");
  const [sqlOutput, setSqlOutput] = useState("");

  const canvasShellRef = useRef<HTMLDivElement | null>(null);
  const pageWrapperRef = useRef<HTMLDivElement | null>(null);
  const [pageWrapperHeight, setPageWrapperHeight] = useState<number | undefined>(
    undefined,
  );
  const autoLayout = useAutoLayout();
  const exportPng = usePngExport();
  // ``useReactFlow`` is only valid inside the ReactFlowProvider that
  // wraps this component (see ``LineageCanvasRoute`` below); we use it
  // for the post-layout fitView call.
  const rf = useReactFlow();

  // Size the Lineage page to exactly fit the viewport below whatever
  // chrome AppShell renders above it (TopBar, padding, sidebar resize,
  // etc.). The wrapper's top edge is measured relative to the
  // viewport, so any future chrome change is absorbed automatically
  // without re-tuning a hardcoded ``calc(100vh - …)``. The canvas
  // itself takes the remaining space via ``flex-1``.
  useLayoutEffect(() => {
    function measure() {
      const el = pageWrapperRef.current;
      if (!el) return;
      const top = el.getBoundingClientRect().top;
      // Small gutter for the AppShell main's bottom padding + Footer
      // slot — keeps the canvas border visible instead of sitting
      // flush against the viewport edge.
      const bottomGutter = 32;
      const next = Math.max(360, window.innerHeight - top - bottomGutter);
      setPageWrapperHeight(next);
    }
    measure();
    window.addEventListener("resize", measure);
    return () => window.removeEventListener("resize", measure);
  }, []);

  // ── Re-open from /lineage?artifact=<id> ─────────────────────────────────
  const loadQ = useQuery({
    queryKey: ["lineage-canvas", artifactId],
    queryFn: () => (artifactId ? loadCanvas(artifactId) : null),
    enabled: !!artifactId,
  });

  useEffect(() => {
    if (loadQ.data) {
      const conv = convertLoadedCanvas(loadQ.data);
      setNodes(conv.nodes);
      setEdges(conv.edges);
      setPrimaryProfile(conv.primaryProfile);
      setArtifactName(conv.artifactName);
      setActiveArtifactId(conv.artifactId);
    }
  }, [loadQ.data]);

  // ── ReactFlow change handlers ───────────────────────────────────────────
  const onNodesChange = useCallback((changes: NodeChange[]) => {
    setNodes((nds) => applyNodeChanges(changes, nds) as CanvasNode[]);
  }, []);
  const onEdgesChange = useCallback((changes: EdgeChange[]) => {
    setEdges((eds) => applyEdgeChanges(changes, eds) as CanvasEdge[]);
  }, []);
  const onConnect = useCallback((conn: Connection) => {
    if (!conn.source || !conn.target || conn.source === conn.target) return;
    const id = `manual-${conn.source}-${conn.target}-${conn.sourceHandle || ""}-${conn.targetHandle || ""}`;
    setEdges((eds) => {
      if (eds.some((e) => e.id === id)) return eds;
      const newEdge: CanvasEdge = {
        id,
        source: conn.source!,
        target: conn.target!,
        sourceHandle: conn.sourceHandle || undefined,
        targetHandle: conn.targetHandle || undefined,
        type: "column-edge",
        data: {
          relationshipType: "lineage_manual",
          source: "manual",
          confidence: 1,
          verdict: "approved",
          hoverLabel: "manual",
        },
        style: {
          stroke: EDGE_COLORS.lineage_manual,
          strokeWidth: 1.6,
        },
      };
      return addEdge(newEdge, eds) as CanvasEdge[];
    });
  }, []);

  // ── Toolbar actions ─────────────────────────────────────────────────────
  function activateProfileFromFirstNode() {
    if (!primaryProfile) {
      const first = nodes.find((n) => n.data.kind === "table");
      if (first && first.data.kind === "table") {
        setPrimaryProfile(first.data.profile);
      }
    }
  }

  function onPickTable(pick: AddTablePick) {
    const autoLogo = logoKeyForBackend(pick.backend);
    setNodes((nds) => {
      // Compute geometry from the latest ``nds`` rather than the
      // stale closure ``nodes`` — the multi-select Add Table modal
      // fires ``onPick`` N times in a tight loop before any React
      // commit lands, so reading ``nodes.length`` outside this
      // updater gave every pick the same position and the new
      // tables stacked at identical coordinates.
      const tableCount = nds.filter((n) => n.data.kind === "table").length;
      const multi = nds.some(
        (n) =>
          n.data.kind === "table" &&
          n.data.profile &&
          n.data.profile !== pick.profile,
      );
      const node = makeTableNode({
        ...pick,
        position: { x: 80 + tableCount * 32, y: 80 + tableCount * 32 },
        multiProfile: multi,
        isAnchor: tableCount === 0,
      });
      if (autoLogo && node.data.kind === "table") {
        node.data.logoKey = autoLogo;
      }
      // Dedupe: AddTableModal optimistically calls onPick twice —
      // once with empty columns (immediate), once after the
      // background fetchTableColumns settles. If the node already
      // exists (same id from ``makeTableNode``), merge the new
      // columns into it instead of pushing a duplicate.
      const existingIdx = nds.findIndex((n) => n.id === node.id);
      if (existingIdx >= 0) {
        const updatedNodes = nds.slice();
        const prev = updatedNodes[existingIdx];
        if (prev.data.kind === "table" && node.data.kind === "table") {
          updatedNodes[existingIdx] = {
            ...prev,
            data: {
              ...prev.data,
              // Only overwrite when the fresh payload actually has
              // columns — keeps the optimistic empty entry from
              // wiping a previously-enriched node.
              columns: node.data.columns.length
                ? node.data.columns
                : prev.data.columns,
            },
          };
        }
        return updatedNodes;
      }
      const appended = [...nds, node] as CanvasNode[];
      if (multi) {
        return appended.map((n) =>
          n.data.kind === "table"
            ? { ...n, data: { ...n.data, showProfileChip: true } }
            : n,
        ) as CanvasNode[];
      }
      return appended;
    });
    if (!primaryProfile) setPrimaryProfile(pick.profile);
  }

  function addOperatorNode(kind: OperatorKind) {
    const id = `op-tmp-${kind}-${Date.now()}`;
    const node: CanvasNode = {
      id,
      type: nodeTypeForOperator(kind),
      position: { x: 240, y: 140 },
      data: {
        kind: "operator",
        id,
        opKind: kind,
        expression: "",
        upstreamColumns: [],
      },
    };
    setNodes((nds) => [...nds, node]);
  }

  function addLogoNode(logo: LogoRow) {
    const id = `logo-tmp-${logo.key}-${Date.now()}`;
    const node: CanvasNode = {
      id,
      type: "logo",
      position: { x: 200 + nodes.length * 20, y: 200 + nodes.length * 20 },
      width: 120,
      height: 120,
      data: {
        kind: "logo",
        id,
        logoKey: logo.key,
        label: logo.label,
      },
    };
    setNodes((nds) => [...nds, node]);
  }

  function addCommentNode() {
    const id = `comment-tmp-${Date.now()}`;
    const node: CanvasNode = {
      id,
      type: "comment",
      position: { x: 120, y: 120 },
      width: 220,
      height: 140,
      data: { kind: "comment", id, color: "amber", text: "", style: "note" },
      dragHandle: ".lcv-comment-grip",
    };
    setNodes((nds) => [...nds, node]);
  }

  function addTextNode() {
    // Plain-text label — same backend table as comments but with
    // ``style='text'`` so CommentNode skips the sticky-note chrome.
    // No ``dragHandle`` constraint here: the whole node is draggable
    // EXCEPT children with ``.nodrag`` (the editor body) — that
    // gives us "click outside text to drag, click inside to edit"
    // out of the box.
    const id = `text-tmp-${Date.now()}`;
    const node: CanvasNode = {
      id,
      type: "comment",
      position: { x: 160, y: 160 },
      width: 220,
      height: 36,
      data: { kind: "comment", id, color: "amber", text: "", style: "text" },
    };
    setNodes((nds) => [...nds, node]);
  }

  /** Discover persisted edges (FK / view DDL / query log / earlier
   *  manual saves) that sit between two tables on the current
   *  canvas. Runs after AI Generate completes and also on the
   *  toolbar's "Discover related" trigger. Tables already on the
   *  canvas keep their geometry; only edges merge in. */
  async function discoverRelatedEdges(): Promise<number> {
    const tables = nodes.filter((n) => n.data.kind === "table");
    if (tables.length < 2) return 0;
    const entityIds: number[] = [];
    const fqnPairs: { profile: string; fqn: string }[] = [];
    for (const n of tables) {
      const d = n.data as TableNodeData;
      if (typeof d.entityId === "number" && d.entityId > 0) {
        entityIds.push(d.entityId);
      } else if (d.profile && d.fqn) {
        fqnPairs.push({ profile: d.profile, fqn: d.fqn });
      }
    }
    if (entityIds.length + fqnPairs.length < 2) return 0;
    let response: Awaited<ReturnType<typeof lineageEdgesAmong>>;
    try {
      response = await lineageEdgesAmong({
        entityIds,
        tables: fqnPairs,
      });
    } catch {
      return 0;
    }
    if (response.edges.length === 0) return 0;
    // Build entity_id → node-id and FQN → node-id maps so backend
    // edges latch onto whichever node id the canvas already uses.
    const entityToNodeId = new Map<number, string>();
    const fqnToNodeId = new Map<string, string>();
    for (const n of tables) {
      const d = n.data as TableNodeData;
      if (typeof d.entityId === "number" && d.entityId > 0) {
        entityToNodeId.set(d.entityId, n.id);
      }
      if (d.fqn) {
        fqnToNodeId.set(d.fqn, n.id);
        const parts = d.fqn.split(".");
        if (parts.length >= 3) {
          const alias = parts.slice(-2).join(".");
          if (!fqnToNodeId.has(alias)) fqnToNodeId.set(alias, n.id);
        }
      }
    }
    let added = 0;
    setEdges((prev) => {
      const next = [...prev];
      const seen = new Set(next.map((e) => e.id));
      for (const row of response.edges) {
        const sId = entityToNodeId.get(row.from_entity_id);
        const tId = entityToNodeId.get(row.to_entity_id);
        if (!sId || !tId) continue;
        const ce = loadedEdgeToCanvasEdge(row, sId, tId);
        if (seen.has(ce.id)) continue;
        seen.add(ce.id);
        next.push(ce);
        added += 1;
      }
      return next;
    });
    return added;
  }

  /** Hydrate columns for every table on canvas that's still showing
   *  ``(no columns cached)`` — the name-match heuristic needs them
   *  on both sides to produce anything useful. Mirrors the
   *  fire-and-forget shape used inside the streaming AI hook. */
  async function hydrateMissingColumns(): Promise<number> {
    const tables = (rf.getNodes() as CanvasNode[]).filter(
      (n) => n.data.kind === "table",
    );
    const targets = tables.filter((n) => {
      const d = n.data as TableNodeData;
      return (d.columns?.length ?? 0) === 0 && d.profile && d.table;
    });
    if (targets.length === 0) return 0;
    const fetched = await Promise.all(
      targets.map((n) => {
        const d = n.data as TableNodeData;
        return fetchTableColumns({
          profile: d.profile,
          database: d.database || "",
          schema: d.schema || "",
          table: d.table,
        })
          .then((cols) => ({ id: n.id, cols }))
          .catch(() => ({ id: n.id, cols: [] }));
      }),
    );
    let updated = 0;
    rf.setNodes((curr) =>
      (curr as CanvasNode[]).map((cn) => {
        const hit = fetched.find((r) => r.id === cn.id);
        if (!hit || hit.cols.length === 0) return cn;
        if (cn.data.kind !== "table") return cn;
        updated += 1;
        return {
          ...cn,
          data: { ...(cn.data as TableNodeData), columns: hit.cols },
        };
      }),
    );
    return updated;
  }

  async function handleDiscoverRelated() {
    const catalogAdded = await discoverRelatedEdges();
    // Make sure every table has its columns loaded before we ask the
    // heuristic to compare — without this, AI-spawned 2-part FQNs
    // come in column-less and the name-match yields nothing on the
    // user's first click of the Discover button.
    await hydrateMissingColumns();
    const liveNodes = rf.getNodes() as CanvasNode[];
    const liveEdges = rf.getEdges() as CanvasEdge[];
    const proposed = proposeNameMatchEdges(liveNodes, {
      existingEdges: liveEdges,
    });
    let heuristicAdded = 0;
    if (proposed.edges.length > 0) {
      rf.setEdges((eds) => {
        const next = [...(eds as CanvasEdge[])];
        const seen = new Set(next.map((e) => e.id));
        for (const e of proposed.edges) {
          if (seen.has(e.id)) continue;
          seen.add(e.id);
          next.push(e);
          heuristicAdded += 1;
        }
        return next;
      });
    }
    const total = catalogAdded + heuristicAdded;
    const parts: string[] = [];
    if (catalogAdded) parts.push(`${catalogAdded} catalog`);
    if (heuristicAdded) parts.push(`${heuristicAdded} name-match`);
    toast.push({
      title: "Discover related",
      description: total
        ? `Added ${parts.join(" + ")} edge${total === 1 ? "" : "s"}.`
        : "No additional edges between the tables on canvas.",
      tone: total ? "success" : "info",
    });
  }

  function handleAutoLayout() {
    // Source the latest nodes / edges straight from ReactFlow's store
    // instead of the React closure: ``onDone`` (and the streaming
    // hook's callback machinery in general) caches its handler at
    // mount, so reading from the rendered ``nodes`` / ``edges``
    // closure here returns stale state and dagre / radial run over
    // an empty graph. ``rf.getNodes()`` / ``rf.getEdges()`` always
    // return the live ReactFlow internals.
    const liveNodes = rf.getNodes() as CanvasNode[];
    const liveEdges = rf.getEdges() as CanvasEdge[];
    let anchorId: string | undefined;
    if (aiAnchor) {
      const found = liveNodes.find(
        (n) =>
          n.data.kind === "table" &&
          (n.data as TableNodeData).fqn === aiAnchor,
      );
      anchorId = found?.id;
    }
    const laidOut = autoLayout(liveNodes, liveEdges, { anchorId });
    rf.setNodes(laidOut);
    // Center the freshly laid-out graph in the viewport so the user
    // sees the whole anchor + neighbourhood without panning. Defer
    // until ReactFlow has committed the new positions — no
    // ``maxZoom`` cap so a small neighbourhood can actually scale
    // up enough to fill the viewport instead of sitting tiny in
    // the middle.
    requestAnimationFrame(() => {
      rf.fitView({ padding: 0.2, duration: 300 });
    });
  }

  // ── Streaming AI ─────────────────────────────────────────────────────────
  // Resolved (fqn → node-id) map for the current batch. Populated
  // inside the setNodes callback below and read back when setEdges
  // builds its edge entries. The two callbacks share this closure
  // and React 18 runs them in order during the next render cycle,
  // so by the time the edge callback fires every FQN is mapped.
  const fqnToIdRef = useRef(new Map<string, string>());
  // Count of nodes synthesised during the in-flight stream. Reset
  // when ``startGenerate`` fires, incremented in each batch, read by
  // ``onDone`` to decide whether to auto-layout. Skipping layout
  // when the stream only added edges (no new tables) keeps already
  // arranged canvases stable.
  const streamingAddedRef = useRef(0);
  const streamingAI = useStreamingAI({
    onBatch: (batch: StreamBatch) => {
      const newlySynthesised: Array<{
        id: string;
        database: string;
        schema: string;
        table: string;
      }> = [];
      const fqnToId = new Map<string, string>();
      fqnToIdRef.current = fqnToId;

      setNodes((prevNodes) => {
        const next = [...prevNodes];

        // Build an FQN → node-id index from existing TABLE nodes so
        // streamed neighbors snap onto whatever the user already
        // dragged in (anchor added via the Add-Table modal, prior
        // LLM batches, etc.) instead of spawning duplicate copies of
        // the same table. This is the fix for "AI Generate getiriyo
        // ama aynı tabloyu yanına atıyo".
        //
        // Also record a 2-part ``schema.table`` alias for any 3-part
        // FQN — the LLM almost always streams ``schema.table`` while
        // the picker stores ``database.schema.table``, so without the
        // alias the anchor never matched the stream and got
        // duplicated by ``ensureTable`` with an empty columns list.
        for (const n of next) {
          if (n.data.kind === "table") {
            const fqn = (n.data as { fqn?: string }).fqn;
            if (!fqn) continue;
            fqnToId.set(fqn, n.id);
            const parts = fqn.split(".");
            if (parts.length >= 3) {
              const alias = parts.slice(-2).join(".");
              if (!fqnToId.has(alias)) fqnToId.set(alias, n.id);
            }
          }
        }

        // Radial placement around the anchor if it's on the canvas.
        // Falls back to a fixed origin when AI Generate runs without
        // a pre-placed anchor (unusual but possible).
        const anchorNode = aiAnchor
          ? next.find(
              (n) =>
                n.data.kind === "table" &&
                (n.data as { fqn?: string }).fqn === aiAnchor,
            )
          : undefined;
        const anchorData = anchorNode?.data.kind === "table"
          ? (anchorNode.data as TableNodeData)
          : undefined;
        const anchorDatabase = anchorData?.database || "";
        const anchorProfile = anchorData?.profile || primaryProfile;
        const anchorLogoKey = anchorData?.logoKey || "";
        const cx = anchorNode?.position?.x ?? 320;
        const cy = anchorNode?.position?.y ?? 200;

        let newlyAdded = 0;
        const ensureTable = (fqn: string): string => {
          if (!fqn) return "";
          const existing = fqnToId.get(fqn);
          if (existing) return existing;

          const parts = fqn.split(".");
          // Inherit the anchor's database when the streamed FQN is
          // only 2-part — without it ``fetchTableColumns`` can't
          // resolve the catalog row and silently returns ``[]``.
          const database = parts.length > 2 ? parts[0] : anchorDatabase;
          const schema = parts.length > 1 ? parts[parts.length - 2] : "";
          const table = parts[parts.length - 1];
          // Place new neighbors around the anchor: 8 evenly spaced
          // slots per ring (45° apart), subsequent rings ~200px
          // further out so a batch of 20 suggestions stays readable.
          const slot = newlyAdded % 8;
          const ring = Math.floor(newlyAdded / 8);
          const angle = slot * (Math.PI / 4);
          const radius = 280 + ring * 200;
          newlyAdded += 1;
          const id = `n-fqn-${fqn}`;
          const tbl: CanvasNode = {
            id,
            type: "table",
            position: {
              x: cx + radius * Math.cos(angle),
              y: cy + radius * Math.sin(angle),
            },
            data: {
              kind: "table",
              id,
              profile: anchorProfile,
              database,
              schema,
              table,
              fqn,
              columns: [],
              // Inherit the anchor's logo so every AI-spawned table
              // gets the same backend badge instead of rendering
              // un-tagged. Manual logo-picker still overrides.
              ...(anchorLogoKey ? { logoKey: anchorLogoKey } : {}),
            },
          };
          next.push(tbl);
          fqnToId.set(fqn, id);
          newlySynthesised.push({ id, database, schema, table });
          return id;
        };

        for (const ed of batch.edges) {
          // Defense in depth — the backend parser already rejects
          // self-loops, but a stray ``from == to`` here would still
          // spawn duplicate-anchor visual artifacts.
          if (ed.from && ed.from === ed.to) continue;
          ensureTable(ed.from);
          ensureTable(ed.to);
        }
        streamingAddedRef.current += newlySynthesised.length;
        return next;
      });

      // Fire-and-forget column enrichment for the just-added tables.
      // Cache-first; failures are silent so the canvas never blocks.
      if (newlySynthesised.length && primaryProfile) {
        Promise.all(
          newlySynthesised.map((n) =>
            fetchTableColumns({
              profile: primaryProfile,
              database: n.database,
              schema: n.schema,
              table: n.table,
            })
              .then((cols) => ({ id: n.id, cols }))
              .catch(() => ({ id: n.id, cols: [] })),
          ),
        ).then((results) => {
          setNodes((curr) =>
            curr.map((cn) => {
              const hit = results.find((r) => r.id === cn.id);
              if (!hit || hit.cols.length === 0) return cn;
              return cn.data.kind === "table"
                ? { ...cn, data: { ...cn.data, columns: hit.cols } }
                : cn;
            }),
          );
        });
      }

      setEdges((prevEdges) => {
        const next = [...prevEdges];
        const seen = new Set(next.map((e) => e.id));
        const fqnToId = fqnToIdRef.current;
        for (const ed of batch.edges) {
          if (ed.from && ed.from === ed.to) continue;
          // Resolve to the canonical node id assigned in the setNodes
          // pass above. Existing nodes (user-placed anchor + prior
          // batches) keep their original id, so streamed edges land
          // ON them instead of next to a stray ``n-fqn-...`` twin.
          const sourceId = fqnToId.get(ed.from) ?? `n-fqn-${ed.from}`;
          const targetId = fqnToId.get(ed.to) ?? `n-fqn-${ed.to}`;
          const id = `${batch.extractor}-${sourceId}-${targetId}-${ed.from_column || ""}-${ed.to_column || ""}`;
          if (seen.has(id)) continue;
          seen.add(id);
          const color = EDGE_COLORS[ed.type] ?? EDGE_COLORS.unknown;
          const dashed =
            ed.type === "name_match" ||
            (ed.type === "lineage_llm" && ed.confidence < 0.7);
          const hoverLabel = buildEdgeHoverLabel({
            fromColumn: ed.from_column,
            toColumn: ed.to_column,
            type: ed.type,
            confidence: ed.confidence,
            dashed,
          });
          next.push({
            id,
            source: sourceId,
            target: targetId,
            sourceHandle: ed.from_column || undefined,
            targetHandle: ed.to_column || undefined,
            type: "column-edge",
            className: "lcv-edge-fadein",
            data: {
              relationshipType: ed.type,
              source: ed.extractor,
              confidence: ed.confidence,
              verdict: "",
              hoverLabel,
            },
            style: {
              stroke: color,
              strokeWidth: ed.confidence >= 0.9 ? 1.6 : 1.1,
              strokeDasharray: dashed ? "5 4" : undefined,
            },
          });
        }
        return next;
      });
    },
    onWarning: (msg) =>
      toast.push({ title: "AI warning", description: msg, tone: "warning" }),
    onError: (msg) => {
      toast.push({ title: "AI stream error", description: msg, tone: "error" });
      setGenerating(false);
    },
    onDone: (totals) => {
      setGenerating(false);
      toast.push({
        title: "Generation complete",
        description: `${totals.total_edges} edge(s) streamed in.`,
        tone: "success",
      });
      // Pull deterministic edges (FK / view DDL / query log) that the
      // anchor-centric LLM extractor never proposes, so neighbour-to-
      // neighbour relationships surface as soon as the stream ends.
      // Fire-and-forget; failures are silent so the stream completion
      // toast stays the primary signal.
      void discoverRelatedEdges();
      // Auto-arrange only when this run actually added new tables —
      // an edges-only stream shouldn't disturb a canvas the user has
      // already laid out by hand. Wait ~80 ms so React has flushed
      // the last batch's setNodes AND the fire-and-forget column
      // enrichment commits have landed; without that the layout
      // pass runs before some neighbours exist and the visual
      // result is exactly the pre-layout cluster the user
      // complained about.
      if (streamingAddedRef.current > 0) {
        window.setTimeout(() => handleAutoLayout(), 80);
      }
    },
  });

  function startGenerate() {
    if (!primaryProfile || !aiAnchor) {
      toast.push({ title: "Pick anchor + profile first", tone: "warning" });
      return;
    }
    // Look up the anchor's database (and prefer the node-local profile
    // if the user added it from a non-primary profile). Passing
    // ``database`` explicitly avoids the backend's 3-part-FQN parsing
    // ambiguity that mistakes ``db.schema.table`` for
    // ``schema.table.column``.
    const anchorNode = nodes.find(
      (n) => n.data.kind === "table" && (n.data as { fqn?: string }).fqn === aiAnchor,
    );
    const anchorDb = anchorNode?.data.kind === "table"
      ? (anchorNode.data as { database?: string }).database || ""
      : "";
    const anchorProfile = anchorNode?.data.kind === "table"
      ? (anchorNode.data as { profile?: string }).profile || primaryProfile
      : primaryProfile;
    setGenerating(true);
    setGenerateOpen(false);
    // Reset the per-stream counter so ``onDone`` only auto-layouts
    // when *this* run added at least one table.
    streamingAddedRef.current = 0;
    streamingAI.start(aiAnchor, { profile: anchorProfile, database: anchorDb });
  }

  // ── Save (manual) ────────────────────────────────────────────────────────
  const saveMut = useMutation({
    mutationFn: async () => {
      activateProfileFromFirstNode();
      const profile = primaryProfile;
      if (!profile) throw new Error("Pick at least one table first to bind the profile.");
      const tables = nodes.filter((n) => n.data.kind === "table") as CanvasNode[];
      if (tables.length === 0) throw new Error("Add at least one node.");
      if (!saveName.trim()) throw new Error("Give the canvas a name.");
      const anchorFqn = (tables[0].data as TableNodeData).fqn;
      const payload = buildSavePayload({
        primaryProfile: profile,
        artifactName: saveName.trim(),
        anchorFqn,
        nodes,
        edges,
      });
      return saveManualCanvas(payload);
    },
    onSuccess: (res) => {
      toast.push({
        title: "Canvas saved",
        description: `Persisted ${res.persisted_edges} edge(s).`,
        tone: "success",
      });
      setSaveOpen(false);
      setActiveArtifactId(res.artifact_id);
      // Navigate by id — never by name. This is the save-canvas bug fix.
      setParams({ artifact: String(res.artifact_id) });
      qc.invalidateQueries({ queryKey: ["lineage-artifacts"] });
    },
    onError: (e: Error) =>
      toast.push({ title: "Save failed", description: e.message, tone: "error" }),
  });

  // ── PNG export ───────────────────────────────────────────────────────────
  async function handleExportPng() {
    if (!canvasShellRef.current) return;
    try {
      await exportPng(
        canvasShellRef.current,
        `${(artifactName || "lineage").replace(/[^A-Za-z0-9_-]+/g, "_")}.png`,
      );
    } catch (e) {
      toast.push({
        title: "PNG export failed",
        description: (e as Error).message,
        tone: "error",
      });
    }
  }

  // ── SQL import ───────────────────────────────────────────────────────────
  async function handleSqlImport() {
    try {
      const parsed = await parseSql(sqlInput);
      setSqlImportOpen(false);
      setSqlInput("");
      // Translate parsed.tables → DataFrameNodes and parsed.operators →
      // OperatorNodes. The FQNs come back as schema.table strings; we
      // synthesize them as profile-less local nodes (user can bind the
      // profile by save-time).
      const next: CanvasNode[] = [];
      let x = 80;
      for (const t of parsed.tables) {
        next.push(
          makeTableNode({
            profile: primaryProfile,
            database: "",
            schema: t.schema,
            table: t.table,
            columns: t.columns.map((c) => ({ name: c, dtype: "unknown" })),
            position: { x, y: 100 },
            multiProfile: false,
          }),
        );
        x += 280;
      }
      let opY = 320;
      for (const op of parsed.operators) {
        next.push({
          id: op.id,
          type: nodeTypeForOperator(op.kind),
          position: { x: 200, y: opY },
          data: {
            kind: "operator",
            id: op.id,
            opKind: (op.kind as OperatorKind),
            expression: op.expression,
            upstreamColumns: [],
          },
        });
        opY += 140;
      }
      setNodes((prev) => [...prev, ...next]);
      toast.push({
        title: "SQL imported",
        description: `${parsed.tables.length} table(s), ${parsed.operators.length} operator(s).`,
        tone: "success",
      });
    } catch (e) {
      toast.push({
        title: "SQL parse failed",
        description: (e as Error).message,
        tone: "error",
      });
    }
  }

  async function handleSqlExport() {
    try {
      const canvas = {
        tables: nodes
          .filter((n) => n.data.kind === "table")
          .map((n) => {
            const d = n.data as TableNodeData;
            return {
              id: d.fqn,
              schema: d.schema,
              table: d.table,
              columns: (d.columns || []).map((c) => c.name),
            };
          }),
        operators: nodes
          .filter((n) => n.data.kind === "operator")
          .map((n) => {
            const d = n.data as { id: string; opKind: string; expression: string };
            return { id: d.id, kind: d.opKind, expression: d.expression };
          }),
      };
      const res = await renderSql(canvas);
      setSqlOutput(res.sql);
      setSqlExportOpen(true);
    } catch (e) {
      toast.push({
        title: "SQL render failed",
        description: (e as Error).message,
        tone: "error",
      });
    }
  }

  // ── Search + Tracker ─────────────────────────────────────────────────────
  function focusNode(nodeId: string) {
    const node = nodes.find((n) => n.id === nodeId);
    if (!node) return;
    setNodes((nds) =>
      nds.map((n) => ({
        ...n,
        selected: n.id === nodeId,
      })),
    );
  }
  function highlightAttribute(nodeId: string, column: string) {
    setNodes((nds) =>
      nds.map((n) => {
        if (n.data.kind !== "table") return n;
        if (n.id !== nodeId) return n;
        return { ...n, data: { ...n.data, tracedColumn: column } } as CanvasNode;
      }),
    );
  }

  // ── Global keyboard shortcuts ───────────────────────────────────────────
  useEffect(() => {
    function onKey(e: globalThis.KeyboardEvent) {
      const meta = e.metaKey || e.ctrlKey;
      if (meta && e.key === "k") {
        e.preventDefault();
        setSearchOpen(true);
      } else if (meta && e.shiftKey && e.key.toLowerCase() === "f") {
        e.preventDefault();
        setTrackerOpen(true);
      } else if (meta && e.key === "s") {
        e.preventDefault();
        if (!saveName) setSaveName(artifactName || `canvas-${Date.now()}`);
        setSaveOpen(true);
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [saveName, artifactName]);

  // ── Share — copy URL to clipboard ───────────────────────────────────────
  async function handleShare() {
    if (!activeArtifactId) {
      toast.push({ title: "Save first", description: "Sharing needs a persisted canvas." });
      return;
    }
    const url = `${window.location.origin}/lineage?artifact=${activeArtifactId}`;
    try {
      await navigator.clipboard.writeText(url);
      toast.push({ title: "Link copied", description: url, tone: "success" });
    } catch {
      toast.push({ title: "Could not copy", description: url, tone: "warning" });
    }
  }

  // ── Tab-friendly KeyDown for canvas root ────────────────────────────────
  function onCanvasKeyDown(e: KeyboardEvent<HTMLDivElement>) {
    // Single-character shortcuts (no modifiers) only fire when the
    // canvas itself is focused, never inside an editable surface.
    // ``contentEditable`` text labels are not HTMLInputElement /
    // HTMLTextAreaElement so we check ``isContentEditable`` too —
    // without that the user's typed letter triggers our shortcut
    // (e.g. typing "table" in a label spawned an Add-Table modal).
    if (
      e.target instanceof HTMLInputElement ||
      e.target instanceof HTMLTextAreaElement ||
      (e.target instanceof HTMLElement && e.target.isContentEditable)
    ) {
      return;
    }
    // Any modifier key combo (Cmd / Ctrl / Alt) is reserved for the
    // global handler — bail so we don't double-fire.
    if (e.metaKey || e.ctrlKey || e.altKey) return;
    const k = e.key.toLowerCase();
    if (k === "d") setAddOpen(true);
    else if (k === "f") addOperatorNode("filter");
    else if (k === "e") addOperatorNode("function");
    else if (k === "g") addOperatorNode("aggregate");
    else if (k === "j") addOperatorNode("join");
    else if (k === "c") addCommentNode();
    else if (k === "t") addTextNode();
    else if (k === "i") setLogoPickerOpen(true);
    else if (k === "l") handleAutoLayout();
  }

  const tablesOnCanvas = useMemo(
    () => nodes.filter((n) => n.data.kind === "table") as CanvasNode[],
    [nodes],
  );

  // Conservative "has work to preserve" check for the Saved-lineage
  // dropdown's confirm prompt. Any node on the canvas counts — we
  // don't track per-edit dirtiness, so the menu errs on the side of
  // asking once before replacing user work. The menu itself
  // suppresses the prompt when the user picks the artifact already
  // open, so the only case that triggers it is "switching away from
  // something I can see on screen."
  const hasUnsavedWork = nodes.length > 0;

  function handleOpenSavedArtifact(id: number) {
    setParams({ artifact: String(id) });
  }

  function handleActiveSavedArtifactDeleted() {
    setNodes([]);
    setEdges([]);
    setPrimaryProfile("");
    setArtifactName("");
    setActiveArtifactId(null);
    setParams({});
  }

  /** Clear the canvas back to a blank slate. Confirms first when
   *  there is unsaved work so a stray click cannot blow away
   *  in-progress edits. */
  function handleNewLineage() {
    if (hasUnsavedWork) {
      if (!window.confirm("Discard the current canvas and start a new lineage?")) {
        return;
      }
    }
    handleActiveSavedArtifactDeleted();
    setAiAnchor("");
    setSaveName("");
  }

  return (
    <div
      ref={pageWrapperRef}
      className="flex flex-col gap-2"
      style={{ height: pageWrapperHeight }}
    >
      <PageHeader
        title="Lineage"
        breadcrumbs={[{ label: "Lineage", to: "/lineage" }]}
        description="AI-generated and hand-authored data lineage across every cached profile."
      />
      <Toolbar
        primaryProfile={primaryProfile}
        onAddTable={() => setAddOpen(true)}
        onAddFilter={() => addOperatorNode("filter")}
        onAddJoin={() => addOperatorNode("join")}
        onAddAggregate={() => addOperatorNode("aggregate")}
        onAddFunction={() => addOperatorNode("function")}
        onAddComment={addCommentNode}
        onAddText={addTextNode}
        onAddLogo={() => setLogoPickerOpen(true)}
        onUndo={() => toast.push({ title: "Undo/Redo wiring in progress" })}
        onRedo={() => toast.push({ title: "Undo/Redo wiring in progress" })}
        canUndo={false}
        canRedo={false}
        onAutoLayout={handleAutoLayout}
        onGenerateAI={() => {
          activateProfileFromFirstNode();
          setAiAnchor(
            tablesOnCanvas[0]
              ? (tablesOnCanvas[0].data as TableNodeData).fqn
              : "",
          );
          setGenerateOpen(true);
        }}
        generating={generating}
        onSearch={() => setSearchOpen(true)}
        onTrackAttribute={() => setTrackerOpen(true)}
        onSave={() => {
          if (!saveName) setSaveName(artifactName || `canvas-${Date.now()}`);
          setSaveOpen(true);
        }}
        onExportPng={handleExportPng}
        onShare={handleShare}
        onImportSql={() => setSqlImportOpen(true)}
        onExportSql={handleSqlExport}
        hasUnsavedWork={hasUnsavedWork}
        activeArtifactId={activeArtifactId}
        onOpenSavedArtifact={handleOpenSavedArtifact}
        onActiveSavedArtifactDeleted={handleActiveSavedArtifactDeleted}
        onDiscoverRelated={() => void handleDiscoverRelated()}
        onNewLineage={handleNewLineage}
      />
      <div
        ref={canvasShellRef}
        className="lcv-canvas-root relative min-h-0 flex-1 overflow-hidden rounded-xl border border-surface-border"
        tabIndex={0}
        onKeyDown={onCanvasKeyDown}
      >
        <ColumnEdgeMarkerDefs />
        <div className="pointer-events-none absolute right-3 top-3 z-10 flex items-center gap-2">
          {generating && (
            <div className="inline-flex items-center gap-1.5 rounded-md bg-surface-raised/90 px-2 py-1 text-[11px] text-fg-muted shadow">
              <Loader2 size={12} className="animate-spin" />
              streaming AI batches…
            </div>
          )}
          <EdgeLegendChip />
        </div>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          fitView
          fitViewOptions={{ padding: 0.2 }}
          proOptions={{ hideAttribution: true }}
          // Loosen the zoom range so the user can scale the canvas
          // way out (default min 0.5 capped large lineages mid-screen)
          // and in (default max 2 lost detail on dense graphs).
          minZoom={0.1}
          maxZoom={4}
          // Multi-select + delete keys. Arrays so both Mac and
          // Windows / Linux work without sniffing the platform:
          //   * Hold ⌘ / Ctrl + click to add a node to the selection.
          //   * Hold Shift and drag the canvas to rubber-band select.
          //   * Press Backspace or Delete with anything selected to
          //     remove every selected node + its incident edges in one
          //     pass — works for tables, operators, comments, text
          //     labels, and logo nodes alike.
          // Selectable / deletable defaults are on; spelling them out
          // makes the contract obvious.
          multiSelectionKeyCode={["Meta", "Control"]}
          selectionKeyCode="Shift"
          deleteKeyCode={["Backspace", "Delete"]}
          nodesDraggable
          nodesConnectable
          elementsSelectable
        >
          <Background gap={20} size={1} />
          <Controls showInteractive={false} />
        </ReactFlow>
      </div>

      <AddTableModal
        open={addOpen}
        onClose={() => setAddOpen(false)}
        defaultProfile={primaryProfile}
        onPick={onPickTable}
        onBatchAdded={(n) => {
          // Multi-pick lands every node along a tight diagonal so
          // the picks are at least visually distinct, then we run
          // the regular auto-layout so they snap into the canvas's
          // standard grid (or radial when an anchor exists).
          if (n > 1) {
            window.setTimeout(() => handleAutoLayout(), 60);
          }
        }}
      />

      <LogoPicker
        open={logoPickerOpen}
        onClose={() => setLogoPickerOpen(false)}
        onPick={addLogoNode}
        title="Add a logo node"
      />

      <SearchModal
        open={searchOpen}
        onClose={() => setSearchOpen(false)}
        nodes={nodes}
        onSelect={focusNode}
      />
      <AttributeTrackerPanel
        open={trackerOpen}
        onClose={() => setTrackerOpen(false)}
        nodes={nodes}
        onHighlight={highlightAttribute}
      />

      <Modal
        open={saveOpen}
        onClose={() => setSaveOpen(false)}
        title={<span>Save canvas</span>}
        description="Persist this canvas as a lineage artifact. The artifact id is the identifier we use to re-open it — the name is purely display."
        footer={
          <div className="flex justify-end gap-2">
            <Button variant="secondary" size="md" onClick={() => setSaveOpen(false)}>
              Cancel
            </Button>
            <Button
              variant="primary"
              size="md"
              loading={saveMut.isPending}
              disabled={!saveName.trim() || saveMut.isPending}
              onClick={() => saveMut.mutate()}
            >
              Save
            </Button>
          </div>
        }
      >
        <label className="block space-y-1 text-sm">
          <span className="text-[10px] uppercase tracking-wide text-fg-muted">
            Canvas name
          </span>
          <input
            type="text"
            value={saveName}
            onChange={(e) => setSaveName(e.target.value)}
            placeholder="my-canvas"
            className="block w-full rounded-md border border-surface-border bg-surface-raised px-3 py-2 text-sm focus:border-accent-default focus:outline-none"
          />
        </label>
      </Modal>

      <Modal
        open={generateOpen}
        onClose={() => setGenerateOpen(false)}
        title={<span>AI Generate lineage</span>}
        description="AMX walks every cached extractor (FK, view DDL, query log, codebase, prior LLM verdicts) and streams edges into the canvas as each batch completes."
        footer={
          <div className="flex justify-end gap-2">
            <Button variant="secondary" size="md" onClick={() => setGenerateOpen(false)}>
              Cancel
            </Button>
            <Button
              variant="primary"
              size="md"
              onClick={startGenerate}
              disabled={!aiAnchor || !primaryProfile}
            >
              Start streaming
            </Button>
          </div>
        }
      >
        <div className="space-y-3 text-sm">
          <label className="block space-y-1">
            <span className="text-[10px] uppercase tracking-wide text-fg-muted">
              Anchor table
            </span>
            <select
              value={aiAnchor}
              onChange={(e) => setAiAnchor(e.target.value)}
              className="block w-full rounded-md border border-surface-border bg-surface-raised px-2 py-1.5 text-sm"
            >
              <option value="">— pick anchor —</option>
              {tablesOnCanvas.map((n) => {
                const d = n.data as TableNodeData;
                return (
                  <option key={n.id} value={d.fqn}>
                    {d.fqn} ({d.profile})
                  </option>
                );
              })}
            </select>
            <p className="text-[11px] text-fg-muted">
              The streamed edges land relative to this anchor. Use the active
              primary profile{" "}
              <span className="font-mono">{primaryProfile || "(none)"}</span>.
            </p>
          </label>
        </div>
      </Modal>

      <Modal
        open={sqlImportOpen}
        onClose={() => setSqlImportOpen(false)}
        title={<span>Import SQL</span>}
        description="Paste a SELECT and AMX will build the canvas nodes for it via sqlglot."
        footer={
          <div className="flex justify-end gap-2">
            <Button variant="secondary" size="md" onClick={() => setSqlImportOpen(false)}>
              Cancel
            </Button>
            <Button variant="primary" size="md" onClick={handleSqlImport} disabled={!sqlInput.trim()}>
              Parse
            </Button>
          </div>
        }
      >
        <textarea
          rows={10}
          spellCheck={false}
          value={sqlInput}
          onChange={(e) => setSqlInput(e.target.value)}
          placeholder="SELECT … FROM …"
          className="block w-full rounded-md border border-surface-border bg-surface-raised px-2 py-2 font-mono text-[12.5px] outline-none focus:border-accent-default"
        />
      </Modal>

      <Modal
        open={sqlExportOpen}
        onClose={() => setSqlExportOpen(false)}
        title={<span>Export SQL</span>}
        description="The current canvas, composed back into a SELECT."
        footer={
          <div className="flex justify-end gap-2">
            <Button variant="secondary" size="md" onClick={() => setSqlExportOpen(false)}>
              Close
            </Button>
            <Button
              variant="primary"
              size="md"
              onClick={async () => {
                try {
                  await navigator.clipboard.writeText(sqlOutput);
                  toast.push({ title: "Copied to clipboard", tone: "success" });
                } catch {
                  toast.push({ title: "Copy failed", tone: "warning" });
                }
              }}
            >
              Copy
            </Button>
          </div>
        }
      >
        <pre className="block max-h-80 w-full overflow-auto rounded-md border border-surface-border bg-surface px-3 py-2 font-mono text-[12px]">
          {sqlOutput}
        </pre>
      </Modal>
    </div>
  );
}

// Suppress unused warning for OPERATOR_COLORS import (it's used by node modules
// indirectly via constants); kept here to confirm the palette is loaded.
void OPERATOR_COLORS;
