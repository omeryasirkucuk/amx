// Edit view for a single Documentation Page.
// Document-first layout: canvas is the hero (no card), outline is a
// quiet sticky text list on the left, the rail on the right is a
// single column divided by hairlines (no nested cards). The editor
// toolbar is hoisted out of PageEditor so it sits above the
// 3-column grid; this way the first content element of each column
// (outline label, canvas H1, rail label) shares the same Y baseline.

import { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import type { Editor } from "@tiptap/react";
import {
  ChevronRight,
  History,
  Loader2,
  Maximize2,
  Minimize2,
  Plus,
  Trash2,
} from "lucide-react";

import AssetChip from "../components/pages/AssetChip";
import EditorFooter from "../components/pages/EditorFooter";
import EditorToolbar from "../components/pages/EditorToolbar";
import EditorViewSwitcher, {
  loadStoredView,
  type EditorView,
} from "../components/pages/EditorViewSwitcher";
import PageEditor from "../components/pages/PageEditor";
import PageExportMenu from "../components/pages/PageExportMenu";
import PageOutline from "../components/pages/PageOutline";
import RegeneratePopover from "../components/pages/RegeneratePopover";
import SourceAttacher from "../components/pages/SourceAttacher";
import VersionsDrawer from "../components/pages/VersionsDrawer";
import { Badge, Button, useToast } from "../components/ui";
import type { BadgeTone } from "../components/ui";
import { cn } from "../lib/cn";
import {
  useDeletePage,
  useGeneratePage,
  usePage,
  useSavePage,
  type PageSource,
  type PageVersion,
} from "../hooks/usePages";

const AUTOSAVE_DELAY_MS = 5000;
const FOCUS_KEY = "amx-pages-focus";

const STATUS_TONE: Record<string, BadgeTone> = {
  draft: "neutral",
  published: "positive",
  deleted: "warning",
};

export default function PageEditRoute() {
  const params = useParams();
  const navigate = useNavigate();
  const toast = useToast();
  const pageId = params.pageId ?? "";

  const detail = usePage(pageId);
  const save = useSavePage(pageId);
  const regen = useGeneratePage(pageId);
  const del = useDeletePage(pageId);

  const [title, setTitle] = useState("");
  const [markdown, setMarkdown] = useState("");
  const [sources, setSources] = useState<PageSource[]>([]);
  const [lastSavedAt, setLastSavedAt] = useState<Date | null>(null);
  const [saving, setSaving] = useState(false);
  const [view, setView] = useState<EditorView>(() => loadStoredView());
  const [versionsOpen, setVersionsOpen] = useState(false);
  const [editor, setEditor] = useState<Editor | null>(null);
  const [focusMode, setFocusMode] = useState<boolean>(() => {
    try {
      return window.sessionStorage.getItem(FOCUS_KEY) === "1";
    } catch {
      return false;
    }
  });

  const initRef = useRef(false);
  const autosaveTimer = useRef<number | null>(null);
  const surfaceRef = useRef<HTMLDivElement | null>(null);

  // Hydrate local state once the detail arrives.
  useEffect(() => {
    if (!detail.data || initRef.current) return;
    initRef.current = true;
    setTitle(detail.data.title);
    setMarkdown(detail.data.markdown_body);
    setSources(detail.data.sources ?? []);
    setLastSavedAt(new Date(detail.data.updated_at));
  }, [detail.data]);

  // Debounced autosave whenever title / markdown change post-hydration.
  useEffect(() => {
    if (!initRef.current || !pageId) return;
    if (autosaveTimer.current) window.clearTimeout(autosaveTimer.current);
    autosaveTimer.current = window.setTimeout(() => {
      void runSave();
    }, AUTOSAVE_DELAY_MS);
    return () => {
      if (autosaveTimer.current) window.clearTimeout(autosaveTimer.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [title, markdown]);

  useEffect(() => {
    try {
      window.sessionStorage.setItem(FOCUS_KEY, focusMode ? "1" : "0");
    } catch {
      /* ignore */
    }
  }, [focusMode]);

  async function runSave(overrideBody?: string) {
    if (!pageId) return;
    setSaving(true);
    try {
      const updated = await save.mutateAsync({
        title: title.trim() || "Untitled",
        markdown_body: overrideBody ?? markdown,
      });
      setLastSavedAt(new Date(updated.updated_at));
    } catch (e) {
      toast.push({ title: (e as Error).message, tone: "error" });
    } finally {
      setSaving(false);
    }
  }

  async function runRegenerate(steering: string) {
    if (!pageId) return;
    try {
      const updated = await regen.mutateAsync();
      setMarkdown(updated.markdown_body);
      setTitle(updated.title);
      setLastSavedAt(new Date(updated.updated_at));
      if (steering) {
        // Pin the steering text on the next version's note so the
        // user can spot which run produced which body.
        await save.mutateAsync({
          markdown_body: updated.markdown_body,
          note: `steering: ${steering}`,
        });
      }
      toast.push({ title: "Page regenerated", tone: "success" });
    } catch (e) {
      toast.push({ title: (e as Error).message, tone: "error" });
    }
  }

  async function runDelete() {
    if (!window.confirm("Delete this page?")) return;
    try {
      await del.mutateAsync();
      toast.push({ title: "Page deleted", tone: "success" });
      navigate("/pages");
    } catch (e) {
      toast.push({ title: (e as Error).message, tone: "error" });
    }
  }

  async function runRestore(version: PageVersion) {
    setMarkdown(version.markdown_body);
    setVersionsOpen(false);
    await runSave(version.markdown_body);
    toast.push({ title: `Restored v${version.version_no}`, tone: "success" });
  }

  const page = detail.data;
  const versions = useMemo<PageVersion[]>(
    () => page?.versions ?? [],
    [page?.versions],
  );

  if (detail.isLoading) {
    return (
      <div className="mx-auto w-full max-w-7xl px-4 py-6">
        <SkeletonShell />
      </div>
    );
  }
  if (detail.error || !page) {
    return (
      <div className="mx-auto w-full max-w-7xl px-4 py-6 text-sm text-critical">
        {(detail.error as Error)?.message ?? "Page not found."}
      </div>
    );
  }

  const toolbarDisabled = view !== "edit";

  return (
    <div
      className={cn(
        "mx-auto w-full px-4 py-6 sm:px-6",
        focusMode ? "max-w-3xl" : "max-w-7xl",
      )}
    >
      {/* ── Header ───────────────────────────────────────────────── */}
      <header className="mb-6">
        <div className="flex items-center gap-1.5 text-[11px] uppercase tracking-wider text-ink-dim">
          <button
            type="button"
            onClick={() => navigate("/pages")}
            className="hover:text-ink"
          >
            Pages
          </button>
          <ChevronRight size={11} />
          <span className="truncate normal-case tracking-normal text-ink-muted">
            {page.title || "Untitled"}
          </span>
        </div>
        <div className="mt-2 flex flex-wrap items-center justify-between gap-3">
          <input
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            placeholder="Untitled"
            aria-label="Page title"
            className="min-w-0 flex-1 bg-transparent text-3xl font-semibold tracking-tight text-ink outline-none placeholder:text-ink-dim focus:border-b focus:border-accent/40 lg:text-4xl"
          />
          <div className="flex flex-wrap items-center gap-2">
            <Badge tone={STATUS_TONE[page.status] ?? "neutral"}>
              {page.status}
            </Badge>
            <SavedIndicator saving={saving} lastSavedAt={lastSavedAt} />
            <PageExportMenu pageId={page.id} pageTitle={title} />
            <Button
              variant="secondary"
              leadingIcon={<Plus size={14} />}
              onClick={() => navigate("/pages/new")}
              title="Start a new documentation page"
            >
              New
            </Button>
          </div>
        </div>
      </header>

      {/* ── View tabs + action chips ─────────────────────────────── */}
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <EditorViewSwitcher value={view} onChange={setView} />
        <div className="flex items-center gap-1">
          {!focusMode && (
            <button
              type="button"
              onClick={() => setVersionsOpen(true)}
              className="inline-flex items-center gap-1 rounded-md border border-border bg-surface px-2.5 py-1 text-xs text-ink-muted hover:border-accent/40 hover:bg-surface-subtle hover:text-ink"
            >
              <History size={12} />
              Versions
              <span className="ml-1 rounded-full bg-surface-subtle px-1.5 text-[10px] font-medium text-ink-dim">
                {versions.length}
              </span>
            </button>
          )}
          <button
            type="button"
            onClick={() => setFocusMode((v) => !v)}
            aria-pressed={focusMode}
            className="inline-flex items-center gap-1 rounded-md border border-border bg-surface px-2.5 py-1 text-xs text-ink-muted hover:border-accent/40 hover:bg-surface-subtle hover:text-ink"
            title={focusMode ? "Exit focus mode" : "Enter focus mode"}
          >
            {focusMode ? <Minimize2 size={12} /> : <Maximize2 size={12} />}
            {focusMode ? "Exit focus" : "Focus mode"}
          </button>
        </div>
      </div>

      {/* ── Toolbar (hoisted out of the canvas) ──────────────────── */}
      <div className="mb-4 border-b border-border pb-2">
        <EditorToolbar editor={editor} disabled={toolbarDisabled} />
      </div>

      {/* ── Body grid ────────────────────────────────────────────── */}
      <div
        className={cn(
          "grid gap-8",
          focusMode
            ? "grid-cols-1"
            : "lg:grid-cols-[180px_minmax(0,1fr)_280px]",
        )}
      >
        {/* Outline */}
        {!focusMode && (
          <div className="order-2 lg:order-1">
            <PageOutline
              markdown={markdown}
              scrollRoot={surfaceRef.current}
            />
          </div>
        )}

        {/* Canvas */}
        <div className={cn("order-1 min-w-0", focusMode ? "" : "lg:order-2")}>
          <PageEditor
            initialMarkdown={page.markdown_body}
            onChange={setMarkdown}
            view={view}
            surfaceRef={surfaceRef}
            onEditorReady={setEditor}
          />
          <EditorFooter
            markdown={markdown}
            assetCount={page.assets.length}
            modelUsed={page.model_used}
          />
        </div>

        {/* Rail */}
        {!focusMode && (
          <aside className="order-3 divide-y divide-border lg:order-3">
            <RailBlock title="Assets">
              {page.assets.length === 0 ? (
                <p className="text-[11px] text-ink-dim">No assets attached.</p>
              ) : (
                <div className="-mx-1 space-y-0.5">
                  {page.assets.map((a, i) => (
                    <AssetChip key={`${a.kind}-${a.ref}-${i}`} asset={a} />
                  ))}
                </div>
              )}
            </RailBlock>
            <RailBlock title="Sources">
              <SourceAttacher
                pageId={page.id}
                sources={sources}
                onChange={setSources}
              />
            </RailBlock>
            <RailBlock title="Actions">
              <div className="space-y-2">
                <RegeneratePopover
                  pending={regen.isPending}
                  onSubmit={runRegenerate}
                />
                <Button
                  variant="danger"
                  onClick={runDelete}
                  loading={del.isPending}
                  leadingIcon={<Trash2 size={13} />}
                  fullWidth
                >
                  Delete
                </Button>
              </div>
            </RailBlock>
          </aside>
        )}
      </div>

      <VersionsDrawer
        open={versionsOpen}
        onClose={() => setVersionsOpen(false)}
        versions={versions}
        currentBody={markdown}
        restoring={saving}
        onRestore={runRestore}
      />
    </div>
  );
}

function RailBlock({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section className="space-y-2 py-4 first:pt-0 last:pb-0">
      <div className="text-[10px] font-semibold uppercase tracking-wider text-ink-dim">
        {title}
      </div>
      {children}
    </section>
  );
}

function SavedIndicator({
  saving,
  lastSavedAt,
}: {
  saving: boolean;
  lastSavedAt: Date | null;
}) {
  if (saving) {
    return (
      <span className="inline-flex items-center gap-1 text-[11px] text-ink-dim">
        <Loader2 size={11} className="animate-spin" />
        Saving...
      </span>
    );
  }
  if (!lastSavedAt) {
    return <span className="text-[11px] text-ink-dim">Not saved</span>;
  }
  return (
    <span className="text-[11px] text-ink-dim">
      Saved {lastSavedAt.toLocaleTimeString()}
    </span>
  );
}

function SkeletonShell() {
  return (
    <div className="space-y-4">
      <div className="h-3 w-24 animate-pulse rounded bg-surface-subtle" />
      <div className="h-9 w-1/2 animate-pulse rounded bg-surface-subtle" />
      <div className="h-7 w-40 animate-pulse rounded bg-surface-subtle" />
      <div className="grid gap-8 lg:grid-cols-[180px_minmax(0,1fr)_280px]">
        <div className="h-64 animate-pulse rounded-md bg-surface-subtle" />
        <div className="h-96 animate-pulse rounded-md bg-surface-subtle" />
        <div className="h-64 animate-pulse rounded-md bg-surface-subtle" />
      </div>
    </div>
  );
}
