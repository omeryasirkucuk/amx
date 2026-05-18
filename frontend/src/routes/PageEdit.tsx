// Edit view for a single Documentation Page.
// Header carries the editable title + status + last-saved indicator;
// the body shows the markdown editor; the rail exposes assets,
// sources, re-generate, export, and delete.

import { useEffect, useRef, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { Loader2, RefreshCw, Trash2 } from "lucide-react";

import PageHeader from "../components/PageHeader";
import PageEditor from "../components/pages/PageEditor";
import SourceAttacher from "../components/pages/SourceAttacher";
import PageExportMenu from "../components/pages/PageExportMenu";
import { Badge, Button, useToast } from "../components/ui";
import type { BadgeTone } from "../components/ui";
import {
  useDeletePage,
  useGeneratePage,
  usePage,
  useSavePage,
  type PageSource,
} from "../hooks/usePages";

const AUTOSAVE_DELAY_MS = 5000;

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
  const initRef = useRef(false);
  const autosaveTimer = useRef<number | null>(null);

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

  async function runSave() {
    if (!pageId) return;
    setSaving(true);
    try {
      const updated = await save.mutateAsync({
        title: title.trim() || "Untitled",
        markdown_body: markdown,
      });
      setLastSavedAt(new Date(updated.updated_at));
    } catch (e) {
      toast.push({ title: (e as Error).message, tone: "error" });
    } finally {
      setSaving(false);
    }
  }

  async function runRegenerate() {
    if (!pageId) return;
    try {
      const updated = await regen.mutateAsync();
      setMarkdown(updated.markdown_body);
      setTitle(updated.title);
      setLastSavedAt(new Date(updated.updated_at));
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

  if (detail.isLoading) {
    return (
      <div className="mx-auto w-full max-w-6xl px-4 py-6 text-sm text-ink-dim">
        Loading page...
      </div>
    );
  }
  if (detail.error || !detail.data) {
    return (
      <div className="mx-auto w-full max-w-6xl px-4 py-6 text-sm text-critical">
        {(detail.error as Error)?.message ?? "Page not found."}
      </div>
    );
  }

  const page = detail.data;

  return (
    <div className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6">
      <PageHeader
        breadcrumbs={[
          { label: "Pages", to: "/pages" },
          { label: page.title || "Untitled" },
        ]}
        title={
          <input
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            placeholder="Untitled"
            aria-label="Page title"
            className="w-full min-w-0 bg-transparent text-[22px] font-semibold tracking-tight text-ink outline-none focus:border-b focus:border-accent/40"
          />
        }
        actions={
          <div className="flex flex-wrap items-center gap-2">
            <Badge tone={STATUS_TONE[page.status] ?? "neutral"}>
              {page.status}
            </Badge>
            <SavedIndicator saving={saving} lastSavedAt={lastSavedAt} />
            <PageExportMenu pageId={page.id} pageTitle={title} />
          </div>
        }
      />
      <div className="flex flex-col gap-6 lg:flex-row">
        <div className="min-w-0 flex-1">
          <PageEditor
            initialMarkdown={page.markdown_body}
            onChange={setMarkdown}
          />
        </div>
        <aside className="w-full lg:w-72 lg:shrink-0 space-y-4">
          <Section title="Assets">
            {page.assets.length === 0 ? (
              <div className="text-xs text-ink-dim">No assets attached.</div>
            ) : (
              <ul className="space-y-1">
                {page.assets.map((a, i) => (
                  <li
                    key={`${a.kind}-${a.ref}-${i}`}
                    className="rounded border border-border bg-surface px-2 py-1 text-xs"
                  >
                    <div className="font-mono text-ink">{a.ref}</div>
                    <div className="text-[10px] uppercase tracking-wide text-ink-dim">
                      {a.kind}
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </Section>
          <Section title="Sources">
            <SourceAttacher
              pageId={page.id}
              sources={sources}
              onChange={setSources}
            />
          </Section>
          <Section title="Actions">
            <div className="flex flex-col gap-2">
              <Button
                variant="secondary"
                onClick={runRegenerate}
                loading={regen.isPending}
                leadingIcon={<RefreshCw size={13} />}
                fullWidth
              >
                Re-generate
              </Button>
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
          </Section>
        </aside>
      </div>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="rounded-md border border-border bg-surface p-3">
      <div className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-ink-dim">
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
