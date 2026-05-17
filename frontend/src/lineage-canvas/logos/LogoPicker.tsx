/**
 * LogoPicker — modal grid picker for the lineage logo registry.
 *
 * Tabs: All / Cloud / Warehouse / BI / Tooling / Custom + "Add custom".
 * Custom rows ship with a delete affordance; defaults don't.
 *
 * Two entry-point modes via the ``onPick`` callback:
 *   - From the toolbar → caller spawns a standalone LogoNode.
 *   - From a DataFrameNode header → caller sets ``data.logoKey``.
 *
 * The "Add custom" tab supports both a file upload (FileReader →
 * base64 → POST) and a URL paste. Auto-derives a slug key from the
 * label if the user doesn't supply one.
 */

import { ChangeEvent, useEffect, useMemo, useState } from "react";
import { Trash2, Upload, X } from "lucide-react";

import Modal from "../../components/Modal";
import { Button, useToast } from "../../components/ui";
import {
  pickLogoSrc,
  useAddLogoMutation,
  useDeleteLogoMutation,
  useLogosQuery,
  type LogoCategory,
  type LogoRow,
} from "./registry";

interface Props {
  open: boolean;
  onClose: () => void;
  onPick: (logo: LogoRow) => void;
  /** Optional title — defaults to "Pick a logo". */
  title?: string;
  /** Clear button shown to the right of "Cancel" — used when the caller
   *  wants to remove a previously selected logo (e.g. clear the header
   *  badge). When provided, picker also surfaces a "Remove" button. */
  onClear?: () => void;
}

type TabKey = "all" | LogoCategory;

const TABS: Array<{ key: TabKey; label: string }> = [
  { key: "all", label: "All" },
  { key: "cloud", label: "Cloud" },
  { key: "warehouse", label: "Warehouse" },
  { key: "bi", label: "BI" },
  { key: "tooling", label: "Tooling" },
  { key: "custom", label: "Custom" },
];

const MAX_BYTES = 200 * 1024;
const ALLOWED_MIMES = new Set([
  "image/svg+xml",
  "image/png",
  "image/jpeg",
  "image/jpg",
  "image/webp",
]);

function slugify(s: string): string {
  return s
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 40);
}

export function LogoPicker({ open, onClose, onPick, title, onClear }: Props) {
  const logosQ = useLogosQuery();
  const addMut = useAddLogoMutation();
  const deleteMut = useDeleteLogoMutation();
  const toast = useToast();
  const [tab, setTab] = useState<TabKey>("all");
  const [addOpen, setAddOpen] = useState(false);

  // "Add custom" inputs
  const [label, setLabel] = useState("");
  const [key, setKey] = useState("");
  const [url, setUrl] = useState("");
  const [dataUrl, setDataUrl] = useState("");

  useEffect(() => {
    if (!open) {
      setTab("all");
      setAddOpen(false);
      setLabel("");
      setKey("");
      setUrl("");
      setDataUrl("");
    }
  }, [open]);

  const rows = logosQ.data?.logos ?? [];
  const filtered = useMemo(() => {
    if (tab === "all") return rows;
    return rows.filter((r) => r.category === tab);
  }, [rows, tab]);

  async function handleFile(file: File) {
    if (!ALLOWED_MIMES.has(file.type)) {
      toast.push({
        title: "Unsupported file type",
        description: `Allowed: SVG, PNG, JPG, WebP. Got ${file.type || "unknown"}.`,
        tone: "error",
      });
      return;
    }
    if (file.size > MAX_BYTES) {
      toast.push({
        title: "File too large",
        description: `${Math.round(file.size / 1024)} KB exceeds 200 KB limit.`,
        tone: "error",
      });
      return;
    }
    const reader = new FileReader();
    reader.onload = () => {
      setDataUrl(String(reader.result || ""));
      if (!label) setLabel(file.name.replace(/\.[^.]+$/, ""));
    };
    reader.readAsDataURL(file);
  }

  async function submitCustom() {
    const finalKey = key || slugify(label);
    if (!finalKey || !label) {
      toast.push({ title: "Need label + key", tone: "warning" });
      return;
    }
    if (!dataUrl && !url) {
      toast.push({
        title: "Upload a file or paste a URL",
        tone: "warning",
      });
      return;
    }
    try {
      const created = await addMut.mutateAsync({
        key: finalKey,
        label,
        category: "custom",
        data_url: dataUrl || undefined,
        url: url || undefined,
      });
      toast.push({ title: "Custom logo added", tone: "success" });
      setAddOpen(false);
      onPick(created);
      onClose();
    } catch (e) {
      toast.push({
        title: "Add failed",
        description: (e as Error).message,
        tone: "error",
      });
    }
  }

  async function handleDelete(row: LogoRow) {
    if (row.source !== "custom") return;
    if (!window.confirm(`Delete custom logo "${row.label}"?`)) return;
    try {
      await deleteMut.mutateAsync(row.id);
      toast.push({ title: "Deleted", tone: "success" });
    } catch (e) {
      toast.push({
        title: "Delete failed",
        description: (e as Error).message,
        tone: "error",
      });
    }
  }

  return (
    <Modal
      open={open}
      onClose={onClose}
      size="md"
      title={<span>{title || "Pick a logo"}</span>}
      description="Choose from the default library or upload your own (SVG / PNG / JPG / WebP, max 200 KB)."
    >
      <div className="space-y-3 text-sm">
        <div className="flex flex-wrap items-center gap-1 border-b border-surface-border pb-2">
          {TABS.map((t) => (
            <button
              key={t.key}
              type="button"
              onClick={() => setTab(t.key)}
              className={
                "rounded px-2 py-1 text-[12px] " +
                (tab === t.key
                  ? "bg-accent-soft text-accent-ink"
                  : "text-fg-muted hover:bg-surface")
              }
            >
              {t.label}
            </button>
          ))}
          <button
            type="button"
            onClick={() => setAddOpen((v) => !v)}
            className="ml-auto inline-flex items-center gap-1 rounded border border-surface-border bg-surface px-2 py-1 text-[12px] text-ink hover:bg-surface-raised"
          >
            <Upload size={11} />
            Add custom
          </button>
        </div>

        {addOpen && (
          <div className="space-y-2 rounded-md border border-surface-border bg-surface p-2">
            <div className="grid grid-cols-2 gap-2">
              <input
                value={label}
                onChange={(e) => setLabel(e.target.value)}
                placeholder="Label (e.g. My Co)"
                className="rounded border border-surface-border bg-surface-raised px-2 py-1 text-[12px]"
              />
              <input
                value={key}
                onChange={(e) => setKey(slugify(e.target.value))}
                placeholder="Key (auto from label)"
                className="rounded border border-surface-border bg-surface-raised px-2 py-1 font-mono text-[12px]"
              />
            </div>
            <input
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              placeholder="… or paste a public URL"
              className="block w-full rounded border border-surface-border bg-surface-raised px-2 py-1 text-[12px]"
            />
            <label className="flex items-center justify-between rounded border border-dashed border-surface-border bg-surface-raised px-2 py-2 text-[12px] text-fg-muted">
              <span>{dataUrl ? "File loaded — ready to add" : "Click to choose file"}</span>
              <input
                type="file"
                hidden
                accept="image/svg+xml,image/png,image/jpeg,image/webp"
                onChange={(e: ChangeEvent<HTMLInputElement>) => {
                  const f = e.target.files?.[0];
                  if (f) handleFile(f);
                }}
              />
              <Upload size={12} />
            </label>
            <div className="flex justify-end gap-2">
              <Button variant="secondary" size="sm" onClick={() => setAddOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                size="sm"
                loading={addMut.isPending}
                onClick={submitCustom}
              >
                Add
              </Button>
            </div>
          </div>
        )}

        <div
          className="grid gap-2 overflow-y-auto"
          style={{
            maxHeight: 320,
            gridTemplateColumns: "repeat(auto-fill, minmax(96px, 1fr))",
          }}
        >
          {logosQ.isLoading ? (
            <div className="col-span-full p-4 text-center text-xs text-fg-muted">
              Loading…
            </div>
          ) : filtered.length === 0 ? (
            <div className="col-span-full p-4 text-center text-xs text-fg-muted">
              No logos in this category.
            </div>
          ) : (
            filtered.map((row) => (
              <button
                key={row.id}
                type="button"
                onClick={() => {
                  onPick(row);
                  onClose();
                }}
                className="group relative flex flex-col items-center gap-1 rounded-md border border-surface-border bg-surface p-2 hover:border-accent-default"
                title={row.label}
              >
                <img
                  src={pickLogoSrc(row)}
                  alt={row.label}
                  className="h-12 w-12 object-contain"
                  draggable={false}
                />
                <span className="w-full truncate text-center text-[10px] text-fg-muted">
                  {row.label}
                </span>
                {row.source === "custom" && (
                  <span
                    role="button"
                    tabIndex={0}
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDelete(row);
                    }}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        handleDelete(row);
                      }
                    }}
                    className="absolute right-1 top-1 rounded p-0.5 text-fg-muted opacity-0 transition group-hover:opacity-100 hover:bg-critical-soft hover:text-critical"
                    aria-label="Delete custom logo"
                  >
                    <Trash2 size={10} />
                  </span>
                )}
              </button>
            ))
          )}
        </div>

        {onClear && (
          <div className="flex justify-end border-t border-surface-border pt-2">
            <Button
              variant="secondary"
              size="sm"
              leadingIcon={<X size={12} />}
              onClick={() => {
                onClear();
                onClose();
              }}
            >
              Remove logo
            </Button>
          </div>
        )}
      </div>
    </Modal>
  );
}
