// Download menu for a Documentation Page: Markdown / PDF.
// Fetches the export endpoint as a blob and triggers a browser download
// so the suggested filename from Content-Disposition is honoured.

import { useEffect, useRef, useState } from "react";
import { ChevronDown, Download, FileText, FileType2 } from "lucide-react";

import { getStoredToken } from "../../lib/auth";
import { cn } from "../../lib/cn";

interface Props {
  pageId: string;
  pageTitle: string;
}

type ExportKind = "md" | "pdf";

export default function PageExportMenu({ pageId, pageTitle }: Props) {
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState<ExportKind | null>(null);
  const wrapRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    function onDoc(e: MouseEvent) {
      if (!wrapRef.current?.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, []);

  async function download(kind: ExportKind) {
    setBusy(kind);
    try {
      const token = getStoredToken();
      const headers: Record<string, string> = {};
      if (token) headers.Authorization = `Bearer ${token}`;
      const res = await fetch(
        `/api/pages/${encodeURIComponent(pageId)}/export/${kind}`,
        { headers },
      );
      if (!res.ok) throw new Error(`Export failed: ${res.status}`);
      const blob = await res.blob();
      const cd = res.headers.get("Content-Disposition") || "";
      const match = cd.match(/filename="?([^"]+)"?/i);
      const safeTitle = sanitizeFilename(pageTitle);
      const fallback = `${safeTitle || "page"}.${kind}`;
      const filename = match?.[1] ?? fallback;
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } finally {
      setBusy(null);
      setOpen(false);
    }
  }

  return (
    <div ref={wrapRef} className="relative inline-block">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        aria-haspopup="menu"
        aria-expanded={open}
        className="inline-flex items-center gap-1.5 rounded-md border border-border bg-surface px-3 py-1.5 text-sm text-ink-muted hover:bg-surface-subtle hover:text-ink"
      >
        <Download size={14} />
        Export
        <ChevronDown size={12} />
      </button>
      {open && (
        <div
          role="menu"
          className="absolute right-0 top-9 z-10 w-44 rounded-md border border-border bg-surface-raised p-1 shadow-md"
        >
          <MenuItem
            icon={<FileText size={13} />}
            label="Markdown (.md)"
            onClick={() => download("md")}
            busy={busy === "md"}
          />
          <MenuItem
            icon={<FileType2 size={13} />}
            label="PDF (.pdf)"
            onClick={() => download("pdf")}
            busy={busy === "pdf"}
          />
        </div>
      )}
    </div>
  );
}

function MenuItem({
  icon,
  label,
  onClick,
  busy,
}: {
  icon: React.ReactNode;
  label: string;
  onClick: () => void;
  busy: boolean;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={busy}
      role="menuitem"
      className={cn(
        "flex w-full items-center gap-2 rounded px-2 py-1.5 text-left text-sm text-ink hover:bg-surface-subtle",
        busy && "cursor-wait opacity-60",
      )}
    >
      {icon}
      <span className="flex-1">{label}</span>
      {busy && <span className="text-[10px] text-ink-dim">...</span>}
    </button>
  );
}

function sanitizeFilename(name: string): string {
  return name.replace(/[^A-Za-z0-9_.-]+/g, "_").slice(0, 80);
}
