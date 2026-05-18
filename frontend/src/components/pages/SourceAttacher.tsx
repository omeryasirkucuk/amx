// Drag-and-drop source uploader for the Documentation Pages editor.
// Posts files to /api/pages/:id/sources and renders the attached list.

import { useRef, useState } from "react";
import { File as FileIcon, Loader2, Upload, X } from "lucide-react";

import { cn } from "../../lib/cn";
import { useUploadPageSource, type PageSource } from "../../hooks/usePages";

const ACCEPT_EXTENSIONS = [
  ".xlsx",
  ".eml",
  ".md",
  ".markdown",
  ".txt",
  ".pdf",
  ".docx",
  ".doc",
  ".html",
  ".htm",
  ".csv",
  ".tsv",
  ".json",
  ".yaml",
  ".yml",
  ".rst",
];

interface Props {
  pageId: string;
  sources: PageSource[];
  onChange: (next: PageSource[]) => void;
}

export default function SourceAttacher({ pageId, sources, onChange }: Props) {
  const upload = useUploadPageSource(pageId);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const [dragOver, setDragOver] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function uploadAll(files: FileList | File[]) {
    setError(null);
    const next: PageSource[] = [...sources];
    for (const file of Array.from(files)) {
      try {
        const result = await upload.mutateAsync(file);
        next.push(result);
      } catch (e) {
        setError((e as Error).message || `Upload failed for ${file.name}`);
        break;
      }
    }
    onChange(next);
  }

  function onDrop(e: React.DragEvent<HTMLDivElement>) {
    e.preventDefault();
    setDragOver(false);
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      void uploadAll(e.dataTransfer.files);
    }
  }

  function onFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    if (e.target.files && e.target.files.length > 0) {
      void uploadAll(e.target.files);
    }
    e.target.value = "";
  }

  function removeAt(index: number) {
    // No DELETE endpoint shipped yet; local removal until backend lands.
    const next = sources.filter((_, i) => i !== index);
    onChange(next);
  }

  return (
    <div className="flex flex-col gap-3 sm:flex-row sm:items-start">
      <div className="flex-1">
        <div
          onDragOver={(e) => {
            e.preventDefault();
            setDragOver(true);
          }}
          onDragLeave={() => setDragOver(false)}
          onDrop={onDrop}
          onClick={() => inputRef.current?.click()}
          role="button"
          tabIndex={0}
          onKeyDown={(e) => {
            if (e.key === "Enter" || e.key === " ") inputRef.current?.click();
          }}
          className={cn(
            "flex cursor-pointer flex-col items-center justify-center gap-2 rounded-md border-2 border-dashed p-6 text-center transition-colors",
            dragOver
              ? "border-accent bg-accent-soft/40"
              : "border-border bg-surface-subtle hover:border-accent/40",
          )}
        >
          {upload.isPending ? (
            <Loader2 size={20} className="animate-spin text-ink-dim" />
          ) : (
            <Upload size={20} className="text-ink-dim" />
          )}
          <div className="text-sm font-medium text-ink">
            {upload.isPending
              ? "Uploading..."
              : "Drop files here or click to browse"}
          </div>
          <div className="text-[11px] text-ink-dim">
            Supports .xlsx, .pdf, .docx, .md, .csv, .json, .yaml, .html, .eml, ...
          </div>
        </div>
        <input
          ref={inputRef}
          type="file"
          multiple
          accept={ACCEPT_EXTENSIONS.join(",")}
          onChange={onFileChange}
          className="hidden"
        />
        {error && (
          <div className="mt-2 text-xs text-critical">{error}</div>
        )}
      </div>
      <div className="flex-1 min-w-0">
        <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-ink-dim">
          Attached sources ({sources.length})
        </div>
        {sources.length === 0 ? (
          <div className="rounded-md border border-dashed border-border bg-surface px-3 py-4 text-center text-xs text-ink-dim">
            No sources attached yet.
          </div>
        ) : (
          <ul className="space-y-1">
            {sources.map((s, i) => (
              <li
                key={`${s.path}-${i}`}
                className="flex items-center gap-2 rounded-md border border-border bg-surface px-2 py-1.5 text-sm"
              >
                <FileIcon size={13} className="shrink-0 text-ink-dim" />
                <span className="min-w-0 flex-1 truncate" title={s.original_name}>
                  {s.original_name}
                </span>
                <span className="shrink-0 rounded bg-surface-subtle px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-ink-dim">
                  {s.kind}
                </span>
                <button
                  type="button"
                  onClick={() => removeAt(i)}
                  aria-label={`Remove ${s.original_name}`}
                  className="shrink-0 rounded p-1 text-ink-dim hover:bg-surface-subtle hover:text-critical"
                >
                  <X size={12} />
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
