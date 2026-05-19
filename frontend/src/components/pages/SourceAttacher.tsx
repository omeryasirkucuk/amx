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
    <div className="space-y-2">
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
          "flex h-24 cursor-pointer items-center justify-center gap-2 rounded-md border border-dashed px-3 text-center transition-colors",
          dragOver
            ? "border-accent bg-accent-soft/40"
            : "border-border bg-surface hover:border-accent/40 hover:bg-surface-subtle",
        )}
      >
        {upload.isPending ? (
          <Loader2 size={14} className="animate-spin text-ink-dim" />
        ) : (
          <Upload size={14} className="text-ink-dim" />
        )}
        <div className="text-left">
          <div className="text-xs font-medium text-ink">
            {upload.isPending ? "Uploading..." : "Drop files or click to browse"}
          </div>
          <div className="text-[10px] text-ink-dim">
            .xlsx · .pdf · .docx · .md · .csv · .json · .yaml · .html · .eml
          </div>
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
      {error && <div className="text-[11px] text-critical">{error}</div>}
      {sources.length === 0 ? (
        <p className="text-[11px] text-ink-dim">No sources attached yet.</p>
      ) : (
        <ul className="space-y-1">
          {sources.map((s, i) => (
            <li
              key={`${s.path}-${i}`}
              className="flex items-center gap-2 rounded px-1 py-1 text-sm hover:bg-surface-subtle"
            >
              <FileIcon size={13} className="shrink-0 text-ink-dim" />
              <span className="min-w-0 flex-1 truncate text-xs text-ink" title={s.original_name}>
                {s.original_name}
              </span>
              <button
                type="button"
                onClick={() => removeAt(i)}
                aria-label={`Remove ${s.original_name}`}
                className="shrink-0 rounded p-1 text-ink-dim hover:bg-surface hover:text-critical"
              >
                <X size={12} />
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
