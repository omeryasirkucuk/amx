// Status bar pinned under the editor canvas.
// Surfaces the metrics that matter while you write — word count,
// rough reading time, attached-asset count, and the model that
// produced the last generation — so the user can sanity-check the
// page without scrolling through the rail.

import { useMemo } from "react";
import { Clock, FileText, Cpu, Layers } from "lucide-react";

interface Props {
  markdown: string;
  assetCount: number;
  modelUsed: string | null;
}

function wordCount(markdown: string): number {
  if (!markdown.trim()) return 0;
  // Strip code fences and inline code so we count prose words only.
  const stripped = markdown
    .replace(/```[\s\S]*?```/g, " ")
    .replace(/`[^`]*`/g, " ")
    .replace(/[#>*_~`|\-]/g, " ");
  const matches = stripped.match(/\b[\w'-]+\b/g);
  return matches ? matches.length : 0;
}

function readingTime(words: number): string {
  if (words === 0) return "0 min";
  const minutes = Math.max(1, Math.round(words / 220));
  return `${minutes} min`;
}

export default function EditorFooter({ markdown, assetCount, modelUsed }: Props) {
  const words = useMemo(() => wordCount(markdown), [markdown]);
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 border-t border-border px-1 pt-2 text-[11px] text-ink-dim">
      <span className="inline-flex items-center gap-1">
        <FileText size={11} />
        {words.toLocaleString()} words
      </span>
      <span className="inline-flex items-center gap-1">
        <Clock size={11} />
        {readingTime(words)}
      </span>
      <span className="inline-flex items-center gap-1">
        <Layers size={11} />
        {assetCount} {assetCount === 1 ? "asset" : "assets"}
      </span>
      {modelUsed && (
        <span className="inline-flex items-center gap-1">
          <Cpu size={11} />
          {modelUsed}
        </span>
      )}
    </div>
  );
}
