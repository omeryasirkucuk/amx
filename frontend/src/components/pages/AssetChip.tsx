// Visual card for a single attached asset in the rail.
// Replaces the bare <li> with a richer surface: a kind-aware icon,
// a truncated ref with a tooltip, and a copy-to-clipboard
// affordance so users can paste the ref into Slack / a code
// snippet without retyping.

import { useState } from "react";
import { Check, Copy, Database, FileText, Workflow } from "lucide-react";

import { cn } from "../../lib/cn";
import type { PageAssetRef } from "../../hooks/usePages";

interface Props {
  asset: PageAssetRef;
}

function iconFor(kind: string) {
  if (kind === "doc_profile") return FileText;
  if (kind === "lineage_artifact") return Workflow;
  return Database; // every db_* kind shares the database icon
}

function labelFor(kind: string): string {
  if (kind === "doc_profile") return "docs";
  if (kind === "lineage_artifact") return "lineage";
  if (kind === "db_column") return "column";
  if (kind === "db_table") return "table";
  if (kind === "db_schema") return "schema";
  if (kind === "db_database") return "database";
  if (kind === "db_profile") return "db profile";
  return kind;
}

export default function AssetChip({ asset }: Props) {
  const [copied, setCopied] = useState(false);
  const Icon = iconFor(asset.kind);

  async function copy() {
    try {
      await navigator.clipboard.writeText(asset.ref);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    } catch {
      /* clipboard unavailable */
    }
  }

  const refText = asset.ref || "(no reference)";
  return (
    <div className="group flex items-start gap-2 rounded-md border border-border bg-surface px-2.5 py-2 transition-colors hover:border-accent/40 hover:bg-surface-subtle">
      <span className="mt-0.5 inline-flex h-6 w-6 shrink-0 items-center justify-center rounded bg-accent-soft/60 text-accent-ink">
        <Icon size={14} />
      </span>
      <div className="min-w-0 flex-1">
        <div
          className="break-all font-mono text-xs leading-tight text-ink"
          title={refText}
        >
          {refText}
        </div>
        <div className="mt-0.5 text-[10px] uppercase tracking-wide text-ink-dim">
          {labelFor(asset.kind)}
        </div>
      </div>
      <button
        type="button"
        onClick={copy}
        aria-label="Copy ref"
        title={copied ? "Copied" : "Copy ref"}
        className={cn(
          "inline-flex h-6 w-6 shrink-0 items-center justify-center rounded text-ink-muted transition hover:bg-surface hover:text-ink",
          "opacity-60 group-hover:opacity-100",
          copied && "!opacity-100 text-accent-ink",
        )}
      >
        {copied ? <Check size={12} /> : <Copy size={12} />}
      </button>
    </div>
  );
}
