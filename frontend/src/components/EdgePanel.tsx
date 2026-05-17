/**
 * EdgePanel — shows provenance + confidence + evidence for a single
 * lineage edge selected on the canvas. Sits as a right-side rail in
 * the Lineage detail view.
 */

import type { LineageEdge } from "../lib/api";

interface Props {
  edge: LineageEdge | null;
}

const TYPE_LABEL: Record<string, string> = {
  lineage_fk: "Foreign key (deterministic)",
  lineage_view_ddl: "View DDL (deterministic)",
  lineage_query_log: "Query history (deterministic)",
  lineage_co_occurs: "Query co-occurrence (heuristic)",
  lineage_llm: "AI suggestion (LLM-inferred)",
  lineage_name_match: "Name + type heuristic",
  lineage_manual: "Manual edge (user-authored)",
};

const TYPE_DOT: Record<string, string> = {
  lineage_fk: "bg-fg-default",
  lineage_view_ddl: "bg-blue-600",
  lineage_query_log: "bg-emerald-600",
  lineage_co_occurs: "bg-neutral-500",
  lineage_llm: "bg-purple-600",
  lineage_name_match: "bg-neutral-400",
  lineage_manual: "bg-fg-default",
};

export function EdgePanel({ edge }: Props) {
  if (!edge) {
    return (
      <aside className="flex h-full flex-col gap-3 rounded-xl border border-surface-border bg-surface-raised p-4 text-sm text-fg-muted">
        <p className="font-medium text-fg-default">Edge inspector</p>
        <p>Click an edge on the canvas to see its provenance, confidence and reasoning.</p>
      </aside>
    );
  }

  const typeLabel = TYPE_LABEL[edge.type] ?? edge.type;
  const dotColor = TYPE_DOT[edge.type] ?? "bg-neutral-500";
  const conf = (edge.confidence * 100).toFixed(0);

  return (
    <aside className="flex h-full flex-col gap-3 rounded-xl border border-surface-border bg-surface-raised p-4 text-sm">
      <div className="flex items-center gap-2">
        <span className={`h-2.5 w-2.5 rounded-full ${dotColor}`} />
        <span className="font-medium text-fg-default">{typeLabel}</span>
      </div>

      <dl className="space-y-2">
        <div>
          <dt className="text-xs uppercase tracking-wide text-fg-muted">Source</dt>
          <dd className="font-mono text-xs break-all">{edge.from}</dd>
        </div>
        <div>
          <dt className="text-xs uppercase tracking-wide text-fg-muted">Target</dt>
          <dd className="font-mono text-xs break-all">{edge.to}</dd>
        </div>
        <div>
          <dt className="text-xs uppercase tracking-wide text-fg-muted">Extractor</dt>
          <dd className="font-mono text-xs">{edge.extractor}</dd>
        </div>
        <div>
          <dt className="text-xs uppercase tracking-wide text-fg-muted">Confidence</dt>
          <dd>
            <div className="mt-1 h-2 w-full rounded-full bg-surface-muted">
              <div
                className="h-2 rounded-full bg-fg-default"
                style={{ width: `${Math.min(100, Math.max(0, conf as unknown as number))}%` }}
              />
            </div>
            <span className="mt-1 inline-block text-xs text-fg-muted">{conf}%</span>
          </dd>
        </div>
        {edge.evidence && (
          <div>
            <dt className="text-xs uppercase tracking-wide text-fg-muted">Evidence</dt>
            <dd className="whitespace-pre-wrap text-xs text-fg-default">{edge.evidence}</dd>
          </div>
        )}
      </dl>
    </aside>
  );
}
