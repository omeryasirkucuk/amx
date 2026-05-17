/**
 * LineageTracePanel — v4 S4 column trace inspector.
 *
 * Mirrors datapav's "Tracing column X" panel. Renders the ordered
 * ORIGIN → HERE chain returned by GET /api/lineage/trace, with a
 * click-step affordance that asks the parent to focus the matching
 * node in the canvas.
 */

import { X } from "lucide-react";
import { useQuery } from "@tanstack/react-query";

import { lineageTrace, type LineageTraceStep } from "../../lib/api";

interface Props {
  profile: string;
  anchorPath: string;
  /** Column the user clicked on the canvas. Empty hides the panel. */
  column: string;
  direction?: "upstream" | "downstream";
  /** Called when the user clicks a step row. The parent typically
   *  invokes LineageCanvas.focusNode and updates the traced-column
   *  highlight. */
  onStepClick?: (step: LineageTraceStep) => void;
  onClose: () => void;
}

const KIND_LABEL: Record<string, string> = {
  filter: "WHERE",
  join: "JOIN",
  aggregate: "GROUP",
  function: "ƒ()",
  projection: "AS",
};

export function LineageTracePanel({
  profile,
  anchorPath,
  column,
  direction = "upstream",
  onStepClick,
  onClose,
}: Props) {
  const trace = useQuery({
    queryKey: ["lineage-trace", profile, anchorPath, column, direction],
    queryFn: () =>
      lineageTrace({
        profile,
        anchorPath,
        column,
        direction,
      }),
    enabled: Boolean(profile && anchorPath && column),
  });

  return (
    <aside className="flex h-full w-72 shrink-0 flex-col border-l border-surface-border bg-surface-raised">
      <div className="flex items-center justify-between border-b border-surface-border px-3 py-2">
        <div className="flex flex-col">
          <span className="text-[10px] uppercase tracking-wide text-fg-muted">
            Tracing column
          </span>
          <span className="truncate font-mono text-sm">{column}</span>
        </div>
        <button
          type="button"
          onClick={onClose}
          className="rounded p-1 text-fg-muted hover:bg-surface-muted"
          aria-label="Close trace panel"
        >
          <X className="h-4 w-4" />
        </button>
      </div>

      <div className="border-b border-surface-border px-3 py-1 text-[10px] uppercase tracking-wide text-fg-muted">
        Origin
      </div>

      <div className="flex-1 overflow-y-auto">
        {trace.isLoading && (
          <div className="p-3 text-xs text-fg-muted">Loading trace…</div>
        )}
        {trace.error && (
          <div className="p-3 text-xs text-critical">
            {(trace.error as Error).message}
          </div>
        )}
        {!trace.isLoading && !trace.error && trace.data && trace.data.steps.length === 0 && (
          <div className="p-3 text-xs text-fg-muted">
            No upstream column edges for {column}.
          </div>
        )}
        <ul className="divide-y divide-surface-border">
          {(trace.data?.steps ?? []).slice().reverse().map((step) => (
            <li key={`${step.entity_id}:${step.column}:${step.step}`}>
              <button
                type="button"
                onClick={() => onStepClick?.(step)}
                className="flex w-full items-baseline gap-2 px-3 py-2 text-left text-xs font-mono hover:bg-surface-muted"
              >
                <span className="w-5 shrink-0 text-fg-muted">{step.step}</span>
                {step.kind === "operator" && step.operator ? (
                  <>
                    <span className="rounded bg-emerald-900/40 px-1 py-px text-[9px] uppercase tracking-wider text-emerald-200">
                      {KIND_LABEL[step.operator.op_kind] ?? step.operator.op_kind}
                    </span>
                    <span className="truncate">{step.operator.expression}</span>
                  </>
                ) : (
                  <span className="truncate">{step.fqn}</span>
                )}
              </button>
            </li>
          ))}
        </ul>
      </div>

      <div className="border-t border-surface-border px-3 py-1 text-[10px] uppercase tracking-wide text-fg-muted">
        Here
        {trace.data?.truncated && (
          <span className="ml-2 text-amber-400" title="Path truncated at max depth">
            ⚠ truncated
          </span>
        )}
      </div>
      <div className="border-t border-surface-border bg-amber-900/20 px-3 py-2 font-mono text-xs text-amber-100">
        {column}
      </div>
    </aside>
  );
}
