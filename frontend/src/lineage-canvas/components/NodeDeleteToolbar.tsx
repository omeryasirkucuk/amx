/**
 * Floating "delete this node" button anchored above any node.
 *
 * Wraps ReactFlow's :class:`NodeToolbar` so the affordance only
 * renders when the node is selected and tracks pan / zoom for free.
 * Shared by DataFrameNode, OperatorNode, FilterNode, and LogoNode —
 * the canvas-wide rule is "click → trash above the node" for every
 * node type that doesn't already have its own toolbar (text labels
 * already ship a richer floating toolbar with their own Delete).
 *
 * Backspace / Delete on a selected node still works through
 * ReactFlow's default ``deleteKeyCode`` wiring; this button is the
 * mouse-driven equivalent for users who didn't discover the shortcut.
 */

import { NodeToolbar, Position, useReactFlow } from "reactflow";
import { Trash2 } from "lucide-react";

interface Props {
  nodeId: string;
  visible: boolean;
}

export function NodeDeleteToolbar({ nodeId, visible }: Props) {
  const rf = useReactFlow();
  return (
    <NodeToolbar isVisible={visible} position={Position.Top} offset={6}>
      <button
        type="button"
        onMouseDown={(e) => e.preventDefault()}
        onClick={(e) => {
          e.stopPropagation();
          rf.deleteElements({ nodes: [{ id: nodeId }] });
        }}
        title="Delete (Backspace)"
        className="inline-flex h-6 w-6 items-center justify-center rounded-md border border-surface-border bg-surface-raised text-fg-muted shadow-lg transition hover:bg-critical-soft hover:text-critical"
      >
        <Trash2 size={12} />
      </button>
    </NodeToolbar>
  );
}
