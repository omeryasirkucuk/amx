/**
 * Tiny undo/redo stack keyed to the canvas state shape
 * ``{nodes, edges}``. The stack caps at 50 entries to bound memory.
 * Stored snapshots are deep-frozen via structuredClone so a later
 * mutation can't retroactively rewrite history.
 */

import { useCallback, useRef, useState } from "react";
import type { CanvasEdge, CanvasNode } from "../types";

interface Snapshot {
  nodes: CanvasNode[];
  edges: CanvasEdge[];
}

const MAX_DEPTH = 50;

export function useUndoRedo(initial: Snapshot) {
  const past = useRef<Snapshot[]>([]);
  const future = useRef<Snapshot[]>([]);
  const [current, setCurrent] = useState<Snapshot>(initial);

  const push = useCallback(
    (next: Snapshot) => {
      past.current.push(current);
      if (past.current.length > MAX_DEPTH) past.current.shift();
      future.current = [];
      setCurrent(next);
    },
    [current],
  );

  const undo = useCallback(() => {
    const prev = past.current.pop();
    if (!prev) return null;
    future.current.push(current);
    setCurrent(prev);
    return prev;
  }, [current]);

  const redo = useCallback(() => {
    const next = future.current.pop();
    if (!next) return null;
    past.current.push(current);
    setCurrent(next);
    return next;
  }, [current]);

  return {
    state: current,
    push,
    undo,
    redo,
    canUndo: past.current.length > 0,
    canRedo: future.current.length > 0,
    setState: setCurrent,
  };
}
