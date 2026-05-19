/**
 * Spot-finding helpers so every newly-added node lands in empty
 * canvas space instead of stacking on top of whatever is already
 * there. Used by every "add" flow — Add Table picker, operator drop,
 * comment / text / logo — so the user never has to drag a fresh
 * node out from under another one.
 *
 * The algorithm is intentionally cheap: rectangle-AABB collision
 * against existing nodes, walked outward in a square spiral from the
 * supplied hint until an empty cell is found. With a few dozen
 * canvases nodes this is well under a millisecond.
 */

import type { CanvasNode } from "./types";

interface Box {
  x: number;
  y: number;
  width: number;
  height: number;
}

interface FindFreeSpotInput {
  /** Nodes already on the canvas — anything not laid out yet should
   *  be excluded by the caller. */
  existing: CanvasNode[];
  /** Where the caller would prefer the new node to land (viewport
   *  centre, last drop point, etc.). The spiral walk starts here. */
  hint: { x: number; y: number };
  /** Width / height of the node being placed. */
  size: { width: number; height: number };
  /** Empty pixels we want around every node so two adjacent nodes
   *  don't crowd each other on screen. */
  gutter?: number;
  /** Hard cap on the spiral radius so a pathological canvas can't
   *  loop forever. */
  maxRing?: number;
}

/** Pull whatever dimensions ReactFlow has on the node, falling back
 *  to generous defaults sized for the largest nodes the canvas
 *  renders (DataFrameNode + FilterNode both blow past the 240×120
 *  React Flow default). Without this defensive sizing, a freshly-
 *  added operator that ReactFlow hasn't measured yet appears as a
 *  small box to the collision check and the next-added node walks
 *  right into it. */
function boxOf(n: CanvasNode, gutter: number): Box {
  // ReactFlow v11+ stores measured dimensions on the controlled
  // state node after the first render. ``measured`` (v12) is also
  // honoured for forward compatibility.
  const measured = (n as { measured?: { width?: number; height?: number } }).measured;
  const w = (n.width ?? measured?.width) ?? 320;
  const h = (n.height ?? measured?.height) ?? 200;
  return {
    x: n.position.x - gutter / 2,
    y: n.position.y - gutter / 2,
    width: w + gutter,
    height: h + gutter,
  };
}

function overlaps(a: Box, b: Box): boolean {
  return (
    a.x < b.x + b.width &&
    a.x + a.width > b.x &&
    a.y < b.y + b.height &&
    a.y + a.height > b.y
  );
}

export function findFreeSpot({
  existing,
  hint,
  size,
  gutter = 32,
  maxRing = 24,
}: FindFreeSpotInput): { x: number; y: number } {
  const others = existing
    .filter((n) => n.position && n.data)
    .map((n) => boxOf(n, gutter));
  const candidate = (x: number, y: number): Box => ({
    x: x - gutter / 2,
    y: y - gutter / 2,
    width: size.width + gutter,
    height: size.height + gutter,
  });
  const clear = (x: number, y: number) =>
    others.every((b) => !overlaps(candidate(x, y), b));
  if (clear(hint.x, hint.y)) return hint;
  // Step ≈ a typical node footprint, so the spiral lands on
  // grid-like positions that look natural next to the existing
  // nodes rather than nudging by a few pixels at a time.
  const step = Math.max(160, Math.min(size.width, size.height) + gutter);
  for (let ring = 1; ring <= maxRing; ring += 1) {
    // Walk the perimeter of a square ring at this radius.
    for (let i = -ring; i <= ring; i += 1) {
      for (let j = -ring; j <= ring; j += 1) {
        if (Math.abs(i) !== ring && Math.abs(j) !== ring) continue;
        const x = hint.x + i * step;
        const y = hint.y + j * step;
        if (clear(x, y)) return { x, y };
      }
    }
  }
  // Last resort — drop the node well below the hint so it lands
  // somewhere reachable even if every spiral cell was taken.
  return { x: hint.x, y: hint.y + (maxRing + 1) * step };
}

/** Convert the canvas wrapper's centre into ReactFlow coordinates.
 *  ``screenToFlowPosition`` is owned by the ``useReactFlow`` instance,
 *  so the caller passes it as a function reference. */
export function viewportCenterFlowCoords(
  canvasEl: HTMLElement | null,
  screenToFlow: (p: { x: number; y: number }) => { x: number; y: number },
): { x: number; y: number } {
  if (!canvasEl) return { x: 200, y: 200 };
  const rect = canvasEl.getBoundingClientRect();
  return screenToFlow({
    x: rect.left + rect.width / 2,
    y: rect.top + rect.height / 2,
  });
}
