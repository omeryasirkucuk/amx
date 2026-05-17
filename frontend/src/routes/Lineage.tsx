/**
 * Lineage route shell — hosts the new <Canvas/> from the
 * lineage-canvas package. All sub-routes (/new, /saved, /share) and
 * the artifact deep-link (?artifact=<id>) resolve to the same canvas;
 * URL state is the only thing that differs.
 */

import LineageCanvasRoute from "../lineage-canvas/Canvas";

export default function Lineage() {
  return <LineageCanvasRoute />;
}
