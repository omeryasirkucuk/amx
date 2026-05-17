/**
 * PNG export for the canvas — runs html-to-image against the
 * React Flow viewport, then triggers a download.
 */

import { useCallback } from "react";
import { toPng } from "html-to-image";

export function usePngExport() {
  return useCallback(async (root: HTMLElement, filename = "lineage.png") => {
    const viewport = root.querySelector<HTMLElement>(".react-flow__viewport");
    const target = viewport || root;
    const dataUrl = await toPng(target, {
      cacheBust: true,
      backgroundColor: getComputedStyle(root).backgroundColor || "#0f0f0e",
      pixelRatio: 2,
    });
    const a = document.createElement("a");
    a.setAttribute("download", filename);
    a.setAttribute("href", dataUrl);
    a.click();
  }, []);
}
