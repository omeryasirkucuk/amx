/**
 * Floating format toolbar for the rich-text label node.
 *
 * Anchored above the node via ReactFlow's :class:`NodeToolbar` so it
 * tracks the node's position on pan / zoom and only shows when the
 * node is selected. Buttons wrap ``document.execCommand`` (deprecated
 * but universally supported and small) so the heavy lift of selection
 * / range manipulation stays in the browser.
 *
 * Layout:  [size ▾]  B  •  [color swatches]  bullet  delete
 *
 * The parent passes ``editorRef`` so the toolbar can focus the
 * contentEditable before issuing an exec — without that, clicking a
 * toolbar button blurs the editor and the command becomes a no-op.
 */

import { RefObject, useState } from "react";
import { NodeToolbar, Position } from "reactflow";
import { Bold, List, Trash2, Type } from "lucide-react";
import clsx from "clsx";

const SIZE_OPTIONS: Array<{ label: string; px: number }> = [
  { label: "S", px: 12 },
  { label: "M", px: 15 },
  { label: "L", px: 20 },
  { label: "XL", px: 28 },
];

const COLOR_OPTIONS: string[] = [
  "rgb(245,244,242)", // ink (default)
  "#fbbf24", // amber
  "#34d399", // emerald
  "#60a5fa", // sky
  "#f472b6", // rose
  "#a78bfa", // violet
  "#f87171", // critical
  "#94a3b8", // slate
];

interface Props {
  visible: boolean;
  editorRef: RefObject<HTMLDivElement>;
  onChange: () => void;
  onDelete: () => void;
}

export function TextToolbar({ visible, editorRef, onChange, onDelete }: Props) {
  const [sizeMenu, setSizeMenu] = useState(false);
  const [colorMenu, setColorMenu] = useState(false);

  function withEditor(fn: () => void) {
    const el = editorRef.current;
    if (!el) return;
    el.focus();
    fn();
    onChange();
  }

  function applyBold() {
    withEditor(() => document.execCommand("bold"));
  }

  function applyBullet() {
    withEditor(() => document.execCommand("insertUnorderedList"));
  }

  function applyColor(color: string) {
    withEditor(() => document.execCommand("foreColor", false, color));
    setColorMenu(false);
  }

  function applySize(px: number) {
    // execCommand("fontSize") only accepts 1-7. We wrap the
    // selection in a span with an inline ``font-size`` instead so the
    // sizes match the picker labels exactly.
    withEditor(() => {
      const sel = window.getSelection();
      if (!sel || sel.rangeCount === 0 || sel.isCollapsed) {
        // No selection → apply to the whole editor as a baseline.
        if (editorRef.current) editorRef.current.style.fontSize = `${px}px`;
        return;
      }
      const range = sel.getRangeAt(0);
      const span = document.createElement("span");
      span.style.fontSize = `${px}px`;
      try {
        span.appendChild(range.extractContents());
        range.insertNode(span);
        // Restore selection across the new span so chained commands
        // keep working naturally.
        sel.removeAllRanges();
        const r = document.createRange();
        r.selectNodeContents(span);
        sel.addRange(r);
      } catch {
        /* extractContents can throw on cross-node ranges; degrade gracefully */
      }
    });
    setSizeMenu(false);
  }

  return (
    <NodeToolbar isVisible={visible} position={Position.Top} offset={8}>
      <div className="flex items-center gap-0.5 rounded-md border border-surface-border bg-surface-raised px-1 py-0.5 shadow-lg">
        <button
          type="button"
          title="Font size"
          onMouseDown={(e) => e.preventDefault()}
          onClick={() => {
            setSizeMenu((v) => !v);
            setColorMenu(false);
          }}
          className="inline-flex h-6 items-center gap-0.5 rounded px-1.5 text-[11px] text-fg-muted hover:bg-surface hover:text-ink"
        >
          <Type size={12} />
          <span>Size</span>
        </button>
        {sizeMenu && (
          <div className="absolute left-1 top-7 z-20 flex gap-0.5 rounded-md border border-surface-border bg-surface-raised p-1 shadow-xl">
            {SIZE_OPTIONS.map((s) => (
              <button
                key={s.label}
                type="button"
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => applySize(s.px)}
                className="rounded px-1.5 py-0.5 text-[11px] text-fg-muted hover:bg-surface hover:text-ink"
                style={{ fontSize: `${Math.min(s.px, 14)}px` }}
              >
                {s.label}
              </button>
            ))}
          </div>
        )}

        <button
          type="button"
          title="Bold (⌘B)"
          onMouseDown={(e) => e.preventDefault()}
          onClick={applyBold}
          className="inline-flex h-6 w-6 items-center justify-center rounded text-fg-muted hover:bg-surface hover:text-ink"
        >
          <Bold size={12} />
        </button>

        <button
          type="button"
          title="Color"
          onMouseDown={(e) => e.preventDefault()}
          onClick={() => {
            setColorMenu((v) => !v);
            setSizeMenu(false);
          }}
          className="inline-flex h-6 w-6 items-center justify-center rounded text-fg-muted hover:bg-surface hover:text-ink"
        >
          <span
            className="block h-3 w-3 rounded-sm border border-white/30"
            style={{ background: "linear-gradient(135deg,#60a5fa,#f472b6,#fbbf24)" }}
          />
        </button>
        {colorMenu && (
          <div
            className={clsx(
              "absolute left-12 top-7 z-20 flex gap-0.5 rounded-md border",
              "border-surface-border bg-surface-raised p-1 shadow-xl",
            )}
          >
            {COLOR_OPTIONS.map((c) => (
              <button
                key={c}
                type="button"
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => applyColor(c)}
                className="h-4 w-4 rounded-full border border-white/30 transition hover:scale-110"
                style={{ background: c }}
              />
            ))}
          </div>
        )}

        <button
          type="button"
          title="Bullet list"
          onMouseDown={(e) => e.preventDefault()}
          onClick={applyBullet}
          className="inline-flex h-6 w-6 items-center justify-center rounded text-fg-muted hover:bg-surface hover:text-ink"
        >
          <List size={12} />
        </button>

        <span className="mx-1 h-4 w-px bg-surface-border" />

        <button
          type="button"
          title="Delete (Backspace)"
          onMouseDown={(e) => e.preventDefault()}
          onClick={onDelete}
          className="inline-flex h-6 w-6 items-center justify-center rounded text-fg-muted hover:bg-critical-soft hover:text-critical"
        >
          <Trash2 size={12} />
        </button>
      </div>
    </NodeToolbar>
  );
}
