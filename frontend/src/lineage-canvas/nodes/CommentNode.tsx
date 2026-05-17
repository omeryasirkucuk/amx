/**
 * CommentNode — canvas annotation (two render modes).
 *
 * Both modes share the same backend table (``lineage_comments``) and
 * the same React Flow data shape; only visuals differ.
 *
 *   - ``data.style === "note"`` (default) — resizable sticky note with
 *     a colored background, header band, color-picker swatch.
 *     ``data.text`` stays plain text in this mode.
 *
 *   - ``data.style === "text"`` — minimal rich-text label. Uses a
 *     ``contentEditable`` div so users can apply per-selection
 *     formatting (font size, bold, color, bullet list). HTML lands in
 *     ``data.text`` after DOMPurify sanitisation; rendered through
 *     ``dangerouslySetInnerHTML`` on mount. The node auto-grows with
 *     its content — no fixed height, no inner scrollbar; users who
 *     want a fixed footprint can resize via :class:`NodeResizer`.
 *
 * Both modes are deletable: ReactFlow's default ``deleteKeyCode``
 * (Backspace) fires when the canvas, not the editor, holds focus.
 * The floating :class:`TextToolbar` exposes an explicit trash button
 * so deletion never depends on the user clicking outside first.
 */

import { memo, useCallback, useEffect, useRef, useState } from "react";
import { NodeProps, NodeResizer, useReactFlow } from "reactflow";
import clsx from "clsx";
import DOMPurify from "dompurify";
import { GripVertical, X } from "lucide-react";

import { TextToolbar } from "../components/TextToolbar";
import { COMMENT_COLORS } from "../constants";
import type { CommentNodeData } from "../types";

const PALETTE_ORDER: Array<keyof typeof COMMENT_COLORS> = [
  "amber",
  "rose",
  "emerald",
  "sky",
  "violet",
  "slate",
];

// Tight allowlist — toolbar only emits these tags + a single inline
// ``style`` attribute. We never allow event handlers, scripts,
// iframes, anchors, or class names. DOMPurify's default CSS
// sanitiser strips dangerous properties (url(), expression(), etc.);
// since the only producer of style is our own toolbar, the
// remaining surface is limited to font-size / color / font-weight.
const SANITIZE_OPTIONS = {
  ALLOWED_TAGS: ["b", "strong", "i", "em", "u", "br", "div", "span", "ul", "li", "p"],
  ALLOWED_ATTR: ["style"],
};

function sanitiseHtml(input: string): string {
  // DOMPurify's return is widened to TrustedHTML in some typings —
  // coerce to string for our `dangerouslySetInnerHTML` consumer.
  return String(DOMPurify.sanitize(input || "", SANITIZE_OPTIONS));
}

function CommentNodeImpl({ id, data, selected }: NodeProps<CommentNodeData>) {
  const rf = useReactFlow();
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [color, setColor] = useState<keyof typeof COMMENT_COLORS>(
    (data.color as keyof typeof COMMENT_COLORS) || "amber",
  );
  // Sticky text is plain — held in state so React drives the textarea.
  const [stickyText, setStickyText] = useState<string>(data.text || "");
  const palette = COMMENT_COLORS[color] ?? COMMENT_COLORS.amber;
  const style = data.style || "note";

  // Rich-text editor ref + uncontrolled initial value. Updates flow
  // OUT (oninput → setNodes) but never back IN after mount, otherwise
  // the caret would jump back to position 0 on every keystroke.
  const editorRef = useRef<HTMLDivElement | null>(null);
  const initialRichHtml = useRef<string>(sanitiseHtml(data.text || ""));

  // Hydrate the editor's initial HTML exactly once on mount.
  useEffect(() => {
    if (style === "text" && editorRef.current && initialRichHtml.current) {
      editorRef.current.innerHTML = initialRichHtml.current;
    }
    // No deps — we explicitly want this to run only on mount.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const commitRichHtml = useCallback(() => {
    const html = sanitiseHtml(editorRef.current?.innerHTML || "");
    rf.setNodes((nodes) =>
      nodes.map((n) =>
        n.id === id ? { ...n, data: { ...n.data, text: html } } : n,
      ),
    );
  }, [id, rf]);

  const commitStickyText = useCallback(
    (next: string) => {
      setStickyText(next);
      rf.setNodes((nodes) =>
        nodes.map((n) =>
          n.id === id ? { ...n, data: { ...n.data, text: next } } : n,
        ),
      );
    },
    [id, rf],
  );

  function commitColor(next: keyof typeof COMMENT_COLORS) {
    setColor(next);
    setPaletteOpen(false);
    rf.setNodes((nodes) =>
      nodes.map((n) =>
        n.id === id ? { ...n, data: { ...n.data, color: next } } : n,
      ),
    );
  }

  const deleteSelf = useCallback(() => {
    rf.deleteElements({ nodes: [{ id }] });
  }, [id, rf]);

  // ── Text (rich) ─────────────────────────────────────────────────────
  if (style === "text") {
    return (
      <div
        className={clsx(
          "lcv-text-node group relative h-full w-full rounded-md",
          selected
            ? "ring-1 ring-accent-default/70 ring-offset-1 ring-offset-bg"
            : "ring-1 ring-transparent",
        )}
        style={{ minWidth: 100, minHeight: 36 }}
      >
        <NodeResizer
          isVisible={selected}
          minWidth={100}
          minHeight={36}
          lineStyle={{ borderColor: "rgb(var(--accent))" }}
        />
        <TextToolbar
          visible={!!selected}
          editorRef={editorRef}
          onChange={commitRichHtml}
          onDelete={deleteSelf}
        />

        {/* Mini header — drag handle on the left, trash on the right.
            Idle: invisible. Hover or selected: fades in so the user
            always has a tactile way to move or delete the node
            without first clicking-outside-to-deselect. */}
        <div
          className={clsx(
            "lcv-text-topbar pointer-events-none absolute inset-x-0 top-0",
            "flex h-4 items-center justify-between px-1",
            "opacity-0 transition-opacity duration-150",
            "group-hover:opacity-100",
            selected && "opacity-100",
          )}
        >
          <span
            className="lcv-comment-grip pointer-events-auto inline-flex h-4 cursor-grab items-center text-fg-muted hover:text-ink"
            title="Drag to move"
          >
            <GripVertical size={10} />
          </span>
          <button
            type="button"
            onMouseDown={(e) => e.preventDefault()}
            onClick={deleteSelf}
            className="nodrag pointer-events-auto inline-flex h-4 w-4 items-center justify-center rounded text-fg-muted hover:bg-critical-soft hover:text-critical"
            title="Delete (Backspace)"
          >
            <X size={11} />
          </button>
        </div>

        <div
          ref={editorRef}
          contentEditable
          suppressContentEditableWarning
          spellCheck={false}
          onInput={commitRichHtml}
          data-placeholder="Text…"
          className={clsx(
            "lcv-text-editor nodrag nowheel",
            "block min-h-[28px] w-full cursor-text bg-transparent px-2 pb-1 pt-3 leading-snug text-ink outline-none",
            "text-[15px] font-medium",
          )}
          style={{
            // Auto-grow with content — no scroll inside the node.
            whiteSpace: "pre-wrap",
            overflowWrap: "anywhere",
            wordBreak: "break-word",
          }}
        />
      </div>
    );
  }

  // ── Note (sticky) ───────────────────────────────────────────────────
  return (
    <div
      className={clsx(
        "relative h-full w-full overflow-hidden rounded-lg shadow-xl",
        selected ? "ring-2 ring-accent-default" : "",
      )}
      style={{
        background: palette.bg,
        border: `2px solid ${palette.border}`,
      }}
    >
      <NodeResizer
        isVisible={selected}
        minWidth={140}
        minHeight={100}
        lineStyle={{ borderColor: palette.border }}
      />
      <div className="lcv-comment-grip flex h-6 items-center justify-between border-b border-white/10 bg-white/5 px-2 text-[10px] text-ink-muted">
        <span className="cursor-grab select-none uppercase tracking-wide">note</span>
        <button
          type="button"
          aria-label="Change color"
          className="nodrag h-3.5 w-3.5 rounded-full border border-white/30"
          style={{ background: palette.border }}
          onClick={(e) => {
            e.stopPropagation();
            setPaletteOpen((v) => !v);
          }}
        />
      </div>
      {paletteOpen && (
        <div
          className="nodrag absolute right-1 top-7 z-10 flex gap-1 rounded border border-surface-border bg-surface-raised p-1 shadow-lg"
          onClick={(e) => e.stopPropagation()}
        >
          {PALETTE_ORDER.map((key) => {
            const p = COMMENT_COLORS[key];
            return (
              <button
                key={key}
                type="button"
                title={key}
                onClick={() => commitColor(key)}
                className="h-4 w-4 rounded-full border border-white/30 transition hover:scale-110"
                style={{ background: p.border }}
              />
            );
          })}
        </div>
      )}
      <textarea
        value={stickyText}
        onChange={(e) => commitStickyText(e.target.value)}
        spellCheck={false}
        placeholder="Note…"
        className="nodrag nowheel absolute inset-x-0 bottom-0 top-6 w-full resize-none border-0 bg-transparent px-2 py-1.5 text-[12px] leading-snug text-ink outline-none placeholder:text-ink-dim"
      />
    </div>
  );
}

export const CommentNode = memo(CommentNodeImpl);
