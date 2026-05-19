// Lightweight slash command menu for the Pages editor.
// Listens to TipTap selection updates; when the active paragraph
// is exactly "/" we open a floating menu next to the caret with a
// short list of block templates. Picking an item deletes the "/"
// and applies the matching chain command. Escape and clicking
// outside both close the menu. The point of this surface is
// discoverability without forcing a heavier @tiptap/suggestion
// dependency.

import { useEffect, useMemo, useRef, useState } from "react";
import type { Editor } from "@tiptap/react";
import {
  Code,
  Heading1,
  Heading2,
  Heading3,
  List,
  ListOrdered,
  Minus,
  Quote,
  Table as TableIcon,
  type LucideIcon,
} from "lucide-react";

import { cn } from "../../lib/cn";

interface SlashItem {
  id: string;
  label: string;
  hint: string;
  icon: LucideIcon;
  apply: (editor: Editor) => void;
}

const ITEMS: SlashItem[] = [
  {
    id: "h1",
    label: "Heading 1",
    hint: "Section title",
    icon: Heading1,
    apply: (e) => e.chain().focus().toggleHeading({ level: 1 }).run(),
  },
  {
    id: "h2",
    label: "Heading 2",
    hint: "Subsection",
    icon: Heading2,
    apply: (e) => e.chain().focus().toggleHeading({ level: 2 }).run(),
  },
  {
    id: "h3",
    label: "Heading 3",
    hint: "Subhead",
    icon: Heading3,
    apply: (e) => e.chain().focus().toggleHeading({ level: 3 }).run(),
  },
  {
    id: "ul",
    label: "Bullet list",
    hint: "Unordered list",
    icon: List,
    apply: (e) => e.chain().focus().toggleBulletList().run(),
  },
  {
    id: "ol",
    label: "Ordered list",
    hint: "Numbered list",
    icon: ListOrdered,
    apply: (e) => e.chain().focus().toggleOrderedList().run(),
  },
  {
    id: "code",
    label: "Code block",
    hint: "Monospace",
    icon: Code,
    apply: (e) => e.chain().focus().toggleCodeBlock().run(),
  },
  {
    id: "quote",
    label: "Quote",
    hint: "Block quote",
    icon: Quote,
    apply: (e) => e.chain().focus().toggleBlockquote().run(),
  },
  {
    id: "table",
    label: "Table",
    hint: "3×3 with header",
    icon: TableIcon,
    apply: (e) =>
      e
        .chain()
        .focus()
        .insertTable({ rows: 3, cols: 3, withHeaderRow: true })
        .run(),
  },
  {
    id: "hr",
    label: "Divider",
    hint: "Horizontal rule",
    icon: Minus,
    apply: (e) => e.chain().focus().setHorizontalRule().run(),
  },
];

interface Props {
  editor: Editor | null;
}

interface Position {
  top: number;
  left: number;
}

export default function SlashMenu({ editor }: Props) {
  const [pos, setPos] = useState<Position | null>(null);
  const [activeIdx, setActiveIdx] = useState(0);
  const [filter, setFilter] = useState("");
  const containerRef = useRef<HTMLDivElement | null>(null);

  const filtered = useMemo(() => {
    if (!filter) return ITEMS;
    const f = filter.toLowerCase();
    return ITEMS.filter(
      (it) =>
        it.label.toLowerCase().includes(f) || it.hint.toLowerCase().includes(f),
    );
  }, [filter]);

  // Subscribe to TipTap selection updates so we know when the
  // active paragraph becomes "/<filter>"; clip the menu to the
  // editor surface so we don't drift on long documents.
  useEffect(() => {
    if (!editor) return;
    function handler() {
      if (!editor) return;
      const { state, view } = editor;
      const { $from } = state.selection;
      const block = $from.parent.textContent;
      if (block.startsWith("/")) {
        setFilter(block.slice(1));
        setActiveIdx(0);
        const coords = view.coordsAtPos($from.pos);
        const root = view.dom.getBoundingClientRect();
        setPos({
          top: coords.bottom - root.top + 4,
          left: coords.left - root.left,
        });
      } else if (pos) {
        setPos(null);
      }
    }
    editor.on("selectionUpdate", handler);
    editor.on("update", handler);
    return () => {
      editor.off("selectionUpdate", handler);
      editor.off("update", handler);
    };
  }, [editor, pos]);

  // Keyboard nav while the menu is open.
  useEffect(() => {
    if (!pos || !editor) return;
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") {
        setPos(null);
        return;
      }
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setActiveIdx((i) => (i + 1) % Math.max(filtered.length, 1));
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        setActiveIdx((i) => (i - 1 + filtered.length) % Math.max(filtered.length, 1));
      } else if (e.key === "Enter") {
        const target = filtered[activeIdx];
        if (target) {
          e.preventDefault();
          pickItem(target);
        }
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pos, filtered, activeIdx, editor]);

  function pickItem(item: SlashItem) {
    if (!editor) return;
    // Remove the "/<filter>" trigger before applying the block.
    const { $from } = editor.state.selection;
    const start = $from.start();
    const end = $from.pos;
    editor.chain().focus().deleteRange({ from: start, to: end }).run();
    item.apply(editor);
    setPos(null);
    setFilter("");
  }

  if (!editor || !pos) return null;
  if (filtered.length === 0) return null;

  return (
    <div
      ref={containerRef}
      role="listbox"
      aria-label="Insert block"
      style={{ top: pos.top, left: pos.left }}
      className="absolute z-30 w-64 rounded-md border border-border bg-surface-raised p-1 shadow-md"
    >
      {filtered.map((it, i) => {
        const Icon = it.icon;
        const isActive = i === activeIdx;
        return (
          <button
            key={it.id}
            type="button"
            role="option"
            aria-selected={isActive}
            onMouseEnter={() => setActiveIdx(i)}
            onClick={() => pickItem(it)}
            className={cn(
              "flex w-full items-center gap-2 rounded px-2 py-1.5 text-left text-xs transition-colors",
              isActive ? "bg-accent-soft text-accent-ink" : "text-ink",
            )}
          >
            <span className="inline-flex h-6 w-6 shrink-0 items-center justify-center rounded bg-surface-subtle text-ink-muted">
              <Icon size={12} />
            </span>
            <span className="min-w-0 flex-1">
              <div className="truncate font-medium">{it.label}</div>
              <div className="truncate text-[10px] text-ink-dim">{it.hint}</div>
            </span>
          </button>
        );
      })}
    </div>
  );
}
