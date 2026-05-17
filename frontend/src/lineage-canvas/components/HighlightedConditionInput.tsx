/**
 * Dual-layer textarea with @-mention highlighting.
 *
 * The visible textarea is transparent; a backdrop ``<div>`` renders the
 * same text underneath with ``<mark>`` tags around @-tokens (e.g.
 * ``@order_id``). Typing scrolls both layers in lockstep so the
 * highlights track the cursor. A small popover under the cursor
 * suggests columns drawn from ``columns`` whenever the user types ``@``.
 *
 * This is the Dataloom Filter operator's signature input. AMX wires
 * ``columns`` to the upstream node's column list so suggestions only
 * surface what's actually in scope.
 */

import {
  ChangeEvent,
  KeyboardEvent,
  UIEvent,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

interface Props {
  value: string;
  onChange: (next: string) => void;
  columns: string[];
  placeholder?: string;
  rows?: number;
}

function renderBackdrop(text: string): string {
  // Escape HTML, then wrap @-tokens in <mark>.
  const escaped = text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
  return escaped.replace(/(@[A-Za-z_][A-Za-z0-9_]*)/g, "<mark>$1</mark>");
}

export function HighlightedConditionInput({
  value,
  onChange,
  columns,
  placeholder,
  rows = 3,
}: Props) {
  const textareaRef = useRef<HTMLTextAreaElement | null>(null);
  const backdropRef = useRef<HTMLDivElement | null>(null);
  const [suggestOpen, setSuggestOpen] = useState(false);
  const [suggestQuery, setSuggestQuery] = useState("");
  const [activeIdx, setActiveIdx] = useState(0);

  const filtered = useMemo(() => {
    const q = suggestQuery.toLowerCase();
    if (!q) return columns.slice(0, 10);
    return columns.filter((c) => c.toLowerCase().includes(q)).slice(0, 10);
  }, [columns, suggestQuery]);

  useEffect(() => {
    setActiveIdx(0);
  }, [suggestOpen, filtered.length]);

  function handleScroll(e: UIEvent<HTMLTextAreaElement>) {
    if (backdropRef.current) {
      backdropRef.current.scrollTop = (e.target as HTMLTextAreaElement).scrollTop;
      backdropRef.current.scrollLeft = (e.target as HTMLTextAreaElement).scrollLeft;
    }
  }

  function handleChange(e: ChangeEvent<HTMLTextAreaElement>) {
    const next = e.target.value;
    onChange(next);
    const cursor = e.target.selectionStart || 0;
    const before = next.slice(0, cursor);
    const atIdx = before.lastIndexOf("@");
    if (atIdx >= 0 && /^@[A-Za-z0-9_]*$/.test(before.slice(atIdx))) {
      setSuggestQuery(before.slice(atIdx + 1));
      setSuggestOpen(true);
    } else {
      setSuggestOpen(false);
    }
  }

  function applySuggestion(name: string) {
    const ta = textareaRef.current;
    if (!ta) return;
    const cursor = ta.selectionStart || 0;
    const before = ta.value.slice(0, cursor);
    const after = ta.value.slice(cursor);
    const atIdx = before.lastIndexOf("@");
    if (atIdx < 0) return;
    const next = before.slice(0, atIdx) + "@" + name + after;
    onChange(next);
    setSuggestOpen(false);
    requestAnimationFrame(() => {
      const newCursor = atIdx + 1 + name.length;
      ta.focus();
      ta.setSelectionRange(newCursor, newCursor);
    });
  }

  function handleKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (!suggestOpen) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActiveIdx((i) => (i + 1) % Math.max(filtered.length, 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActiveIdx((i) => (i - 1 + filtered.length) % Math.max(filtered.length, 1));
    } else if (e.key === "Enter") {
      if (filtered[activeIdx]) {
        e.preventDefault();
        applySuggestion(filtered[activeIdx]);
      }
    } else if (e.key === "Escape") {
      setSuggestOpen(false);
    }
  }

  return (
    <div className="relative w-full">
      <div
        ref={backdropRef}
        aria-hidden
        className="lcv-mention-backdrop rounded-md border-surface-border font-mono text-[12.5px] leading-snug"
        dangerouslySetInnerHTML={{ __html: renderBackdrop(value || "") }}
      />
      <textarea
        ref={textareaRef}
        value={value}
        onChange={handleChange}
        onKeyDown={handleKeyDown}
        onScroll={handleScroll}
        placeholder={placeholder || "e.g. @status == 'completed' AND @amount > 100"}
        rows={rows}
        spellCheck={false}
        className="relative z-10 w-full resize-none rounded-md border border-surface-border bg-transparent px-2.5 py-2 font-mono text-[12.5px] leading-snug text-ink outline-none focus:border-accent-default"
        style={{ caretColor: "rgb(var(--ink))" }}
      />
      {suggestOpen && filtered.length > 0 && (
        <ul className="absolute left-2 top-full z-20 mt-1 max-h-44 w-56 overflow-y-auto rounded-md border border-surface-border bg-surface-raised text-[12px] shadow-2xl">
          {filtered.map((name, idx) => (
            <li
              key={name}
              className={
                "flex cursor-pointer items-center gap-2 px-2 py-1 " +
                (idx === activeIdx ? "bg-accent-soft text-accent-ink" : "")
              }
              onMouseEnter={() => setActiveIdx(idx)}
              onMouseDown={(e) => {
                e.preventDefault();
                applySuggestion(name);
              }}
            >
              <span className="font-mono text-fg-muted">@</span>
              <span className="font-mono">{name}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
