// Auto-generated table of contents for the Pages editor.
// Parses headings out of the raw markdown so the outline reflects
// the same content as Edit, Preview, and Source views. Clicking a
// heading scrolls the matching node in the rendered surface into
// view; an IntersectionObserver highlights the heading the user is
// currently reading.

import { useEffect, useMemo, useRef, useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";

import { cn } from "../../lib/cn";

interface OutlineItem {
  id: string;
  text: string;
  level: 1 | 2 | 3;
}

interface Props {
  markdown: string;
  /** A scrolling container (usually the canvas wrapper). When the
   *  user clicks a heading we scrollIntoView the matching DOM node;
   *  the IntersectionObserver targets the same elements so the
   *  active highlight tracks reading position. */
  scrollRoot?: HTMLElement | null;
  collapsed?: boolean;
  onToggleCollapsed?: () => void;
}

const HEADING_RE = /^(#{1,3})\s+(.+?)\s*$/gm;

function slugify(text: string, taken: Set<string>): string {
  const base = text
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, "")
    .trim()
    .replace(/\s+/g, "-") || "section";
  let slug = base;
  let n = 1;
  while (taken.has(slug)) {
    slug = `${base}-${n++}`;
  }
  taken.add(slug);
  return slug;
}

export function buildOutline(markdown: string): OutlineItem[] {
  const items: OutlineItem[] = [];
  const taken = new Set<string>();
  // Strip fenced code blocks before scanning headings so a ``# foo``
  // line inside a code sample never lands in the TOC.
  const stripped = markdown.replace(/```[\s\S]*?```/g, "");
  for (const m of stripped.matchAll(HEADING_RE)) {
    const level = m[1].length as 1 | 2 | 3;
    const text = m[2].replace(/`/g, "").trim();
    if (!text) continue;
    items.push({ id: slugify(text, taken), text, level });
  }
  return items;
}

export default function PageOutline({
  markdown,
  scrollRoot,
  collapsed,
  onToggleCollapsed,
}: Props) {
  const items = useMemo(() => buildOutline(markdown), [markdown]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const observerRef = useRef<IntersectionObserver | null>(null);

  useEffect(() => {
    if (!scrollRoot || items.length === 0) return;
    // Find heading elements inside the rendered surface. Both the
    // TipTap DOM and the markdown-it output expose h1/h2/h3 nodes
    // without id attributes by default, so we tag them on each
    // markdown change using the same slug rule the outline uses.
    const headings = scrollRoot.querySelectorAll<HTMLElement>("h1, h2, h3");
    const taken = new Set<string>();
    headings.forEach((node) => {
      const text = (node.textContent || "").trim();
      if (!text) return;
      const slug = slugify(text, taken);
      node.id = slug;
    });

    observerRef.current?.disconnect();
    // jsdom + some older browsers lack IntersectionObserver. Tag the
    // ids regardless so click-to-scroll still works, and skip the
    // scroll-spy in those environments.
    if (typeof IntersectionObserver === "undefined") return;
    observerRef.current = new IntersectionObserver(
      (entries) => {
        const visible = entries
          .filter((e) => e.isIntersecting)
          .sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top)[0];
        if (visible) setActiveId(visible.target.id);
      },
      { root: scrollRoot, rootMargin: "0px 0px -70% 0px", threshold: 0 },
    );
    headings.forEach((h) => observerRef.current?.observe(h));
    return () => observerRef.current?.disconnect();
  }, [markdown, scrollRoot, items.length]);

  function handleClick(id: string) {
    if (!scrollRoot) return;
    const target = scrollRoot.querySelector<HTMLElement>(`#${CSS.escape(id)}`);
    if (target) {
      target.scrollIntoView({ behavior: "smooth", block: "start" });
      setActiveId(id);
    }
  }

  return (
    <nav
      aria-label="Page outline"
      className="rounded-md border border-border bg-surface"
    >
      <button
        type="button"
        onClick={onToggleCollapsed}
        className="flex w-full items-center justify-between gap-2 px-3 py-2 text-[10px] font-semibold uppercase tracking-wider text-ink-dim hover:text-ink"
      >
        <span>Outline</span>
        {collapsed ? <ChevronRight size={12} /> : <ChevronDown size={12} />}
      </button>
      {!collapsed && (
        <div className="border-t border-border p-2">
          {items.length === 0 ? (
            <p className="px-1 py-2 text-[11px] text-ink-dim">
              No headings yet. Add one with <code>#</code>, <code>##</code>, or <code>###</code>.
            </p>
          ) : (
            <ul className="space-y-0.5">
              {items.map((it) => (
                <li key={it.id}>
                  <button
                    type="button"
                    onClick={() => handleClick(it.id)}
                    className={cn(
                      "block w-full truncate rounded px-2 py-1 text-left text-[12px] transition-colors",
                      it.level === 1 && "font-medium",
                      it.level === 2 && "pl-4",
                      it.level === 3 && "pl-6 text-[11px]",
                      activeId === it.id
                        ? "bg-accent-soft text-accent-ink"
                        : "text-ink-muted hover:bg-surface-subtle hover:text-ink",
                    )}
                    title={it.text}
                  >
                    {it.text}
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </nav>
  );
}
