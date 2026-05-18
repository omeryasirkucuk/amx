// Read-only Markdown renderer for the Pages editor Preview mode.
// Uses the same markdown-it instance that ships transitively with
// tiptap-markdown so Edit (TipTap) and Preview agree on the parse
// rules. Output is wrapped in the prose classes the editor already
// uses, so switching between modes looks visually consistent.

import { useMemo } from "react";
import MarkdownIt from "markdown-it";

import { cn } from "../../lib/cn";

const md = new MarkdownIt({
  html: false,
  linkify: true,
  typographer: true,
  breaks: false,
});

interface Props {
  markdown: string;
  className?: string;
}

export default function PreviewPane({ markdown, className }: Props) {
  const html = useMemo(() => md.render(markdown || ""), [markdown]);
  if (!markdown.trim()) {
    return (
      <div
        className={cn(
          "min-h-[400px] rounded-md border border-border bg-surface p-6 text-sm text-ink-dim",
          className,
        )}
      >
        Nothing to preview yet. Switch to Edit or click Re-generate.
      </div>
    );
  }
  return (
    <div
      className={cn(
        "min-h-[400px] rounded-md border border-border bg-surface px-6 py-5",
        "prose prose-sm lg:prose-base max-w-none",
        "prose-headings:text-ink prose-headings:font-semibold prose-headings:tracking-tight",
        "prose-p:text-ink prose-li:text-ink prose-strong:text-ink",
        "prose-a:text-accent prose-a:no-underline hover:prose-a:underline",
        "prose-code:rounded prose-code:bg-surface-subtle prose-code:px-1 prose-code:py-[1px] prose-code:text-accent-ink",
        "prose-pre:bg-surface-subtle prose-pre:text-ink",
        "prose-blockquote:border-accent/40 prose-blockquote:text-ink-muted",
        "prose-hr:border-border",
        "prose-table:text-sm",
        className,
      )}
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}
