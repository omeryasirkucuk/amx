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
          "min-h-[400px] text-sm text-ink-dim",
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
        "min-h-[400px]",
        "prose prose-base lg:prose-lg max-w-none",
        "prose-headings:text-ink prose-headings:font-semibold prose-headings:tracking-tight",
        "prose-h1:text-3xl prose-h1:mt-0 prose-h1:mb-4",
        "prose-h2:text-2xl prose-h2:mt-8 prose-h2:mb-3",
        "prose-h3:text-xl prose-h3:mt-6 prose-h3:mb-2",
        "prose-p:text-ink prose-p:leading-relaxed",
        "prose-li:text-ink prose-li:marker:text-ink-dim",
        "prose-strong:text-ink prose-strong:font-semibold",
        "prose-a:text-accent prose-a:no-underline hover:prose-a:underline",
        "prose-code:rounded prose-code:bg-surface-subtle prose-code:px-1 prose-code:py-[1px] prose-code:text-accent-ink prose-code:font-medium prose-code:before:hidden prose-code:after:hidden",
        "prose-pre:bg-surface-subtle prose-pre:text-ink prose-pre:border prose-pre:border-border",
        "prose-blockquote:border-accent/40 prose-blockquote:text-ink-muted prose-blockquote:not-italic",
        "prose-hr:border-border",
        "prose-table:text-sm",
        className,
      )}
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}
