// Rich-text editor surface for the Pages feature.
// Hosts the TipTap WYSIWYG with markdown round-trip, a slash menu
// for inserting blocks, and lets the route swap between three
// views: WYSIWYG editing, rendered preview, and raw markdown
// source. The view choice is owned by the caller so it can persist
// through localStorage and synchronise the page header pill.

import { useEffect, useMemo, useRef, useState } from "react";
import { EditorContent, useEditor, type Editor } from "@tiptap/react";
import StarterKit from "@tiptap/starter-kit";
import { Link } from "@tiptap/extension-link";
import {
  Table,
  TableCell,
  TableHeader,
  TableRow,
} from "@tiptap/extension-table";
import { Markdown } from "tiptap-markdown";
import MarkdownIt from "markdown-it";

import { cn } from "../../lib/cn";
import PreviewPane from "./PreviewPane";
import SlashMenu from "./SlashMenu";
import type { EditorView } from "./EditorViewSwitcher";

interface Props {
  initialMarkdown: string;
  onChange: (markdown: string) => void;
  view: EditorView;
  readOnly?: boolean;
  /** Forwarded to the canvas root so the route can use it for the
   *  outline scroll-spy and focus-mode wiring. */
  surfaceRef?: React.MutableRefObject<HTMLDivElement | null>;
  /** Lets the route render the editor toolbar above the 3-column
   *  grid (document-first layout) instead of nesting it inside the
   *  canvas card. Receives ``null`` while the editor is mounting and
   *  again on unmount so the caller can grey-out toolbar buttons. */
  onEditorReady?: (editor: Editor | null) => void;
}

const RICH_CLASS = cn(
  "prose prose-base lg:prose-lg max-w-none",
  // Headings: explicit size/weight so they read clearly against the
  // body, plus colour + tracking matching the rest of AMX.
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
  "prose-table:text-sm prose-th:text-ink prose-td:text-ink prose-td:border-border prose-th:border-border",
);

// Shared markdown-it instance: we use it to pre-render markdown to
// HTML before handing it to TipTap, because tiptap-markdown's
// auto-parser only kicks in for paste / typed input — content set
// via the ``content`` config option or ``setContent`` does NOT get
// parsed, which is why a raw markdown string used to render with
// literal ``#`` and ``**`` characters in the WYSIWYG view.
const mdRenderer = new MarkdownIt({
  html: false,
  linkify: true,
  typographer: true,
  breaks: false,
});

// Some LLMs wrap their entire reply in a ```markdown ... ``` fence
// even when the prompt forbids it. We strip the wrapper at the
// editor boundary so existing rows that were saved before the
// backend fix also render correctly.
const FENCE_RE = /^\s*```(?:markdown|md)?\s*\n([\s\S]*?)\n```\s*$/i;

function stripFence(body: string): string {
  if (!body) return body;
  const m = body.trim().match(FENCE_RE);
  return m ? m[1] : body;
}

function markdownToHtml(body: string): string {
  return mdRenderer.render(stripFence(body));
}

export default function PageEditor({
  initialMarkdown,
  onChange,
  view,
  readOnly = false,
  surfaceRef,
  onEditorReady,
}: Props) {
  const cleanInitial = useMemo(() => stripFence(initialMarkdown), [initialMarkdown]);
  const [rawValue, setRawValue] = useState(cleanInitial);
  const initialMarkdownRef = useRef(cleanInitial);

  const editor = useEditor({
    extensions: [
      StarterKit,
      Link.configure({ openOnClick: false }),
      Table.configure({ resizable: false }),
      TableRow,
      TableHeader,
      TableCell,
      Markdown.configure({
        html: true,
        tightLists: true,
        linkify: true,
        breaks: false,
      }),
    ],
    content: markdownToHtml(cleanInitial),
    editable: !readOnly && view === "edit",
    onUpdate({ editor: e }) {
      const md =
        (e.storage as { markdown?: { getMarkdown: () => string } }).markdown?.getMarkdown() ??
        "";
      setRawValue(md);
      onChange(md);
    },
  });

  // Mirror the editable flag whenever the caller flips views.
  useEffect(() => {
    if (!editor) return;
    editor.setEditable(!readOnly && view === "edit");
  }, [editor, view, readOnly]);

  // Surface the TipTap instance to the route so the toolbar above
  // the 3-column grid can drive it. Re-publish on mount and clear
  // on unmount so the caller can grey out controls when there is
  // no editor (e.g. while the page is still loading).
  useEffect(() => {
    onEditorReady?.(editor ?? null);
    return () => onEditorReady?.(null);
  }, [editor, onEditorReady]);

  // Reset content when a new initialMarkdown arrives (e.g. after fetch
  // or a Restore from the versions drawer). We render to HTML first so
  // TipTap rebuilds the document with real heading / bold / list nodes
  // instead of a single text block.
  useEffect(() => {
    if (!editor) return;
    const clean = stripFence(initialMarkdown);
    if (clean === initialMarkdownRef.current) return;
    initialMarkdownRef.current = clean;
    editor.commands.setContent(markdownToHtml(clean), { emitUpdate: false });
    setRawValue(clean);
  }, [initialMarkdown, editor]);

  // When the user edits in Source view, push the change back into
  // TipTap so switching to Edit/Preview later shows the latest body.
  function handleRawChange(next: string) {
    setRawValue(next);
    onChange(next);
    if (editor) {
      editor.commands.setContent(markdownToHtml(next), { emitUpdate: false });
    }
  }

  if (view === "preview") {
    return <PreviewPane markdown={stripFence(rawValue)} className={RICH_CLASS} />;
  }

  if (view === "source") {
    return (
      <textarea
        value={rawValue}
        onChange={(e) => handleRawChange(e.target.value)}
        readOnly={readOnly}
        spellCheck={false}
        className="min-h-[500px] w-full resize-y border-0 bg-transparent p-0 font-mono text-sm text-ink outline-none focus:outline-none"
      />
    );
  }

  return (
    <div
      ref={surfaceRef ?? undefined}
      className="relative"
    >
      <EditorContent
        editor={editor}
        className={cn(
          "[&_.ProseMirror]:min-h-[500px] [&_.ProseMirror]:outline-none",
          RICH_CLASS,
        )}
      />
      <SlashMenu editor={editor} />
    </div>
  );
}
