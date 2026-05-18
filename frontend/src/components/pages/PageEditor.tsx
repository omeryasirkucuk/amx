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
import {
  Bold,
  Code,
  Heading1,
  Heading2,
  Heading3,
  Italic,
  Link as LinkIcon,
  List,
  ListOrdered,
  MoreHorizontal,
  Minus,
  Table as TableIcon,
} from "lucide-react";

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
}

const RICH_CLASS = cn(
  "prose prose-sm lg:prose-base max-w-none",
  "prose-headings:text-ink prose-headings:font-semibold prose-headings:tracking-tight",
  "prose-p:text-ink prose-li:text-ink prose-strong:text-ink",
  "prose-a:text-accent prose-a:no-underline hover:prose-a:underline",
  "prose-code:rounded prose-code:bg-surface-subtle prose-code:px-1 prose-code:py-[1px] prose-code:text-accent-ink",
  "prose-pre:bg-surface-subtle prose-pre:text-ink",
  "prose-blockquote:border-accent/40 prose-blockquote:text-ink-muted",
  "prose-hr:border-border",
  "prose-table:text-sm",
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
        className="min-h-[500px] w-full rounded-md border border-border bg-surface p-4 font-mono text-sm text-ink focus:border-accent/60 focus:outline-none"
      />
    );
  }

  return (
    <div className="flex flex-col gap-2">
      <EditorToolbar editor={editor} disabled={readOnly} />
      <div
        ref={surfaceRef ?? undefined}
        className="relative rounded-md border border-border bg-surface"
      >
        <EditorContent
          editor={editor}
          className={cn(
            "px-5 py-4 [&_.ProseMirror]:min-h-[500px] [&_.ProseMirror]:outline-none",
            RICH_CLASS,
          )}
        />
        <SlashMenu editor={editor} />
      </div>
    </div>
  );
}

interface ToolbarProps {
  editor: Editor | null;
  disabled: boolean;
}

function EditorToolbar({ editor, disabled }: ToolbarProps) {
  const [moreOpen, setMoreOpen] = useState(false);

  if (!editor) return null;

  function btn(
    onClick: () => void,
    icon: React.ReactNode,
    label: string,
    isActive = false,
  ) {
    return (
      <button
        type="button"
        onClick={onClick}
        aria-label={label}
        title={label}
        aria-pressed={isActive}
        disabled={disabled}
        className={cn(
          "inline-flex h-7 w-7 items-center justify-center rounded text-ink-muted transition-colors",
          isActive && "bg-accent-soft text-accent-ink",
          !isActive && "hover:bg-surface-subtle hover:text-ink",
          disabled && "cursor-not-allowed opacity-50",
        )}
      >
        {icon}
      </button>
    );
  }

  function addLink() {
    if (!editor) return;
    const prev = editor.getAttributes("link").href as string | undefined;
    const url = window.prompt("Link URL", prev ?? "https://");
    if (url === null) return;
    if (url === "") {
      editor.chain().focus().extendMarkRange("link").unsetLink().run();
      return;
    }
    editor.chain().focus().extendMarkRange("link").setLink({ href: url }).run();
  }

  const primary = (
    <>
      {btn(
        () => editor.chain().focus().toggleHeading({ level: 1 }).run(),
        <Heading1 size={14} />,
        "Heading 1",
        editor.isActive("heading", { level: 1 }),
      )}
      {btn(
        () => editor.chain().focus().toggleHeading({ level: 2 }).run(),
        <Heading2 size={14} />,
        "Heading 2",
        editor.isActive("heading", { level: 2 }),
      )}
      {btn(
        () => editor.chain().focus().toggleHeading({ level: 3 }).run(),
        <Heading3 size={14} />,
        "Heading 3",
        editor.isActive("heading", { level: 3 }),
      )}
      {btn(
        () => editor.chain().focus().toggleBold().run(),
        <Bold size={14} />,
        "Bold",
        editor.isActive("bold"),
      )}
      {btn(
        () => editor.chain().focus().toggleItalic().run(),
        <Italic size={14} />,
        "Italic",
        editor.isActive("italic"),
      )}
    </>
  );

  const secondary = (
    <>
      {btn(
        () => editor.chain().focus().toggleBulletList().run(),
        <List size={14} />,
        "Bullet list",
        editor.isActive("bulletList"),
      )}
      {btn(
        () => editor.chain().focus().toggleOrderedList().run(),
        <ListOrdered size={14} />,
        "Ordered list",
        editor.isActive("orderedList"),
      )}
      {btn(
        () => editor.chain().focus().toggleCode().run(),
        <Code size={14} />,
        "Inline code",
        editor.isActive("code"),
      )}
      {btn(addLink, <LinkIcon size={14} />, "Insert link", editor.isActive("link"))}
      {btn(
        () =>
          editor
            .chain()
            .focus()
            .insertTable({ rows: 3, cols: 3, withHeaderRow: true })
            .run(),
        <TableIcon size={14} />,
        "Insert table",
      )}
      {btn(
        () => editor.chain().focus().setHorizontalRule().run(),
        <Minus size={14} />,
        "Horizontal rule",
      )}
    </>
  );

  return (
    <div className="flex flex-wrap items-center gap-0.5">
      {primary}
      <div className="hidden sm:flex items-center gap-0.5">{secondary}</div>
      <div className="sm:hidden relative">
        <button
          type="button"
          onClick={() => setMoreOpen((v) => !v)}
          aria-label="More formatting"
          aria-expanded={moreOpen}
          className="inline-flex h-7 w-7 items-center justify-center rounded text-ink-muted hover:bg-surface-subtle hover:text-ink"
        >
          <MoreHorizontal size={14} />
        </button>
        {moreOpen && (
          <div className="absolute left-0 top-8 z-10 flex gap-0.5 rounded-md border border-border bg-surface-raised p-1 shadow-md">
            {secondary}
          </div>
        )}
      </div>
      <span className="ml-auto text-[10px] text-ink-dim">
        Tip: type <kbd className="rounded border border-border bg-surface px-1">/</kbd> for blocks
      </span>
    </div>
  );
}
