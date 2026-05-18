// Rich-text editor for the Documentation Pages feature.
// Wraps TipTap with markdown round-trip plus a raw textarea toggle so
// the user can edit either the WYSIWYG form or the markdown source.

import { useEffect, useRef, useState } from "react";
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

interface Props {
  initialMarkdown: string;
  onChange: (markdown: string) => void;
  readOnly?: boolean;
}

export default function PageEditor({
  initialMarkdown,
  onChange,
  readOnly = false,
}: Props) {
  const [rawMode, setRawMode] = useState(false);
  const [rawValue, setRawValue] = useState(initialMarkdown);
  const initialMarkdownRef = useRef(initialMarkdown);

  const editor = useEditor({
    extensions: [
      StarterKit,
      Link.configure({ openOnClick: false }),
      Table.configure({ resizable: false }),
      TableRow,
      TableHeader,
      TableCell,
      Markdown.configure({
        html: false,
        tightLists: true,
        linkify: true,
        breaks: false,
      }),
    ],
    content: initialMarkdown,
    editable: !readOnly,
    onUpdate({ editor: e }) {
      const md =
        (e.storage as { markdown?: { getMarkdown: () => string } }).markdown?.getMarkdown() ??
        "";
      setRawValue(md);
      onChange(md);
    },
  });

  // Reset content when a new initialMarkdown arrives (e.g. after fetch).
  useEffect(() => {
    if (!editor) return;
    if (initialMarkdown === initialMarkdownRef.current) return;
    initialMarkdownRef.current = initialMarkdown;
    editor.commands.setContent(initialMarkdown, { emitUpdate: false });
    setRawValue(initialMarkdown);
  }, [initialMarkdown, editor]);

  function switchToRaw() {
    if (!editor) return;
    const md =
      (editor.storage as { markdown?: { getMarkdown: () => string } }).markdown?.getMarkdown() ??
      "";
    setRawValue(md);
    setRawMode(true);
  }

  function switchToRich() {
    if (!editor) return;
    editor.commands.setContent(rawValue, { emitUpdate: false });
    onChange(rawValue);
    setRawMode(false);
  }

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap items-center justify-between gap-2">
        {editor && !rawMode ? (
          <EditorToolbar editor={editor} disabled={readOnly} />
        ) : (
          <div className="text-xs text-ink-dim">Raw markdown mode</div>
        )}
        <button
          type="button"
          onClick={() => (rawMode ? switchToRich() : switchToRaw())}
          className="rounded-md border border-border bg-surface px-2 py-1 text-[11px] text-ink-muted hover:bg-surface-subtle hover:text-ink"
        >
          {rawMode ? "Rich editor" : "Raw markdown"}
        </button>
      </div>
      {rawMode ? (
        <textarea
          value={rawValue}
          onChange={(e) => {
            setRawValue(e.target.value);
            onChange(e.target.value);
          }}
          readOnly={readOnly}
          spellCheck={false}
          className="min-h-[400px] w-full rounded-md border border-border bg-surface p-3 font-mono text-sm text-ink focus:border-accent/60 focus:outline-none"
        />
      ) : (
        <div className="rounded-md border border-border bg-surface">
          <EditorContent
            editor={editor}
            className="prose prose-sm max-w-none px-3 py-2 [&_.ProseMirror]:min-h-[400px] [&_.ProseMirror]:outline-none"
          />
        </div>
      )}
    </div>
  );
}

interface ToolbarProps {
  editor: Editor;
  disabled: boolean;
}

function EditorToolbar({ editor, disabled }: ToolbarProps) {
  const [moreOpen, setMoreOpen] = useState(false);

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
      {/* Secondary cluster: visible inline on sm+, collapsed into More menu on xs */}
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
    </div>
  );
}
