// Top-of-canvas formatting bar for the Pages editor.
// Lives in the route shell (not nested inside PageEditor) so the
// outline / canvas / rail columns all share the same top baseline
// in the document-first layout. ``editor`` is null while the
// TipTap instance is still mounting and when the active view is
// Preview / Source; the buttons stay rendered but go disabled.

import { useState } from "react";
import type { Editor } from "@tiptap/react";
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
  Minus,
  MoreHorizontal,
  Table as TableIcon,
} from "lucide-react";

import { cn } from "../../lib/cn";

interface Props {
  editor: Editor | null;
  disabled?: boolean;
}

export default function EditorToolbar({ editor, disabled = false }: Props) {
  const [moreOpen, setMoreOpen] = useState(false);
  const inert = disabled || !editor;

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
        disabled={inert}
        className={cn(
          "inline-flex h-7 w-7 items-center justify-center rounded text-ink-muted transition-colors",
          isActive && "bg-accent-soft text-accent-ink",
          !isActive && !inert && "hover:bg-surface-subtle hover:text-ink",
          inert && "cursor-not-allowed opacity-40",
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
        () => editor?.chain().focus().toggleHeading({ level: 1 }).run(),
        <Heading1 size={14} />,
        "Heading 1",
        !!editor?.isActive("heading", { level: 1 }),
      )}
      {btn(
        () => editor?.chain().focus().toggleHeading({ level: 2 }).run(),
        <Heading2 size={14} />,
        "Heading 2",
        !!editor?.isActive("heading", { level: 2 }),
      )}
      {btn(
        () => editor?.chain().focus().toggleHeading({ level: 3 }).run(),
        <Heading3 size={14} />,
        "Heading 3",
        !!editor?.isActive("heading", { level: 3 }),
      )}
      {btn(
        () => editor?.chain().focus().toggleBold().run(),
        <Bold size={14} />,
        "Bold",
        !!editor?.isActive("bold"),
      )}
      {btn(
        () => editor?.chain().focus().toggleItalic().run(),
        <Italic size={14} />,
        "Italic",
        !!editor?.isActive("italic"),
      )}
    </>
  );

  const secondary = (
    <>
      {btn(
        () => editor?.chain().focus().toggleBulletList().run(),
        <List size={14} />,
        "Bullet list",
        !!editor?.isActive("bulletList"),
      )}
      {btn(
        () => editor?.chain().focus().toggleOrderedList().run(),
        <ListOrdered size={14} />,
        "Ordered list",
        !!editor?.isActive("orderedList"),
      )}
      {btn(
        () => editor?.chain().focus().toggleCode().run(),
        <Code size={14} />,
        "Inline code",
        !!editor?.isActive("code"),
      )}
      {btn(addLink, <LinkIcon size={14} />, "Insert link", !!editor?.isActive("link"))}
      {btn(
        () =>
          editor
            ?.chain()
            .focus()
            .insertTable({ rows: 3, cols: 3, withHeaderRow: true })
            .run(),
        <TableIcon size={14} />,
        "Insert table",
      )}
      {btn(
        () => editor?.chain().focus().setHorizontalRule().run(),
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
          disabled={inert}
          className="inline-flex h-7 w-7 items-center justify-center rounded text-ink-muted hover:bg-surface-subtle hover:text-ink disabled:cursor-not-allowed disabled:opacity-40"
        >
          <MoreHorizontal size={14} />
        </button>
        {moreOpen && !inert && (
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
