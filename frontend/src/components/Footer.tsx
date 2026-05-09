import { ExternalLink } from "lucide-react";

import Logo from "./brand/Logo";

/**
 * Site footer. Pinned to the bottom of the viewport beside the
 * sidebar (see AppShell) and links to the AMX docs site.
 * Intentionally low-key — a single horizontal row, no panel chrome.
 */
export default function Footer() {
  return (
    <footer className="border-t border-border pt-3 pb-2 text-xs text-ink-dim">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <Logo size={11} />
          <span>
            Agentic Metadata Extractor — open source under Apache 2.0.
          </span>
        </div>
        <a
          href="https://amxcli.com"
          target="_blank"
          rel="noreferrer"
          className="inline-flex items-center gap-1 rounded text-ink-muted transition-colors duration-fast hover:text-accent"
        >
          amxcli.com
          <ExternalLink size={11} aria-hidden="true" />
        </a>
      </div>
    </footer>
  );
}
