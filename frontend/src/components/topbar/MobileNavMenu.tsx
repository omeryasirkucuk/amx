import { useEffect, useRef, useState } from "react";
import { NavLink, useLocation } from "react-router-dom";
import { MoreVertical } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { cn } from "../../lib/cn";
import IconButton from "../ui/IconButton";

export interface MobileNavItem {
  to: string;
  label: string;
  icon: LucideIcon;
}

interface Props {
  items: MobileNavItem[];
}

/**
 * Kebab popover that surfaces the TopBar's primary nav on phone
 * viewports. The inline desktop nav is hidden below `sm`; this
 * compact menu replaces it.
 */
export default function MobileNavMenu({ items }: Props) {
  const [open, setOpen] = useState(false);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const location = useLocation();

  useEffect(() => {
    setOpen(false);
  }, [location.pathname]);

  useEffect(() => {
    if (!open) return;
    function onClick(e: MouseEvent) {
      if (!wrapperRef.current) return;
      if (!wrapperRef.current.contains(e.target as Node)) setOpen(false);
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") setOpen(false);
    }
    window.addEventListener("mousedown", onClick);
    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("mousedown", onClick);
      window.removeEventListener("keydown", onKey);
    };
  }, [open]);

  return (
    <div ref={wrapperRef} className="relative sm:hidden">
      <IconButton
        icon={<MoreVertical size={16} />}
        label="Open navigation menu"
        size="sm"
        onClick={() => setOpen((v) => !v)}
      />
      {open && (
        <nav
          role="menu"
          aria-label="Primary navigation"
          className="absolute right-0 top-full z-50 mt-1 w-48 rounded-md border border-border bg-surface-raised shadow-lg"
        >
          <ul className="py-1">
            {items.map((item) => (
              <li key={item.to}>
                <NavLink
                  to={item.to}
                  role="menuitem"
                  className={({ isActive }) =>
                    cn(
                      "flex items-center gap-2 px-3 py-2 text-sm",
                      isActive
                        ? "bg-accent-soft text-accent-ink"
                        : "text-ink hover:bg-surface-subtle",
                    )
                  }
                >
                  <item.icon size={14} />
                  {item.label}
                </NavLink>
              </li>
            ))}
          </ul>
        </nav>
      )}
    </div>
  );
}
