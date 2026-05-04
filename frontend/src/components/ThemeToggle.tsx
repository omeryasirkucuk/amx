import { Monitor, Moon, Sun } from "lucide-react";

import { useTheme, type Theme } from "../lib/theme";
import { cn } from "../lib/cn";

const ORDER: Theme[] = ["system", "light", "dark"];

const ICONS: Record<Theme, typeof Sun> = {
  light: Sun,
  dark: Moon,
  system: Monitor,
};

const LABELS: Record<Theme, string> = {
  light: "Light",
  dark: "Dark",
  system: "System",
};

// Cycles through system → light → dark → system. Keeping all three
// modes addressable means a user on a system-dark laptop can pin
// the visualizer to light when they prefer that contrast.
export default function ThemeToggle({ className }: { className?: string }) {
  const { theme, setTheme } = useTheme();
  const Icon = ICONS[theme];
  const next: Theme = ORDER[(ORDER.indexOf(theme) + 1) % ORDER.length];

  return (
    <button
      type="button"
      onClick={() => setTheme(next)}
      title={`Theme: ${LABELS[theme]} — click for ${LABELS[next]}`}
      className={cn(
        "rounded-md p-1.5 text-ink-muted transition hover:bg-surface-subtle hover:text-ink",
        className,
      )}
    >
      <Icon size={16} />
    </button>
  );
}
