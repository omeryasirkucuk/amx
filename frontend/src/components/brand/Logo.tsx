import { cn } from "../../lib/cn";

// Classic AMX wordmark, recreated as a 5x7 pixel-art font matching
// the README banner. Each letter is a string grid; the renderer
// emits a 1×1 SVG <rect> for each filled cell. `currentColor` lets
// the parent set the fill (default is the AMX accent amber, applied
// via the `text-accent` class on the wrapper).

const A = [
  ".XXX.",
  "X...X",
  "X...X",
  "XXXXX",
  "X...X",
  "X...X",
  "X...X",
];

const M = [
  "X...X",
  "XX.XX",
  "X.X.X",
  "X...X",
  "X...X",
  "X...X",
  "X...X",
];

const X = [
  "X...X",
  "X...X",
  ".X.X.",
  "..X..",
  ".X.X.",
  "X...X",
  "X...X",
];

// Letter at xOffset; one column gap between letters → 5+1+5+1+5 = 17 wide
const LETTERS: Array<{ grid: string[]; x: number }> = [
  { grid: A, x: 0 },
  { grid: M, x: 6 },
  { grid: X, x: 12 },
];

interface Props {
  /** Rendered pixel height. Width auto-derives from the 17:7 ratio. */
  size?: number;
  className?: string;
  /** Override the fill color (defaults to currentColor → accent). */
  color?: string;
}

export default function Logo({ size = 14, className, color }: Props) {
  const width = Math.round((size * 17) / 7);
  return (
    <svg
      width={width}
      height={size}
      viewBox="0 0 17 7"
      role="img"
      aria-label="AMX"
      shapeRendering="crispEdges"
      className={cn("shrink-0 text-accent", className)}
    >
      {LETTERS.flatMap(({ grid, x: xOff }) =>
        grid.flatMap((row, y) =>
          row.split("").map((ch, x) =>
            ch === "X" ? (
              <rect
                key={`${xOff}-${x}-${y}`}
                x={xOff + x}
                y={y}
                width="1"
                height="1"
                fill={color ?? "currentColor"}
              />
            ) : null,
          ),
        ),
      )}
    </svg>
  );
}
