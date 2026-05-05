import { cn } from "../../lib/cn";

// "STUDIO" wordmark drawn on the same 5×7 pixel grid as the AMX
// Logo, so the two sit cleanly side-by-side at any scale. Rendered
// smaller (default 10px tall vs the AMX wordmark's 16px) and in white
// so it reads as a subordinate label next to the brand mark.

const S = [
  ".XXXX",
  "X....",
  "X....",
  ".XXX.",
  "....X",
  "....X",
  "XXXX.",
];

const T = [
  "XXXXX",
  "..X..",
  "..X..",
  "..X..",
  "..X..",
  "..X..",
  "..X..",
];

const U = [
  "X...X",
  "X...X",
  "X...X",
  "X...X",
  "X...X",
  "X...X",
  ".XXX.",
];

const D = [
  "XXXX.",
  "X...X",
  "X...X",
  "X...X",
  "X...X",
  "X...X",
  "XXXX.",
];

const I = [
  "XXXXX",
  "..X..",
  "..X..",
  "..X..",
  "..X..",
  "..X..",
  "XXXXX",
];

const O = [
  ".XXX.",
  "X...X",
  "X...X",
  "X...X",
  "X...X",
  "X...X",
  ".XXX.",
];

// 5-wide letter + 1-col gap = 6 cols per slot; last letter has no
// trailing gap, so total width = 6*5 + 5 = 35 cols.
const LETTERS: Array<{ grid: string[]; x: number }> = [
  { grid: S, x: 0 },
  { grid: T, x: 6 },
  { grid: U, x: 12 },
  { grid: D, x: 18 },
  { grid: I, x: 24 },
  { grid: O, x: 30 },
];

interface Props {
  /** Rendered pixel height. Width auto-derives from the 35:7 ratio. */
  size?: number;
  className?: string;
  /** Override the fill color (defaults to currentColor → white via text-white). */
  color?: string;
}

export default function StudioMark({ size = 10, className, color }: Props) {
  const width = Math.round((size * 35) / 7);
  return (
    <svg
      width={width}
      height={size}
      viewBox="0 0 35 7"
      role="img"
      aria-label="Studio"
      shapeRendering="crispEdges"
      className={cn("shrink-0 text-white", className)}
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
