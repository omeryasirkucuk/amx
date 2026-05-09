import { cn } from "../../lib/cn";

// AMX brand mark + optional pixel-art suffix.
//
// The AMX block is the canonical orange-on-transparent bitmap shipped
// from amx-docs (placed at /amx-logo.png by the build_amx_brand_logos
// generator). The suffix word ("STUDIO" / "CLI") is rendered as an
// inline SVG of 5x7 pixel-art rects so it stays crisp at any size —
// baking it into the PNG was killing the strokes once the topbar
// downscaled the asset to ~h-7. The two pieces share a common bottom
// baseline.

const GLYPHS: Record<string, string[]> = {
  S: [".XXXX", "X....", "X....", ".XXX.", "....X", "....X", "XXXX."],
  T: ["XXXXX", "..X..", "..X..", "..X..", "..X..", "..X..", "..X.."],
  U: ["X...X", "X...X", "X...X", "X...X", "X...X", "X...X", "XXXXX"],
  D: ["XXXX.", "X...X", "X...X", "X...X", "X...X", "X...X", "XXXX."],
  I: ["XXXXX", "..X..", "..X..", "..X..", "..X..", "..X..", "XXXXX"],
  O: [".XXX.", "X...X", "X...X", "X...X", "X...X", "X...X", ".XXX."],
  C: [".XXXX", "X....", "X....", "X....", "X....", "X....", ".XXXX"],
  L: ["X....", "X....", "X....", "X....", "X....", "X....", "XXXXX"],
};

const LETTER_W = 5;
const ROW_H = 7;
const LETTER_GAP = 1;

function suffixCols(text: string): number {
  if (text.length === 0) return 0;
  return text.length * LETTER_W + (text.length - 1) * LETTER_GAP;
}

interface Props {
  /** Rendered pixel height of the AMX bitmap. The suffix scales to match. */
  size?: number;
  className?: string;
  /** Optional pixel-art wordmark rendered to the right of "AMX". */
  suffix?: string;
}

export default function Logo({ size = 14, className, suffix }: Props) {
  const text = (suffix ?? "").toUpperCase();
  const cols = suffixCols(text);

  // The suffix word sits at ~78% of the AMX height and shares the same
  // baseline (bottom edge). Width is whatever the 5x7 grid demands.
  const suffixHeight = Math.max(7, Math.round(size * 0.78));
  const suffixWidth =
    cols === 0 ? 0 : Math.max(1, Math.round((suffixHeight * cols) / ROW_H));
  const gap = Math.max(2, Math.round(size * 0.18));

  const ariaLabel = text ? `AMX ${text}` : "AMX";

  const rects: JSX.Element[] = [];
  if (cols > 0) {
    let col = 0;
    for (let i = 0; i < text.length; i += 1) {
      const ch = text[i];
      const grid = GLYPHS[ch];
      if (grid) {
        for (let y = 0; y < grid.length; y += 1) {
          const row = grid[y];
          for (let x = 0; x < row.length; x += 1) {
            if (row[x] === "X") {
              rects.push(
                <rect
                  key={`${i}-${x}-${y}`}
                  x={col + x}
                  y={y}
                  width={1}
                  height={1}
                />,
              );
            }
          }
        }
      }
      col += LETTER_W + LETTER_GAP;
    }
  }

  return (
    <span
      role="img"
      aria-label={ariaLabel}
      className={cn("inline-flex items-end", className)}
      style={{ gap }}
    >
      <img
        src="/amx-logo.png"
        alt=""
        aria-hidden="true"
        draggable={false}
        style={{
          height: size,
          width: "auto",
          imageRendering: "pixelated",
          display: "block",
        }}
      />
      {cols > 0 && (
        <svg
          width={suffixWidth}
          height={suffixHeight}
          viewBox={`0 0 ${cols} ${ROW_H}`}
          shapeRendering="crispEdges"
          fill="rgb(245, 245, 242)"
          aria-hidden="true"
          style={{ display: "block" }}
        >
          {rects}
        </svg>
      )}
    </span>
  );
}
