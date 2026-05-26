/**
 * ColumnTypeGlyph — Databricks-style monochrome type marker shown at the
 * left of every column row. Databricks does not color-code these: a
 * numeric column shows a small "123", a string shows "Aᵇc", a date a
 * calendar, a timestamp a clock, a decimal ".00". We mirror that exact
 * vocabulary so the column rail reads identically.
 *
 * The raw dtype is inspected directly (not the coarse ``normalizeType``)
 * so date vs timestamp resolve to different glyphs, matching Databricks.
 */

import { Calendar, Clock, ToggleLeft } from "lucide-react";

function TextGlyph({ children }: { children: React.ReactNode }) {
  return (
    <span className="inline-flex w-4 shrink-0 justify-center font-mono text-[10px] leading-none text-fg-muted">
      {children}
    </span>
  );
}

export function ColumnTypeGlyph({ dtype }: { dtype: string | null | undefined }) {
  const t = String(dtype || "").toLowerCase();

  // Order matters: "timestamp" contains "time"; "date" must not steal
  // "timestamp"/"datetime", so test timestamp/time first.
  if (t.includes("timestamp") || t.includes("datetime") || (t.includes("time") && !t.includes("date"))) {
    return <Clock size={12} className="shrink-0 text-fg-muted" />;
  }
  if (t.includes("date")) {
    return <Calendar size={12} className="shrink-0 text-fg-muted" />;
  }
  if (t.includes("bool")) {
    return <ToggleLeft size={12} className="shrink-0 text-fg-muted" />;
  }
  if (
    t.includes("decimal") ||
    t.includes("numeric") ||
    t.includes("float") ||
    t.includes("double") ||
    t.includes("real") ||
    t.includes("money")
  ) {
    return <TextGlyph>.00</TextGlyph>;
  }
  if (t.includes("int") || t.includes("long") || t.includes("serial") || t.includes("number")) {
    return <TextGlyph>123</TextGlyph>;
  }
  // Strings and everything else (json / uuid / binary / unknown) fall
  // back to the alphabetic glyph, as Databricks does for text-like types.
  return (
    <TextGlyph>
      A<sup className="text-[7px]">ᵇ</sup>c
    </TextGlyph>
  );
}
