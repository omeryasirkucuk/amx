// Shared types for the SQL language features. Pure declarations —
// no vscode imports — so the scanner and resolver stay unit-testable
// in plain vitest.
import type { ColumnMeta, TableMeta } from "../api/catalogCache";

/** One identifier segment of a (possibly dotted) chain. */
export interface TokenPart {
  /** Unquoted identifier text exactly as written (case preserved). */
  text: string;
  /** Offset of the first character of the part (including quotes). */
  start: number;
  /** Offset one past the last character of the part. */
  end: number;
  /** True when the part was written with ", ` or [] quoting. */
  quoted: boolean;
}

/** Syntactic slot an identifier chain occupies in its statement. */
export type TokenContext = "tablePosition" | "columnPosition" | "aliasDef" | "cteDef";

/** An identifier chain (up to 4 dotted parts) found by the scanner. */
export interface SqlToken {
  parts: TokenPart[];
  context: TokenContext;
  /** Index into ScanResult.statements. */
  statementIndex: number;
}

/** Per-statement bookkeeping collected during the scan. */
export interface StatementInfo {
  /** Offset of the first character of the statement. */
  start: number;
  /** Offset one past the last character (the `;` or end of text). */
  end: number;
  /** Lowercased alias → lowercased dotted table chain it stands for. */
  aliases: Map<string, string>;
  /** Lowercased CTE names defined in this statement. */
  ctes: Set<string>;
}

export interface ScanResult {
  tokens: SqlToken[];
  statements: StatementInfo[];
}

/**
 * How certain the resolver is about a match. "exact" — one catalog
 * entry fits; "ambiguous" — several fit and candidates are listed;
 * "weak" — a single heuristic guess (e.g. bare table.column).
 */
export type ResolutionConfidence = "exact" | "ambiguous" | "weak";

export interface ResolvedTableRef {
  kind: "table";
  confidence: ResolutionConfidence;
  /** Best match (first candidate when ambiguous). */
  table: TableMeta;
  /** Every catalog entry that fits the reference. */
  candidates: TableMeta[];
}

export interface ColumnCandidate {
  table: TableMeta;
  column: ColumnMeta;
}

export interface ResolvedColumnRef {
  kind: "column";
  confidence: ResolutionConfidence;
  /** Owning table of the best match. */
  table: TableMeta;
  /** Best match (first candidate when ambiguous). */
  column: ColumnMeta;
  candidates: ColumnCandidate[];
}

export type ResolvedRef = ResolvedTableRef | ResolvedColumnRef;
