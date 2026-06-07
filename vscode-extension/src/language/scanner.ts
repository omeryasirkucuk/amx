// Catalog-anchored SQL tokenizer. Single pass, no parse tree and no
// grammar dependency: it only finds identifier chains and tags the
// syntactic slot they occupy (table position, column position, alias
// definition, CTE definition). The catalog resolver decides what the
// chains actually mean. Pure module — no vscode imports.
import { RESERVED_KEYWORDS, TABLE_TRIGGER_KEYWORDS, WITH_LIST_KEYWORDS } from "./keywords";
import type { ScanResult, SqlToken, StatementInfo, TokenPart } from "./types";

const MAX_CHAIN_PARTS = 4;

function isIdentStart(ch: string): boolean {
  return /[A-Za-z_]/.test(ch);
}

function isIdentChar(ch: string): boolean {
  return /[A-Za-z0-9_$]/.test(ch);
}

function isSpace(ch: string): boolean {
  return ch === " " || ch === "\t" || ch === "\r" || ch === "\n";
}

/** Scan SQL text into identifier tokens plus per-statement metadata. */
export function scanSql(text: string): ScanResult {
  return new Scanner(text).run();
}

/** Find the token whose character range contains the given offset. */
export function tokenAt(scan: ScanResult, offset: number): SqlToken | undefined {
  return scan.tokens.find((token) => {
    const first = token.parts[0];
    const last = token.parts[token.parts.length - 1];
    return first !== undefined && last !== undefined && offset >= first.start && offset <= last.end;
  });
}

class Scanner {
  private i = 0;
  private readonly tokens: SqlToken[] = [];
  private readonly statements: StatementInfo[] = [];

  // Per-statement context state.
  private statementStart = 0;
  private aliases = new Map<string, string>();
  private ctes = new Set<string>();
  private parenDepth = 0;
  /** Next identifier chain sits in table position. */
  private pendingTable = false;
  /** A table chain just ended — a bare identifier now is its alias. */
  private afterTableChain = false;
  /** Lowercased dotted text of the chain behind afterTableChain. */
  private lastTableChain = "";
  /** Inside a FROM-list at this depth — comma re-arms pendingTable. */
  private inTableList = false;
  private tableListDepth = 0;
  /** Inside a WITH list; next identifier at expectCte is a CTE name. */
  private inWithList = false;
  private withDepth = 0;
  private expectCte = false;

  constructor(private readonly text: string) {}

  run(): ScanResult {
    const text = this.text;
    const len = text.length;
    while (this.i < len) {
      const ch = text[this.i]!;
      const next = text[this.i + 1];
      if (ch === "-" && next === "-") {
        this.skipLineComment();
      } else if (ch === "#") {
        this.skipLineComment();
      } else if (ch === "/" && next === "*") {
        this.skipBlockComment();
      } else if (ch === "{" && (next === "{" || next === "%")) {
        this.skipJinja(next === "{" ? "}}" : "%}");
      } else if (ch === '"' && next === '"' && text[this.i + 2] === '"') {
        this.skipDelimited('"""');
      } else if (ch === "'") {
        this.skipQuoted("'");
      } else if (ch === "$" && next === "$") {
        this.skipDelimited("$$");
      } else if (ch === ";" && this.parenDepth === 0) {
        this.endStatement(this.i);
        this.i += 1;
        this.statementStart = this.i;
      } else if (ch === "(") {
        this.parenDepth += 1;
        this.pendingTable = false;
        this.afterTableChain = false;
        this.i += 1;
      } else if (ch === ")") {
        this.parenDepth = Math.max(0, this.parenDepth - 1);
        this.afterTableChain = false;
        this.i += 1;
      } else if (ch === ",") {
        this.handleComma();
        this.i += 1;
      } else if (isIdentStart(ch) || ch === '"' || ch === "`" || ch === "[") {
        this.handleChain();
      } else if (/[0-9]/.test(ch)) {
        // Numeric literal: swallow the digits plus any identifier
        // tail (1e5, 0x1f) so the tail never reads as an identifier.
        while (this.i < len && (isIdentChar(text[this.i]!) || text[this.i] === ".")) this.i += 1;
      } else {
        if (!isSpace(ch)) this.afterTableChain = false;
        this.i += 1;
      }
    }
    this.endStatement(len);
    return { tokens: this.tokens, statements: this.statements };
  }

  // --- skipping ---

  private skipLineComment(): void {
    const nl = this.text.indexOf("\n", this.i);
    this.i = nl === -1 ? this.text.length : nl + 1;
  }

  private skipBlockComment(): void {
    const close = this.text.indexOf("*/", this.i + 2);
    this.i = close === -1 ? this.text.length : close + 2;
  }

  private skipJinja(closer: string): void {
    const close = this.text.indexOf(closer, this.i + 2);
    this.i = close === -1 ? this.text.length : close + closer.length;
  }

  /** Skip until `closer` (no escaping inside): $$…$$ and """…""". */
  private skipDelimited(closer: string): void {
    const close = this.text.indexOf(closer, this.i + closer.length);
    this.i = close === -1 ? this.text.length : close + closer.length;
  }

  /** Skip a quoted run where a doubled quote is an escape. */
  private skipQuoted(quote: string): void {
    let j = this.i + 1;
    const len = this.text.length;
    while (j < len) {
      if (this.text[j] === quote) {
        if (this.text[j + 1] === quote) {
          j += 2;
          continue;
        }
        j += 1;
        break;
      }
      j += 1;
    }
    this.i = Math.min(j, len);
  }

  // --- structure ---

  private endStatement(end: number): void {
    this.statements.push({
      start: this.statementStart,
      end,
      aliases: this.aliases,
      ctes: this.ctes,
    });
    this.aliases = new Map();
    this.ctes = new Set();
    this.parenDepth = 0;
    this.pendingTable = false;
    this.afterTableChain = false;
    this.lastTableChain = "";
    this.inTableList = false;
    this.inWithList = false;
    this.expectCte = false;
  }

  private handleComma(): void {
    if (this.inWithList && this.parenDepth === this.withDepth) {
      this.expectCte = true;
    } else if (this.inTableList && this.parenDepth === this.tableListDepth) {
      this.pendingTable = true;
    }
    this.afterTableChain = false;
  }

  // --- identifiers ---

  private handleChain(): void {
    const parts = this.readChainParts();
    if (parts.length === 0) return;
    const single = parts.length === 1 ? parts[0] : undefined;
    if (single && !single.quoted && RESERVED_KEYWORDS.has(single.text.toUpperCase())) {
      this.handleKeyword(single.text.toUpperCase());
      return;
    }
    if (this.followedByParen()) {
      // Function or table-function call — never a catalog reference.
      this.pendingTable = false;
      this.afterTableChain = false;
      return;
    }
    const statementIndex = this.statements.length;
    if (this.expectCte) {
      this.expectCte = false;
      this.pendingTable = false;
      this.afterTableChain = false;
      if (single) this.ctes.add(single.text.toLowerCase());
      this.tokens.push({ parts, context: "cteDef", statementIndex });
      return;
    }
    if (this.pendingTable) {
      this.pendingTable = false;
      this.afterTableChain = true;
      this.lastTableChain = parts.map((p) => p.text.toLowerCase()).join(".");
      this.inTableList = true;
      this.tableListDepth = this.parenDepth;
      this.tokens.push({ parts, context: "tablePosition", statementIndex });
      return;
    }
    if (this.afterTableChain && single) {
      this.afterTableChain = false;
      this.aliases.set(single.text.toLowerCase(), this.lastTableChain);
      this.tokens.push({ parts, context: "aliasDef", statementIndex });
      return;
    }
    this.afterTableChain = false;
    this.tokens.push({ parts, context: "columnPosition", statementIndex });
  }

  private handleKeyword(keyword: string): void {
    if (keyword === "WITH") {
      this.inWithList = true;
      this.withDepth = this.parenDepth;
      this.expectCte = true;
      this.pendingTable = false;
      this.afterTableChain = false;
      return;
    }
    if (keyword === "AS") {
      // `FROM t AS x` keeps the alias slot open; `WITH x AS (` keeps
      // the WITH list alive. AS changes no state.
      return;
    }
    if (
      this.inWithList &&
      this.parenDepth === this.withDepth &&
      !WITH_LIST_KEYWORDS.has(keyword)
    ) {
      // Main query started (`WITH … SELECT`): the WITH list is over.
      this.inWithList = false;
      this.expectCte = false;
    }
    this.afterTableChain = false;
    this.inTableList = false;
    this.pendingTable = TABLE_TRIGGER_KEYWORDS.has(keyword);
  }

  private readChainParts(): TokenPart[] {
    const parts: TokenPart[] = [];
    for (;;) {
      const part = this.readPart();
      if (!part) break;
      parts.push(part);
      if (parts.length >= MAX_CHAIN_PARTS) break;
      const dot = this.peekPastSpaces();
      if (dot !== ".") break;
      this.skipSpaces();
      this.i += 1; // consume the dot
      this.skipSpaces();
    }
    return parts;
  }

  private readPart(): TokenPart | undefined {
    const text = this.text;
    const start = this.i;
    const ch = text[this.i];
    if (ch === undefined) return undefined;
    if (ch === '"' || ch === "`" || ch === "[") {
      const closer = ch === "[" ? "]" : ch;
      let j = this.i + 1;
      let value = "";
      while (j < text.length) {
        const c = text[j]!;
        if (c === closer) {
          if (closer !== "]" && text[j + 1] === closer) {
            value += closer;
            j += 2;
            continue;
          }
          j += 1;
          break;
        }
        value += c;
        j += 1;
      }
      this.i = j;
      return { text: value, start, end: j, quoted: true };
    }
    if (!isIdentStart(ch)) return undefined;
    let j = this.i + 1;
    while (j < text.length && isIdentChar(text[j]!)) j += 1;
    this.i = j;
    return { text: text.slice(start, j), start, end: j, quoted: false };
  }

  private peekPastSpaces(): string | undefined {
    let j = this.i;
    while (j < this.text.length && isSpace(this.text[j]!)) j += 1;
    return this.text[j];
  }

  private skipSpaces(): void {
    while (this.i < this.text.length && isSpace(this.text[this.i]!)) this.i += 1;
  }

  private followedByParen(): boolean {
    return this.peekPastSpaces() === "(";
  }
}
