// Area registry for the Studio webview panels. Each area maps one
// VS Code webview panel (singleton per area) onto a deep link into
// the Studio SPA. Route strings mirror the SPA route table in
// frontend/src/App.tsx — keep the two in sync when routes move.
export type PanelArea =
  | "ask"
  | "run"
  | "runDetail"
  | "lineage"
  | "pages"
  | "settings"
  | "table";

/** Arguments accepted by area route builders. All optional; each
 *  builder degrades to the closest parent route when keys are
 *  missing (e.g. a table link without a database falls back to the
 *  profile browse page). */
export interface PanelAreaArgs {
  readonly runId?: string;
  readonly profile?: string;
  readonly database?: string;
  readonly catalog?: string;
  readonly schema?: string;
  readonly table?: string;
}

export interface AreaDefinition {
  readonly area: PanelArea;
  /** Serializer registration key, "amx.panel.<area>". */
  readonly viewType: string;
  readonly buildTitle: (args: PanelAreaArgs) => string;
  readonly buildRoute: (args: PanelAreaArgs) => string;
}

const seg = (value: string): string => encodeURIComponent(value);

// Browse paths encode the full scope (see App.tsx):
//   /db/:profile/:database/:schema/:table   (2-level engines)
//   /cat/:profile/:catalog/:schema/:table   (3-level engines)
// A table link missing its database/catalog segment cannot reach the
// detail page, so it degrades to the profile browse page (or the
// landing page when even the profile is unknown).
function tableRoute(args: PanelAreaArgs): string {
  const { profile, database, catalog, schema, table } = args;
  if (profile && schema && table) {
    if (catalog) return `/cat/${seg(profile)}/${seg(catalog)}/${seg(schema)}/${seg(table)}`;
    if (database) return `/db/${seg(profile)}/${seg(database)}/${seg(schema)}/${seg(table)}`;
  }
  if (profile) return `/db/${seg(profile)}`;
  return "/";
}

function tableTitle(args: PanelAreaArgs): string {
  if (args.schema && args.table) return `AMX: ${args.schema}.${args.table}`;
  return "AMX Catalog";
}

function define(
  area: PanelArea,
  buildTitle: (args: PanelAreaArgs) => string,
  buildRoute: (args: PanelAreaArgs) => string,
): AreaDefinition {
  return { area, viewType: `amx.panel.${area}`, buildTitle, buildRoute };
}

export const AREA_DEFINITIONS: Readonly<Record<PanelArea, AreaDefinition>> = {
  ask: define("ask", () => "AMX Ask", () => "/ask"),
  run: define("run", () => "AMX New Run", () => "/runs/new"),
  runDetail: define(
    "runDetail",
    (args) => (args.runId ? `AMX Run ${args.runId}` : "AMX Runs"),
    (args) => (args.runId ? `/runs/${seg(args.runId)}` : "/runs"),
  ),
  lineage: define("lineage", () => "AMX Lineage", () => "/lineage"),
  pages: define("pages", () => "AMX Pages", () => "/pages"),
  settings: define("settings", () => "AMX Studio Settings", () => "/settings"),
  table: define("table", tableTitle, tableRoute),
};

export function isPanelArea(value: unknown): value is PanelArea {
  return typeof value === "string" && value in AREA_DEFINITIONS;
}

const ARG_KEYS = ["runId", "profile", "database", "catalog", "schema", "table"] as const;

/** Narrow untrusted input (command arguments, restored webview
 *  state) to the known string-valued argument keys. */
export function coercePanelArgs(value: unknown): PanelAreaArgs {
  if (typeof value !== "object" || value === null) return {};
  const source = value as Record<string, unknown>;
  const result: { -readonly [K in keyof PanelAreaArgs]?: string } = {};
  for (const key of ARG_KEYS) {
    const candidate = source[key];
    if (typeof candidate === "string" && candidate.length > 0) result[key] = candidate;
  }
  return result;
}
