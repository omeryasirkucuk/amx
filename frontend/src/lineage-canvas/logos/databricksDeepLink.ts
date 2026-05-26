/**
 * Builds an "open in Databricks" URL for a lineage canvas node.
 *
 * Tables link to Catalog Explorer by their 3-part FQN; assets link by
 * their platform external id. Returns null when the host is unknown or
 * the node kind has no workspace destination, so callers render no link
 * rather than a broken one.
 */

interface DeepLinkArgs {
  kind: string;
  host: string | undefined;
  fqn?: string;
  externalId?: string | undefined;
}

function normalizeHost(host: string): string {
  const h = host.trim().replace(/\/+$/, "");
  return /^https?:\/\//.test(h) ? h : `https://${h}`;
}

export function databricksDeepLink(args: DeepLinkArgs): string | null {
  const { kind, fqn, externalId } = args;
  if (!args.host) return null;
  const base = normalizeHost(args.host);

  if (kind === "table") {
    const parts = (fqn || "").split(".").filter(Boolean);
    if (parts.length !== 3) return null;
    return `${base}/explore/data/${parts[0]}/${parts[1]}/${parts[2]}`;
  }
  if (!externalId) return null;
  switch (kind) {
    case "notebook":
      return `${base}/editor/notebooks/${externalId}`;
    case "job":
      return `${base}/jobs/${externalId}`;
    case "pipeline":
      return `${base}/pipelines/${externalId}`;
    case "query":
      return `${base}/sql/editor/${externalId}`;
    default:
      return null;
  }
}
