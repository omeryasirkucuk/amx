/**
 * Maps a DB profile's ``backend`` field to the registry ``key`` of the
 * default logo we want to auto-bind on the DataFrameNode header.
 *
 * Lowercase normalisation happens at the call site. Backends that
 * don't map to a shipped default (DuckDB, SQL Server, ClickHouse, …)
 * fall through to "no badge"; the user can still pick one manually.
 */

export const BACKEND_TO_LOGO: Record<string, string> = {
  postgresql: "postgres",
  postgres: "postgres",
  mysql: "mysql",
  snowflake: "snowflake",
  databricks: "databricks",
  bigquery: "bigquery",
  redshift: "redshift",
  trino: "trino",
  presto: "trino",
  hive: "hive",
};

export function logoKeyForBackend(backend: string | undefined | null): string {
  if (!backend) return "";
  return BACKEND_TO_LOGO[String(backend).toLowerCase()] ?? "";
}
