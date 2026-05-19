# Team Workspace Concept

## What is the team workspace?

The **team workspace** is AMX's shared backend that stores history runs,
lineage diagrams, and documentation pages for **all** your database
profiles in one place. It is controlled by the `history_store_profile`
config key and activated with the `/history-store enable` CLI command.

A single database profile hosts the shared store — not one per DB profile.
This means your team's entire AMX output (regardless of which database
was analyzed) is visible in one shared location.

## Why one shared backend?

- Every DB profile's analysis results, diagrams, and pages feed into the
  same workspace, so teammates see each other's work without switching
  contexts.
- Access control (viewer vs. admin roles) is enforced at the workspace
  level, not per-profile.
- The audit trail captures all changes across every profile in a single
  chronological log.

## Enabling the workspace

From the AMX CLI:

```
/history-store enable
```

AMX will prompt for the profile to use as the workspace host. Choose a
database that is reachable by all team members (e.g. a shared Postgres or
Snowflake instance, not a personal local DuckDB file).

Backends that support shared history: PostgreSQL, MySQL / MariaDB,
Snowflake, BigQuery, Redshift, Databricks, CockroachDB, Azure SQL.

SQLite is local-only and cannot serve as a shared workspace.

## Migration: auto-backfill on first connect

When you enable the shared workspace, AMX automatically migrates your
existing local lineage diagrams and documentation pages into the shared
store. This happens in the background; the Studio shows a banner
("Migrating your local lineage and pages to the team workspace…") while
the migration is in progress.

To retry a failed migration manually, run:

```
/history sync-local
```

## Permissions: admin and viewer roles

The workspace has two roles:

| Role   | Read | Write | Member management | Audit log |
|--------|------|-------|-------------------|-----------|
| admin  |  Yes |  Yes  |        Yes        |    Yes    |
| viewer |  Yes |  Yes  |        No         |    Yes    |

The first user to connect automatically becomes an admin. Admins can
promote other members to admin or demote them back to viewer.

### Promoting a member

From the CLI:

```
/admin promote <username>
```

From Studio: open **Workspace Admin → Members** and click the shield icon
on the member's row.

### Audit log

Every admin action (promote, demote, revoke, forced overwrites) is
recorded in the audit log. View it from Studio under
**Workspace Admin → Audit log**, or from the CLI with `/admin audit`.

## Conflict resolution

When two team members edit the same lineage comment or documentation page
body simultaneously, AMX uses optimistic concurrency control (OCC) to
detect the conflict. The second writer sees a conflict dialog (Studio) or
an interactive prompt (CLI) with four choices:

- **Cancel** — discard your change, keep the current version
- **Keep theirs** — reload with the current server version
- **Overwrite with mine** — force-write your version
- **Edit my version** — return to editing with both versions visible

For non-interactive use, pass `--on-conflict=cancel|overwrite|fail` to
any CLI command that performs an OCC-protected update.
