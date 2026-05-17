"""Default logo library — placeholder SVGs generated from a manifest.

The library ships 20 brand entries spanning cloud providers, warehouses,
BI tools, and data tooling. Each entry is rendered into an SVG at seed
time (see :func:`render_logo_svg`) — a brand-colored rounded square
with the brand's 1- or 2-letter initial centered in white. Treat the
visuals as placeholders: users replace any of them through the custom
upload UI without touching this module.

Why generated, not bundled binaries:

  * One round-trip for visual review (read this module, see the brand
    palette).
  * No license headaches with corporate trademarks bundled into the
    source tree — placeholders are pure shape + text.
  * The shadow rule (same ``key`` + ``source='custom'``) makes the
    upgrade path obvious: drop a real SVG into the Studio picker.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Category = Literal["cloud", "warehouse", "bi", "tooling"]


@dataclass(frozen=True)
class DefaultLogo:
    """One row in the default library.

    Two icon source slugs let the seeder fall back gracefully:

    * ``simpleicons_slug`` — first preference (single-color brand
      mark, smallest payload). SimpleIcons has dropped a number of
      brands for trademark reasons (AWS, Azure, Power BI, Tableau,
      Redshift, dbt, …) so a slug here may 404.
    * ``iconify_slug`` — fallback in ``<set>:<name>`` form (e.g.
      ``logos:aws``, ``mdi:microsoft-sharepoint``). Iconify is much
      wider and ships multicolor logos in its ``logos`` collection.

    ``initials`` is the offline / hard-fallback used by
    :func:`render_logo_svg` when both fetches fail — brand-colored
    square + initials.
    """

    key: str
    label: str
    category: Category
    color: str  # CSS hex — placeholder fill + SimpleIcons tint
    initials: str  # last-resort fallback
    simpleicons_slug: str  # ``""`` when SimpleIcons has no entry
    iconify_slug: str = ""  # ``set:name``; ``""`` when Iconify also lacks it


# Brand colors are deliberately conservative — these match each brand's
# documented primary palette so the placeholder + tinted variants still
# read as the right product even when the real mark is unavailable.
DEFAULT_LOGOS: tuple[DefaultLogo, ...] = (
    # Cloud providers
    DefaultLogo(
        "aws",
        "Amazon Web Services",
        "cloud",
        "#FF9900",
        "AWS",
        "amazonwebservices",
        "logos:aws",
    ),
    DefaultLogo(
        "gcp",
        "Google Cloud",
        "cloud",
        "#4285F4",
        "GCP",
        "googlecloud",
        "logos:google-cloud",
    ),
    DefaultLogo(
        "azure",
        "Microsoft Azure",
        "cloud",
        "#0078D4",
        "AZ",
        "microsoftazure",
        "logos:microsoft-azure",
    ),
    # Warehouses + databases
    DefaultLogo(
        "snowflake",
        "Snowflake",
        "warehouse",
        "#29B5E8",
        "SF",
        "snowflake",
        "logos:snowflake-icon",
    ),
    DefaultLogo(
        "databricks",
        "Databricks",
        "warehouse",
        "#FF3621",
        "DB",
        "databricks",
        "logos:databricks-icon",
    ),
    DefaultLogo(
        "bigquery",
        "BigQuery",
        "warehouse",
        "#669DF6",
        "BQ",
        "googlebigquery",
        "logos:google-bigquery",
    ),
    DefaultLogo(
        "postgres",
        "PostgreSQL",
        "warehouse",
        "#336791",
        "PG",
        "postgresql",
        "logos:postgresql",
    ),
    DefaultLogo(
        "mysql",
        "MySQL",
        "warehouse",
        "#00758F",
        "MY",
        "mysql",
        "logos:mysql-icon",
    ),
    DefaultLogo(
        "redshift",
        "Amazon Redshift",
        "warehouse",
        "#C8232C",
        "RS",
        "amazonredshift",
        "logos:aws-redshift",
    ),
    # BI / sinks
    DefaultLogo(
        "powerbi",
        "Power BI",
        "bi",
        "#F2C811",
        "PB",
        "powerbi",
        "logos:microsoft-power-bi",
    ),
    DefaultLogo(
        "tableau",
        "Tableau",
        "bi",
        "#1F77B4",
        "TB",
        "tableau",
        "logos:tableau-icon",
    ),
    DefaultLogo(
        "looker",
        "Looker",
        "bi",
        "#4285F4",
        "LK",
        "looker",
        "logos:looker-icon",
    ),
    DefaultLogo(
        "metabase",
        "Metabase",
        "bi",
        "#509EE3",
        "MB",
        "metabase",
        "logos:metabase",
    ),
    DefaultLogo(
        "superset",
        "Apache Superset",
        "bi",
        "#20A7C9",
        "SS",
        "apachesuperset",
        "logos:apache-superset",
    ),
    # Data tooling
    DefaultLogo(
        "dbt",
        "dbt",
        "tooling",
        "#FF694A",
        "dbt",
        "dbt",
        "logos:dbt-icon",
    ),
    DefaultLogo(
        "airflow",
        "Apache Airflow",
        "tooling",
        "#E32A77",
        "AF",
        "apacheairflow",
        "logos:airflow-icon",
    ),
    # Fivetran has no Iconify or SimpleIcons hit — placeholder until
    # the user shadows it via custom upload.
    DefaultLogo(
        "fivetran",
        "Fivetran",
        "tooling",
        "#0073B6",
        "FT",
        "fivetran",
        "",
    ),
    DefaultLogo(
        "kafka",
        "Apache Kafka",
        "tooling",
        "#231F20",
        "KA",
        "apachekafka",
        "logos:kafka-icon",
    ),
    DefaultLogo(
        "spark",
        "Apache Spark",
        "tooling",
        "#E25A1C",
        "SP",
        "apachespark",
        "logos:apache-spark",
    ),
    # Apache Iceberg also has no official iconify entry — placeholder.
    DefaultLogo(
        "iceberg",
        "Apache Iceberg",
        "tooling",
        "#0E7C7B",
        "IB",
        "apacheiceberg",
        "",
    ),
    # External collaboration / consulting
    DefaultLogo(
        "sharepoint",
        "Microsoft SharePoint",
        "tooling",
        "#0078D4",
        "SP",
        "microsoftsharepoint",
        "mdi:microsoft-sharepoint",
    ),
    # Dfive ships only as a placeholder until the user uploads the real
    # mark via the picker's "Add Custom" tab.
    DefaultLogo("dfive", "Dfive", "tooling", "#1B9AAA", "D5", "", ""),
)


def render_logo_svg(logo: DefaultLogo) -> str:
    """Render a 128x128 placeholder SVG for one logo.

    The shape is intentionally simple — a brand-colored rounded square
    with white initials. Wider initial strings get a smaller font so
    three-letter brands like 'AWS' or 'GCP' still fit cleanly.
    """
    text_len = max(len(logo.initials), 1)
    if text_len == 1:
        font_size = 68
    elif text_len == 2:
        font_size = 52
    else:
        font_size = 38
    safe_initials = logo.initials.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    # ``<!-- amx-placeholder -->`` marker lets ``seed_default_logos``
    # tell its own placeholder output apart from a real fetched logo.
    # Without it, a re-seed after a network outage could not upgrade
    # rows that previously fell back to the placeholder branch.
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 128 128" '
        'width="128" height="128">'
        "<!-- amx-placeholder -->"
        f'<rect x="4" y="4" width="120" height="120" rx="20" ry="20" '
        f'fill="{logo.color}"/>'
        f'<text x="64" y="64" font-family="-apple-system, BlinkMacSystemFont, '
        f'\'Segoe UI\', Inter, sans-serif" font-weight="700" '
        f'font-size="{font_size}" fill="#ffffff" text-anchor="middle" '
        f'dominant-baseline="central">{safe_initials}</text>'
        "</svg>"
    )


# Public marker the seed reads back from stored data URLs to know
# whether a row is a real fetched logo or a placeholder waiting for
# a fresh attempt.
PLACEHOLDER_MARKER = "amx-placeholder"


__all__ = ["DEFAULT_LOGOS", "DefaultLogo", "PLACEHOLDER_MARKER", "render_logo_svg"]
