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
    """One row in the default library."""

    key: str
    label: str
    category: Category
    color: str  # background fill (CSS hex)
    initials: str  # rendered in white in the center


DEFAULT_LOGOS: tuple[DefaultLogo, ...] = (
    # Cloud providers
    DefaultLogo("aws", "Amazon Web Services", "cloud", "#FF9900", "AWS"),
    DefaultLogo("gcp", "Google Cloud", "cloud", "#4285F4", "GCP"),
    DefaultLogo("azure", "Microsoft Azure", "cloud", "#0078D4", "AZ"),
    # Warehouses + databases
    DefaultLogo("snowflake", "Snowflake", "warehouse", "#29B5E8", "SF"),
    DefaultLogo("databricks", "Databricks", "warehouse", "#FF3621", "DB"),
    DefaultLogo("bigquery", "BigQuery", "warehouse", "#669DF6", "BQ"),
    DefaultLogo("postgres", "PostgreSQL", "warehouse", "#336791", "PG"),
    DefaultLogo("mysql", "MySQL", "warehouse", "#00758F", "MY"),
    DefaultLogo("redshift", "Amazon Redshift", "warehouse", "#C8232C", "RS"),
    # BI / sinks
    DefaultLogo("powerbi", "Power BI", "bi", "#F2C811", "PB"),
    DefaultLogo("tableau", "Tableau", "bi", "#1F77B4", "TB"),
    DefaultLogo("looker", "Looker", "bi", "#4285F4", "LK"),
    DefaultLogo("metabase", "Metabase", "bi", "#509EE3", "MB"),
    DefaultLogo("superset", "Apache Superset", "bi", "#20A7C9", "SS"),
    # Data tooling
    DefaultLogo("dbt", "dbt", "tooling", "#FF694A", "dbt"),
    DefaultLogo("airflow", "Apache Airflow", "tooling", "#E32A77", "AF"),
    DefaultLogo("fivetran", "Fivetran", "tooling", "#0073B6", "FT"),
    DefaultLogo("kafka", "Apache Kafka", "tooling", "#231F20", "KA"),
    DefaultLogo("spark", "Apache Spark", "tooling", "#E25A1C", "SP"),
    DefaultLogo("iceberg", "Apache Iceberg", "tooling", "#0E7C7B", "IB"),
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
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 128 128" '
        'width="128" height="128">'
        f'<rect x="4" y="4" width="120" height="120" rx="20" ry="20" '
        f'fill="{logo.color}"/>'
        f'<text x="64" y="64" font-family="-apple-system, BlinkMacSystemFont, '
        f'\'Segoe UI\', Inter, sans-serif" font-weight="700" '
        f'font-size="{font_size}" fill="#ffffff" text-anchor="middle" '
        f'dominant-baseline="central">{safe_initials}</text>'
        "</svg>"
    )


__all__ = ["DEFAULT_LOGOS", "DefaultLogo", "render_logo_svg"]
