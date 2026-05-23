"""Platform system-table lineage extractors.

One module per warehouse family. Each module exposes a class whose
``extract_for_profile`` reads the warehouse's own lineage system
tables and writes the resulting edges into the local store, plus a
``build_query_runner`` factory the dispatcher uses to lazily resolve
the adapter for a given profile.

Databricks lands first; Snowflake (``ACCOUNT_USAGE.OBJECT_DEPENDENCIES``)
and BigQuery (``INFORMATION_SCHEMA.JOBS_BY_PROJECT``) follow the same
shape when they get built.
"""

from __future__ import annotations

__all__: list[str] = []
