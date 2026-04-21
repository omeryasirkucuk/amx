-- =============================================================================
-- AMX test fixture: non-trivial SQL around SAP billing header VBRK
-- Purpose: exercise codebase/SQL scanners (qualified names, joins, CTEs, windows)
-- Not executable as-is without your catalog — safe to point AMX /code-scan at this folder.
-- =============================================================================

SET search_path TO sap_s6p, public;

/* ---------------------------------------------------------------------------
   Layer 1: base extracts — billing documents with organizational context
   Typical ETL: land raw → conform → publish to analytics schema
--------------------------------------------------------------------------- */

WITH billing_header_core AS (
    SELECT
        h.mandt,
        h.vbeln,
        h.fkdat::date AS billing_date,
        h.waerk,
        h.netwr,
        h.mwsbk,
        h.bukrs,
        h.kunrg AS payer_kunnr,
        h.kunag AS sold_to_kunnr,
        h.fkart,
        h.fksto,
        h.rfbsk
    FROM sap_s6p.vbrk AS h
    WHERE h.mandt = '100'
      AND h.fksto <> 'X'          -- exclude cancelled billing docs in this slice
      AND h.fkdat >= CURRENT_DATE - INTERVAL '730 days'
),

-- Item-level rollups joined back to header (VBRP = billing item; referenced for realism)
line_agg AS (
    SELECT
        i.mandt,
        i.vbeln,
        COUNT(*) AS line_count,
        SUM(i.fkimg) AS total_qty,
        SUM(i.netwr) AS lines_netwr
    FROM sap_s6p.vbrp AS i
    INNER JOIN billing_header_core bh
        ON bh.mandt = i.mandt
       AND bh.vbeln = i.vbeln
    GROUP BY i.mandt, i.vbeln
),

-- Enriched header: attach aggregates + derived KPIs
enriched AS (
    SELECT
        bh.*,
        COALESCE(la.line_count, 0) AS line_count,
        COALESCE(la.total_qty, 0) AS billed_quantity,
        CASE
            WHEN bh.netwr IS NULL OR bh.netwr = 0 THEN NULL
            ELSE ROUND((bh.mwsbk / NULLIF(bh.netwr, 0))::numeric, 4)
        END AS implicit_tax_ratio
    FROM billing_header_core bh
    LEFT JOIN line_agg la
        ON la.mandt = bh.mandt
       AND la.vbeln = bh.vbeln
),

-- Partitioned metrics: rolling company currency exposure (illustrative)
ranked AS (
    SELECT
        e.*,
        ROW_NUMBER() OVER (
            PARTITION BY e.bukrs, e.waerk
            ORDER BY e.billing_date DESC, e.vbeln
        ) AS rn_recent_per_ccy,
        SUM(e.netwr) OVER (
            PARTITION BY e.bukrs
            ORDER BY e.billing_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_netwr_bukrs
    FROM enriched e
)

SELECT
    r.mandt,
    r.vbeln,
    r.billing_date,
    r.bukrs,
    r.waerk,
    r.netwr,
    r.mwsbk,
    r.line_count,
    r.billed_quantity,
    r.implicit_tax_ratio,
    r.rn_recent_per_ccy,
    r.cumulative_netwr_bukrs
FROM ranked r
WHERE r.rn_recent_per_ccy <= 500
ORDER BY r.billing_date DESC, r.vbeln;


-- =============================================================================
-- Second statement: anti-join pattern + EXISTS (different join topology)
-- =============================================================================

SELECT DISTINCT
    outer_hdr.vbeln,
    outer_hdr.fkdat,
    outer_hdr.kunag
FROM sap_s6p.vbrk AS outer_hdr
WHERE outer_hdr.mandt = '100'
  AND EXISTS (
        SELECT 1
        FROM sap_s6p.vbrk AS inner_dup
        WHERE inner_dup.mandt = outer_hdr.mandt
          AND inner_dup.kunag = outer_hdr.kunag
          AND inner_dup.vbeln <> outer_hdr.vbeln
          AND inner_dup.fkdat BETWEEN outer_hdr.fkdat - 7 AND outer_hdr.fkdat + 7
    )
  AND NOT EXISTS (
        SELECT 1
        FROM sap_s6p.vbrk AS cancelled
        WHERE cancelled.mandt = outer_hdr.mandt
          AND cancelled.vbeln = outer_hdr.vbeln
          AND cancelled.fksto = 'X'
    );


-- =============================================================================
-- Third: subquery in SELECT list + double-quoted identifiers (dialect portability)
-- =============================================================================

SELECT
    "sap_s6p"."vbrk"."VBELN" AS document_number,
    (
        SELECT MAX(sub.fkdat)
        FROM sap_s6p.vbrk AS sub
        WHERE sub.mandt = "sap_s6p"."vbrk".mandt
          AND sub.bukrs = "sap_s6p"."vbrk".bukrs
    ) AS max_fkdat_same_company
FROM sap_s6p.vbrk
WHERE mandt = '100'
LIMIT 100;


-- =============================================================================
-- Fourth: UNION ALL across two schema-qualified references to same logical table
-- (tests duplicate table extraction / dedup in tooling)
-- =============================================================================

SELECT 'A' AS src, vbeln, netwr FROM sap_s6p.vbrk WHERE mandt = '100' AND fkart IN ('F2', 'G2')
UNION ALL
SELECT 'B' AS src, vbeln, netwr FROM public.vbrk WHERE 1 = 0  -- second branch intentionally empty for sample
;


-- =============================================================================
-- Inline note for PySpark / notebook migration (often copied into repos):
-- spark.read.table("sap_s6p.vbrk").filter(col("mandt") === lit("100"))
-- =============================================================================
