# ETL pipelines

The operational store described in the other documents is mirrored
nightly into the analytics warehouse. This page summarises the jobs
that own each table.

## Jobs

- `sync_orders_daily` — pulls all `orders` rows whose `placed_at` is
  within the last 7 days into `analytics.orders_daily`. Runs at 02:00
  UTC. Idempotent: re-running produces the same target rows.
- `sync_customers_full` — full snapshot of `customers` weekly on
  Sunday at 03:00 UTC. GDPR-erased rows ship through with PII columns
  nulled but `customer_id` intact.
- `sync_products_cdc` — change-data-capture stream from `products` and
  `product_variants` into the warehouse, near-real-time (15s lag).
- `sync_inventory_hourly` — hourly snapshot of `product_variants.stock_on_hand`
  into `analytics.inventory_hourly` for stock-trend dashboards.

## Failure modes

- A failed `sync_orders_daily` blocks the morning revenue dashboard.
  On-call should re-run by hand from the orchestrator UI; the job is
  safe to re-run any number of times.
- `sync_customers_full` over a weekend interacts with the GDPR job —
  always run the GDPR scrub *before* this job, never after, or scrubbed
  rows arrive at the warehouse before the erasure is recorded there.
