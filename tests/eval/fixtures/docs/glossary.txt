# Business glossary

A small set of terms that recur across tables and dashboards.

## GMV (Gross Merchandise Value)

The sum of `orders.total_amount` for orders whose `status` is `confirmed`
or later. Refunds are not subtracted — refunds reduce *net* revenue,
not GMV.

## AOV (Average Order Value)

`GMV / count(orders)` over the same window. Excludes cancelled orders
from both numerator and denominator.

## Conversion rate

The ratio of unique customers who completed an order to unique
sessions in the same window. Sessions are tracked outside this
database (in the analytics warehouse); the storefront only sees
authenticated customers.

## Churn

A customer is considered churned when their last order is more than
180 days ago and they have opened no support tickets since. The
`customer_status_v` view computes this on the fly.

## ATC (Add To Cart)

An event in the analytics warehouse, not a column in this database.
Mentioned in dashboards that pivot funnel metrics by category.

## SKU

Stock Keeping Unit. Lives on `products.sku` and on each row of
`product_variants.sku`. The warehouse system treats SKU as the source
of truth; do not change a SKU after the variant has shipped.
