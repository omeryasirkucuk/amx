# Orders table

The `orders` table stores one row per customer order placed through the
storefront. Each row represents a confirmed purchase intent — abandoned
carts live in a separate `carts` table and are not promoted here until
checkout completes.

## Columns

- `order_id` — surrogate primary key, monotonically increasing integer.
- `customer_id` — foreign key into `customers.customer_id`. Required;
  guest checkouts produce a synthetic customer row first so this column
  is never NULL.
- `placed_at` — UTC timestamp when the storefront accepted the order.
  Indexed for date-range queries.
- `total_amount` — the sum of all line items plus tax, in the order's
  currency. Discounts are already applied. Refunds do not mutate this
  value; they create a separate `refunds` row.
- `currency_code` — ISO-4217 three-letter currency identifier, e.g.
  `USD`, `EUR`, `TRY`.
- `status` — one of `pending`, `confirmed`, `shipped`, `delivered`,
  `cancelled`. State transitions are enforced in the `orders_state`
  trigger.

## Common joins

- `order_items` on `order_id` — line items including product, quantity,
  unit price, and any item-level discount.
- `customers` on `customer_id` — buyer profile and contact details.
- `payments` on `order_id` — the payment attempts for this order;
  multiple rows on retries.

## Retention

Orders are kept indefinitely for accounting purposes. Personal
identifiers in linked `customers` rows are scrubbed on GDPR request
without affecting `orders` rows themselves — order history remains a
business record even after the customer is forgotten.
