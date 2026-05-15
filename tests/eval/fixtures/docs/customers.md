# Customers table

The `customers` table holds one row per known buyer, including guest
checkouts (synthesised at checkout time with a placeholder email).

## Columns

- `customer_id` — surrogate primary key.
- `email` — the buyer's contact email. Unique among non-guest rows;
  guest rows share the sentinel value `guest+<order_id>@example.invalid`.
- `created_at` — UTC timestamp when the row first appeared.
- `country_code` — ISO-3166 alpha-2 country code derived from the
  shipping address on the first order.
- `consent_marketing` — boolean recording whether the customer opted
  into marketing emails. Defaults to false; toggled via the preference
  centre.
- `gdpr_erased_at` — UTC timestamp set when a GDPR erasure request
  scrubs `email`, `country_code`, and other PII. The row itself is
  retained so historical `orders.customer_id` references stay valid.

## Joins

- `orders` on `customer_id` — the customer's order history.
- `addresses` on `customer_id` — every address ever associated with the
  customer; the latest is used as the default for the next order.

## PII handling

`email` and any column under `addresses` are personally identifiable.
A nightly job removes detached rows older than 90 days that have no
linked orders. Erased rows keep `customer_id` so the foreign key chain
from `orders` does not break.
