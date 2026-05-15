# Products catalogue

The `products` table is the authoritative list of items the storefront
can sell. Variants (size, colour) are stored in `product_variants`.

## Columns

- `product_id` — surrogate primary key.
- `sku` — vendor-assigned stock keeping unit. Unique. Used by the
  warehouse system as the join key; never reuse a SKU even for a
  rebadged product.
- `name` — display name shown to shoppers.
- `description_md` — long-form Markdown description; rendered to HTML
  at request time, never stored as HTML.
- `category_id` — foreign key into `categories`.
- `is_active` — boolean. `false` hides the product from search but does
  not delete its history; analytics queries must filter on this column
  if they want only currently-listed items.
- `created_at`, `updated_at` — audit timestamps in UTC.

## Variants

Each row in `product_variants` carries its own SKU, price, and stock
level. Joining `products` to `product_variants` on `product_id` is the
usual pattern; do not assume one variant per product.

## Pricing

Prices live on the variant, not the product. The product page shows a
"from" price computed as `MIN(variant.price)` across active variants.
