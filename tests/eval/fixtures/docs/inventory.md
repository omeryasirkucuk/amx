# Inventory tracking

Inventory levels live in `product_variants.stock_on_hand` for the
single-warehouse case. Multi-warehouse deployments use the
`inventory_levels` table joined to `product_variants` on `variant_id`.

## Stock columns

- `stock_on_hand` — physical units currently in the warehouse.
- `stock_reserved` — units allocated to in-flight orders that have not
  yet shipped. Subtract from `stock_on_hand` to get available-to-sell.
- `reorder_threshold` — when `stock_on_hand` drops below this number,
  the procurement job creates a draft purchase order.
- `last_counted_at` — UTC timestamp of the most recent cycle count.

## Reservation lifecycle

1. Cart checkout: stock is *not* reserved (carts can expire).
2. Order placed (`orders.status='pending'`): stock reserved.
3. Order shipped: reservation released; `stock_on_hand` decremented.
4. Order cancelled before ship: reservation released, no decrement.

## Common bugs

Phantom stock — when `stock_reserved` exceeds `stock_on_hand` — almost
always means a cancelled order's reservation never released. The
`stuck_reservations_v` view surfaces these.
