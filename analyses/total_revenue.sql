with order as (
    select 
        *
    from {{ ref('stg_jaffle_shop__orders') }}
)
select subtotal, tax_paid, order_total 
from order
where subtotal + tax_paid = order_total