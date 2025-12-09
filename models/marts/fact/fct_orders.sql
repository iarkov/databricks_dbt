{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with order as (
    select 
        order_id,
        customer_id,
        order_date,
        store_id,
        subtotal,
        tax_paid,
        order_total
    from {{ ref('stg_jaffle_shop__orders') }}
)
select 
    order.order_id,
    order.customer_id,
    order.order_date,
    order.store_id,
    order.subtotal,
    order.tax_paid,
    order.order_total
from order
/*{% if is_incremental() %}
    where order.order_date >= (select max(order_date) from {{ this }})
{% endif %}*/