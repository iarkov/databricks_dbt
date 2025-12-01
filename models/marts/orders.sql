{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

with order_payment as (
    select 
        order_id,
        sum(amount) as total_amount
    from {{ ref('stg_payments') }}
    where payment_status <> 'fail'
    group by order_id
), order as (
    select 
        order_id,
        customer_id,
        order_date,
        order_status
    from {{ ref('stg_orders') }}
)
select 
    order.order_id,
    order.customer_id,
    order.order_date,
    order.order_status,
    order_payment.total_amount
from order
left join order_payment
on order.order_id = order_payment.order_id
/*{% if is_incremental() %}
    where order.order_date >= (select max(order_date) from {{ this }})
{% endif %}*/