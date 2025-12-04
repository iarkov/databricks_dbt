{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}


with customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

)/*,

payments as (

    select * from {{ ref('stg_payments') }}

),

employees as (

    select * from {{ ref('employees') }}

)*/,

customer_orders as (

    select
        customer_id,

        min(orders.order_date) as first_order_date,
        max(orders.order_date) as most_recent_order_date,
        count(distinct orders.order_id) as number_of_orders,
        sum(orders.order_total) as lifetime_value
    from orders /*
    left join payments 
        on orders.order_id = payments.order_id and 
        payments.payment_status <> 'fail'
    where orders.order_status <> 'returned'*/

    group by 1

),

final as (

    select
        customers.customer_id,
        split_part(customers.name, ' ', 1) first_name,
        split_part(customers.name, ' ', 2) last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_orders.lifetime_value, 0) as lifetime_value

    from customers

    left join customer_orders using (customer_id)

    --left join employees using (customer_id)

)

select * from final
