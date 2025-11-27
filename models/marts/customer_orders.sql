-- import CTEs

with customer as (
    select 
        *
    from {{ ref("stg_customers") }}
)
, order as (
    select 
        *
        , min(order_date) over (partition by customer_id) min_order_date
    from {{ ref("stg_orders") }}
)
, order_payment as (
    select 
        order_id
        , max(date_created_at) last_payment_date
        , sum(amount) as total_amount_paid
    from {{ ref("stg_payments") }}
    where payment_status <> 'fail'
    group by 1
)

-- logical CTEs

-- final CTEs

, paid_orders as (
    select 
        order.order_id
        , order.customer_id
        , order.order_date
        , order.order_status
        , order_payment.total_amount_paid
        , order_payment.last_payment_date
        , customer.first_name as customer_first_name
        , customer.last_name as customer_last_name
        , row_number() over (partition by order.customer_id order by order.order_id) as customer_sales_seq
        , case 
            when order.order_date = order.min_order_date
            then 'new'
            else 'return' 
        end as new_vs_return
        , sum(order_payment.total_amount_paid) over (
            partition by order.customer_id order by order.order_date
        ) as customer_lifetime_value
        , order.min_order_date as first_order_date
    from order
    left join order_payment 
        on order.order_id = order_payment.order_id
    left join customer
        on order.customer_id = customer.customer_id
)

select * from paid_orders