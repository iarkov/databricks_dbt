with payments as (
    select * from {{ ref('stg_payments') }}
)

select 
    order_id, 
    sum(amount) as total_revenue
from payments
where payment_status <> 'fail'
group by 1