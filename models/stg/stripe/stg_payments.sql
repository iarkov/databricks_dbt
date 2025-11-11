select
    id as line_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status as payment_status,
    amount,
    created
from {{ source('stripe', 'payments') }}