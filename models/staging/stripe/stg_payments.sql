select
    id as line_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status as payment_status,
    {{ cents_to_dollars("amount") }} as amount,
    created as date_created_at
from {{ source('stripe', 'payments') }}