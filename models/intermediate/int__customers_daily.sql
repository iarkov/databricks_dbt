select 
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'order_date']) }} as primary_key,
    customer_id,
    order_date,
    count(*) nb_customers
from {{ ref('stg_orders') }}
group by 2, 3
