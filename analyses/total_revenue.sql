with order as (
    select 
        *
    from {{ ref('stg_orders') }}
)
select * 
from order