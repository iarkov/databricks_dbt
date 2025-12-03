with 

source as (

    select * from {{ source('jaffle_shop', 'orders') }}

),

renamed as (

    select
        id order_id,
        customer customer_id,
        ordered_at,
        store_id,
        subtotal,
        tax_paid,
        order_total

    from source

)

select * from renamed