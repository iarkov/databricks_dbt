with 

source as (

    select * from {{ source('jaffle_shop', 'items') }}

),

renamed as (

    select
        id item_id,
        order_id,
        sku

    from source

)

select * from renamed