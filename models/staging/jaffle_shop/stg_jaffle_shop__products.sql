with 

source as (

    select * from {{ source('jaffle_shop', 'products') }}

),

renamed as (

    select
        sku,
        name product_name,
        type product_type,
        price,
        description

    from source

)

select * from renamed