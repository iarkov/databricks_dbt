with 

source as (

    select * from {{ source('jaffle_shop', 'supplies') }}

),

renamed as (

    select
        id supply_id,
        name supply_name,
        cost,
        perishable,
        sku

    from source

)

select * from renamed