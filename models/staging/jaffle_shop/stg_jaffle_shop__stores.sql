with 

source as (

    select * from {{ source('jaffle_shop', 'stores') }}

),

renamed as (

    select
        id store_id,
        name store_name,
        opened_at,
        tax_rate

    from source

)

select * from renamed