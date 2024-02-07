with products as (
    select * from {{ source('ecommarce_shopes','product') }}
)
select * from products