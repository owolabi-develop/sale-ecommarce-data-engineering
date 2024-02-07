with product_details as (
    select * from {{ source('ecommarce_shopes','product_details') }}
)
select * from product_details