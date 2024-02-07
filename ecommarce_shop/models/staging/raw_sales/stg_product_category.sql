with product_category_name as (
    select * from {{ source('ecommarce_shopes','product_category_name') }}
)
select * from  product_category_name