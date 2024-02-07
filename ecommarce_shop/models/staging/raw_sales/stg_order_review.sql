with order_reviews as (
    select * from {{ source('ecommarce_shopes','order_reviews') }}
)
select * from order_reviews