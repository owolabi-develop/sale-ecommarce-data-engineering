with payments as (
    select * from {{ source('ecommarce_shopes','payments') }}
)
select * from payments