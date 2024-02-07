
with customers as (
    select
	CUSTOMER_ID,
	CUSTOMER_UNIQUE_ID,
	CUSTOMER_ZIP_CODE_PREFIX,
	CUSTOMER_CITY,
	CUSTOMER_STATE
    from {{ source ('ecommarce_shopes','customers')}}
)
select * from customers


