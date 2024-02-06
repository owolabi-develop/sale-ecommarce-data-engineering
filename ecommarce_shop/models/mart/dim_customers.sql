{{ config(materialized='view') }}



with customers as (
    select
	CUSTOMER_ID,
	CUSTOMER_UNIQUE_ID,
	CUSTOMER_ZIP_CODE_PREFIX,
	CUSTOMER_CITY,
	CUSTOMER_STATE
    from ECOMMARCE.RAW_PRODUCTS.CUSTOMERS
)
select * from customers


