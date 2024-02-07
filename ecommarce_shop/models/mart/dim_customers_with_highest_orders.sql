with customers as (
    select CUSTOMER_ID, customer_city, customer_state, CUSTOMER_ZIP_CODE_PREFIX from {{ ref('stg_customer')}}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customers_orders as (
    select
        order_id,
        CUSTOMER_ID,
        order_status,
        ORDER_DELIVERED_CUSTOMER_DATE,
        max(ORDER_PURCHASE_TIMESTAMP) as most_recent_order,
        count(order_id) as number_of_orders
    from orders
    group by  order_id, CUSTOMER_ID, order_status, ORDER_DELIVERED_CUSTOMER_DATE
),
payments as (
    select * from {{ ref('stg_payment') }}
),

customer_payments as (
    select 
        order_id,
        payment_type,
        payment_value as payment_amount
    from payments
),

final as (
    select
        customers.CUSTOMER_ID,
        customers.customer_city, 
        customers.customer_state, 
        customers.CUSTOMER_ZIP_CODE_PREFIX,
        customers_orders.order_id,
        customers_orders.order_status,
        customer_payments.payment_type,
        customer_payments.payment_amount,
        customers_orders.ORDER_DELIVERED_CUSTOMER_DATE,
        coalesce(customers_orders.number_of_orders, 0) as number_of_orders
    from customers
    join customers_orders on customers_orders.CUSTOMER_ID = customers.CUSTOMER_ID
    join customer_payments on customer_payments.order_id = customers_orders.order_id
)

select * from final
