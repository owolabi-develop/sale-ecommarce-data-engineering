
use role ACCOUNTADMIN;

USE WAREHOUSE DATAENGINE;


list @raw_ecommarce_stage;

--- LOAD DATAS INTO TABLES
create OR REPLACE pipe ECOMMARCE.RAW_PRODUCTS.PRODUCT_PIPE auto_ingest=true as
COPY INTO ECOMMARCE.RAW_PRODUCTS.PRODUCT(
    PRODUCT_ID,
    product_category_name,
    product_name_length
)
FROM 
(SELECT $1::VARCHAR AS PRODUCT_ID, $2::VARCHAR AS PRODUCT_CATEGORY_NAME, $3::INT AS product_name_length
from @raw_ecommarce_stage/olist_products_dataset.csv
(FILE_FORMAT =>products_csv__with_double_qout_format));


create OR REPLACE pipe ECOMMARCE.RAW_PRODUCTS.PRODUCT_DETAILS_PIPE auto_ingest=true as
COPY INTO  ECOMMARCE.RAW_PRODUCTS.PRODUCT_DETAILS (
    product_id,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
FROM
(SELECT $1::VARCHAR AS PRODUCT_ID, $4::INT AS PRODUCT_DESCRIPTION_LENGTH, $5::INT AS PRODUCT_PHOTOS_QTY,
$6::FLOAT AS PRODUCT_WEIGHT_G, $7::FLOAT AS PRODUCT_LENGTH_CM, $8::FLOAT AS PRODUCT_HEIGHT_CM,
$9::FLOAT AS PRODUCT_WIDTH_CM
from @raw_ecommarce_stage/olist_products_dataset.csv
(FILE_FORMAT =>products_csv__with_double_qout_format));

create OR REPLACE pipe ECOMMARCE.RAW_PRODUCTS.PRODUCT_CATEGORY_PIPE auto_ingest=true as
copy into ECOMMARCE.RAW_PRODUCTS.PRODUCT_CATEGORY_NAME
 from @raw_ecommarce_stage/product_category_name_translation.csv
 FILE_FORMAT = (FORMAT_NAME = products_csv__with_double_qout_format);

create OR REPLACE pipe ECOMMARCE.RAW_PRODUCTS.CUSTOMER_PIPE auto_ingest=true as
 copy into ECOMMARCE.RAW_PRODUCTS.CUSTOMERS
 from @raw_ecommarce_stage/olist_customers_dataset.csv
 FILE_FORMAT = (FORMAT_NAME = products_csv__with_double_qout_format);




create OR REPLACE pipe ECOMMARCE.RAW_PRODUCTS.SELLERS_PIPE auto_ingest=true as
 copy into ECOMMARCE.RAW_PRODUCTS.SELLER
 from @raw_ecommarce_stage/olist_sellers_dataset.csv
 FILE_FORMAT = (FORMAT_NAME = products_csv__with_double_qout_format);


create OR REPLACE pipe ECOMMARCE.RAW_PRODUCTS.PAYMENT_PIPE auto_ingest=true as
copy into ECOMMARCE.RAW_PRODUCTS.PAYMENTS
 from @raw_ecommarce_stage/olist_order_payments_dataset.csv
 FILE_FORMAT = (FORMAT_NAME = products_csv__with_double_qout_format);

create OR REPLACE pipe ECOMMARCE.RAW_PRODUCTS.GEO_PIPE auto_ingest=true as
copy into ECOMMARCE.RAW_PRODUCTS.GEOLOCATION
 from @raw_ecommarce_stage/olist_geolocation_dataset.csv
 FILE_FORMAT = (FORMAT_NAME = products_csv__with_double_qout_format);




create OR REPLACE pipe ECOMMARCE.RAW_PRODUCTS.ORDER_PIP auto_ingest=true as
copy into ECOMMARCE.RAW_PRODUCTS.ORDERS
 from @raw_ecommarce_stage/olist_orders_dataset.csv
 FILE_FORMAT = (FORMAT_NAME = products_csv__with_double_qout_format);


create OR REPLACE pipe ECOMMARCE.RAW_PRODUCTS.ORDER_REVIEW_PIPE auto_ingest=true as
copy into ECOMMARCE.RAW_PRODUCTS.ORDER_REVIEWS
 from @raw_ecommarce_stage/olist_order_reviews_dataset.csv
 FILE_FORMAT = (FORMAT_NAME = products_csv__with_double_qout_format);



 create OR REPLACE pipe ECOMMARCE.RAW_PRODUCTS.ORDER_ITEMS_PIPE auto_ingest=true as
copy into ECOMMARCE.RAW_PRODUCTS.ORDER_ITEMS
 from @raw_ecommarce_stage/olist_order_items_dataset.csv
 FILE_FORMAT = (FORMAT_NAME = products_csv__with_double_qout_format);

------ END OF COPY INTO DATALE AUTO INGEST

SHOW PIPES;