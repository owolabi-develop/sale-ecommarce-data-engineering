use role ACCOUNTADMIN;
--CREATE WAREHOUSE
CREATE OR REPLACE WAREHOUSE DATAENGINE WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;

--USE WAREHOUSE
USE WAREHOUSE DATAENGINE;

 -- CREATE DATABASE
CREATE OR REPLACE DATABASE ECOMMARCE;

--DROP PUBLIC SCHEMA
DROP SCHEMA ECOMMARCE.PUBLIC;
-- CREATE RAW_NEWSARTICLES
CREATE OR REPLACE SCHEMA  ECOMMARCE.RAW_PRODUCTS;

create or replace schema ECOMMARCE.EHHANCE_PRODUCTS;


--- create storage intergration

CREATE or replace STORAGE INTEGRATION ecommarce_data_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::076482969119:role/snowflake_ecommarce_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://raw-ecommarce-data/raw/');
 
 --- Retrieve the AWS IAM User for your Snowflake Account

 DESC INTEGRATION ecommarce_data_int;

--- just granting privilege for the schema to my self noting special
GRANT CREATE STAGE ON SCHEMA ECOMMARCE.RAW_PRODUCTS TO ROLE ACCOUNTADMIN;

GRANT USAGE ON INTEGRATION ecommarce_data_int TO ROLE ACCOUNTADMIN;

--- Create an External Stage 
use schema  ECOMMARCE.RAW_PRODUCTS;

--CREATE FILE FORMAT 
CREATE OR REPLACE FILE FORMAT products_csv__with_double_qout_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = true;

CREATE OR REPLACE FILE FORMAT products_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;

CREATE or replace STAGE raw_ecommarce_stage
  STORAGE_INTEGRATION = ecommarce_data_int
  URL = 's3://raw-ecommarce-data/raw/';
 

-- testing stage to retrive data
list @raw_ecommarce_stage;


  


--create SEQUENCE
CREATE OR REPLACE SEQUENCE raw_p;

CREATE OR REPLACE SEQUENCE enhance_p;

-- create table tables to load raw products data

 --- create product
CREATE OR REPLACE TABLE ECOMMARCE.RAW_PRODUCTS.PRODUCT (
    product_id VARCHAR NOT NULL,
    product_category_name VARCHAR,
    product_name_length INT,
    primary key (product_id)
);


---- create product details
CREATE OR REPLACE TABLE ECOMMARCE.RAW_PRODUCTS.PRODUCT_DETAILS (
    product_id VARCHAR NOT NULL,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT,

    CONSTRAINT FK_PRODUCT FOREIGN KEY (product_id) REFERENCES product(product_id)
);



 --- create product cagegory
CREATE OR REPLACE TABLE ECOMMARCE.RAW_PRODUCTS.PRODUCT_CATEGORY_NAME(
    --ID NUMBER DEFAULT RAW_P.NEXTVAL,
    product_category_name VARCHAR,
    product_category_name_english VARCHAR
);


--- create orders

CREATE OR REPLACE TABLE ECOMMARCE.RAW_PRODUCTS.ORDERS (
    order_id VARCHAR NOT NULL,
    customer_id VARCHAR,
    order_status VARCHAR,
    order_purchase_timestamp TIMESTAMP_NTZ,
    order_approved_at TIMESTAMP_NTZ,
    order_delivered_carrier_date TIMESTAMP_NTZ,
    order_delivered_customer_date TIMESTAMP_NTZ,
    order_estimated_delivery_date TIMESTAMP_NTZ,
    primary key (order_id),
    -- CUSTOMER FOREIGN KEY
    CONSTRAINT FK_CUSTOMER FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id)
);

--- create orderitems
CREATE OR REPLACE TABLE ECOMMARCE.RAW_PRODUCTS.ORDER_ITEMS (
    order_id VARCHAR,
    order_item_id INT,
    product_id VARCHAR,
    seller_id VARCHAR,
    shipping_limit_date TIMESTAMP_NTZ,
    price FLOAT,
    freight_value FLOAT,

    CONSTRAINT FK_PRODUCT_ID  FOREIGN KEY (PRODUCT_id) REFERENCES PRODUCT(PRODUCT_id),
    CONSTRAINT FK_SELLER_ID  FOREIGN KEY (seller_id) REFERENCES SELLER(seller_id),
    CONSTRAINT FK_ORDER_ID  FOREIGN KEY (order_id) REFERENCES ORDERS(order_id)
);


---- create order review
CREATE OR REPLACE TABLE ECOMMARCE.RAW_PRODUCTS.ORDER_REVIEWS (
    review_id VARCHAR,
    order_id VARCHAR,
    review_score INT,
    review_comment_title VARCHAR,
    review_comment_message VARCHAR,
    review_creation_date TIMESTAMP_NTZ,
    review_answer_timestamp TIMESTAMP_NTZ,

    CONSTRAINT FK_ORDER_ID FOREIGN KEY (order_id) REFERENCES ORDERS(order_id)
);

 ---- create  GEOLOCATION

CREATE OR REPLACE TABLE ECOMMARCE.RAW_PRODUCTS.GEOLOCATION (
    geolocation_zip_code_prefix INT,
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city VARCHAR,
    geolocation_state VARCHAR
);

 --- create seller
CREATE OR REPLACE TABLE ECOMMARCE.RAW_PRODUCTS.SELLER(
     seller_id VARCHAR NOT NULL,
     seller_zip_code_prefix  VARCHAR,
     seller_city VARCHAR,
     seller_state VARCHAR,
     primary key (seller_id)

);

-- create seller

CREATE OR REPLACE TABLE ECOMMARCE.RAW_PRODUCTS.CUSTOMERS (
    customer_id VARCHAR NOT NULL,
    customer_unique_id VARCHAR,
    customer_zip_code_prefix INT,
    customer_city VARCHAR,
    customer_state VARCHAR,
     primary key (customer_id)
);



CREATE OR REPLACE TABLE ECOMMARCE.RAW_PRODUCTS.PAYMENTS (
    order_id VARCHAR,
    payment_sequential INT,
    payment_type VARCHAR,
    payment_installments INT,
    payment_value FLOAT,

    CONSTRAINT FK_ORDER_ID  FOREIGN KEY (order_id) REFERENCES ORDERS(order_id)
);








