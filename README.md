## Data Engineering Project With Snowflake and dbt
 End to end data engineering project that extract ecommarce data from download kaggle dataset
 load the raw data to s3 bucket and configure aws sqs to trigger snowflake to use snowpip to auto ingest data
 to raw data to approprate table anytime there's new data in the s3 bucket then use dbt to tranform and build 
 model with the data

 ## list of  data model in this project
 1. dim_best_review_product
 2. dim_best_selling_product
 3. dim_customers_with_highest_orders
 4. dim_highest_seller
 5. fct_total_oders 




## Architecture Diagram
<img width="2961" alt="project4" src="https://github.com/owolabi-develop/sale-ecommarce-data-engineering/assets/94055941/3f7af8e6-1723-431f-95a6-c43922e88e17">

## Visualization
![ecommarce_dashboard](https://github.com/owolabi-develop/sale-ecommarce-data-engineering/assets/94055941/30a46ef6-1170-4498-9581-f8d2534c6514)



## Tech stack
1. python
2. airflow
3. dbt
4. snowflake
5. power bi

