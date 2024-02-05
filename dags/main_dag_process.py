from extract_csv import (
    extract_olist_order_reviews_dataset,
    extract_customer_dataset,
    extract_geolocation_dataset,
    extract_olist_orders_dataset,
    extract_olist_products_dataset,
    extract_olist_sellers_dataset,
    extract_order_items_dataset,
    extract_order_payments_dataset,
    extract_product_category_name_translation,
    load_raw_data_to_s3
    )

import pendulum
import datetime
from airflow.operators.python import PythonOperator
import shutil
from airflow.decorators import dag, task



@dag(
    dag_id="process_sales_dataset",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def process_sales_dataset():
    rawdata_path ="/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dags/rawdata"
    
    dataset_path = [
        '/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dataset/olist_customers_dataset.csv',
        '/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dataset/olist_geolocation_dataset.csv',
        '/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dataset/olist_order_items_dataset.csv',
        '/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dataset/olist_order_payments_dataset.csv',
        '/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dataset/olist_order_reviews_dataset.csv',
        '/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dataset/olist_orders_dataset.csv',
        '/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dataset/olist_products_dataset.csv',
        '/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dataset/olist_sellers_dataset.csv',
        '/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dataset/product_category_name_translation.csv'
    ]
    
    
    
    customer_dataset = PythonOperator(task_id="customer_dataset",
                               python_callable=extract_customer_dataset,
                               op_kwargs={'filepath':dataset_path[0]}
                               )
    geolocation_dataset = PythonOperator(task_id="geolocation_dataset",
                               python_callable=extract_geolocation_dataset,
                               op_kwargs={'filepath':dataset_path[1]}
                               )
    
    order_items_dataset = PythonOperator(task_id="order_items_dataset",
                               python_callable=extract_order_items_dataset,
                               op_kwargs={'filepath':dataset_path[2]}
                               )
    
    order_payments_dataset = PythonOperator(task_id="order_payments_dataset",
                               python_callable=extract_order_payments_dataset,
                               op_kwargs={'filepath':dataset_path[3]}
                               )
    
    order_reviews_dataset = PythonOperator(task_id="order_reviews_dataset",
                               python_callable=extract_olist_order_reviews_dataset,
                               op_kwargs={'filepath':dataset_path[4]}
                               )
    
    orders_dataset = PythonOperator(task_id="orders_dataset",
                               python_callable=extract_olist_orders_dataset,
                               op_kwargs={'filepath':dataset_path[5]}
                               )
    
    products_dataset = PythonOperator(task_id="products_dataset",
                               python_callable=extract_olist_products_dataset,
                               op_kwargs={'filepath':dataset_path[6]}
                               )
    
    sellers_dataset = PythonOperator(task_id="sellers_dataset",
                               python_callable=extract_olist_sellers_dataset,
                               op_kwargs={'filepath':dataset_path[7]}
                               )
    
    category_name_translation = PythonOperator(task_id="category_name_translation",
                               python_callable=extract_product_category_name_translation,
                               op_kwargs={'filepath':dataset_path[8]}
                               )
    
    
    
    load_to_s3 = PythonOperator(task_id="load_to_s3",
                                python_callable=load_raw_data_to_s3,
                                op_kwargs={'extractpath': rawdata_path,
                                           'bucket_name':'raw-ecommarce-data',
                                           'folder_name':"raw",
                                           'aws_access_key_id':'AKIARDTV5PYPY5DFBB6P',
                                           'aws_secret_access_key':'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'
                                           })
    
    [customer_dataset >> geolocation_dataset >> order_items_dataset >>
     order_payments_dataset >> order_reviews_dataset >> orders_dataset >> products_dataset
     >> sellers_dataset >> category_name_translation] >> load_to_s3


dag = process_sales_dataset()