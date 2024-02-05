import pandas as pd

import boto3

import datetime

import os
import glob



def upload_dataset_to_s3_bucket(bucket_name, folder_name, file_path,aws_access_key_id,aws_secret_access_key):
    s3_client = boto3.client('s3',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        )
    file_key = f"{folder_name +"/"+ file_path.split('/')[-1]}" # Extract the file name

    s3_client.upload_file(file_path, bucket_name, file_key)
    
    
    
    




def extract_customer_dataset(filepath):
    customer_df = pd.read_csv(filepath)
    df = customer_df.drop_duplicates().dropna()
    df.to_csv("/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dags/rawdata/olist_customers_dataset.csv",index=False)
     



def extract_geolocation_dataset(filepath):
    geolocation_df = pd.read_csv(filepath)
    df =  geolocation_df.dropna()
    df.to_csv("/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dags/rawdata/olist_geolocation_dataset.csv",index=False)
    
    

def extract_order_items_dataset(filepath):
    order_items_df = pd.read_csv(filepath)
    df =  order_items_df.dropna()
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'])
    df.to_csv("/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dags/rawdata/olist_order_items_dataset.csv",index=False)
    
    
    
    
def extract_order_payments_dataset(filepath):
    order_items_df = pd.read_csv(filepath,sep=",",skip_blank_lines=True)
    df =  order_items_df.drop_duplicates().fillna(0)
    df.to_csv("/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dags/rawdata/olist_order_payments_dataset.csv",index=False)
    


def extract_olist_order_reviews_dataset(filepath):
    order_items_df = pd.read_csv(filepath,sep=",",skip_blank_lines=True)
    df =  order_items_df.drop_duplicates().dropna()
    df.to_csv("/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dags/rawdata/olist_order_reviews_dataset.csv",index=False) 


def extract_olist_orders_dataset(filepath):
    order_items_df = pd.read_csv(filepath,sep=",",skip_blank_lines=True)
    order_items_df['order_delivered_carrier_date'] = pd.to_datetime(order_items_df['order_delivered_carrier_date'])
    order_items_df['order_purchase_timestamp'] = pd.to_datetime(order_items_df['order_purchase_timestamp'])
    order_items_df['order_approved_at'] = pd.to_datetime(order_items_df['order_approved_at'])
    order_items_df['order_delivered_customer_date'] = pd.to_datetime(order_items_df['order_delivered_customer_date'])
    order_items_df['order_estimated_delivery_date'] = pd.to_datetime(order_items_df['order_estimated_delivery_date'])
    
    
    df =  order_items_df.drop_duplicates().fillna(0)
    df.to_csv("/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dags/rawdata/olist_orders_dataset.csv",index=False)



def extract_olist_products_dataset(filepath):
    order_items_df = pd.read_csv(filepath,sep=",",skip_blank_lines=True)
    df =  order_items_df.drop_duplicates().dropna()
    df.to_csv("/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dags/rawdata/olist_products_dataset.csv",index=False)
    


def extract_olist_sellers_dataset(filepath):
    order_items_df = pd.read_csv(filepath,sep=",",skip_blank_lines=True)
    df =  order_items_df.drop_duplicates().dropna()
    df.to_csv("/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dags/rawdata/olist_sellers_dataset.csv",index=False)
    

def extract_olist_sellers_dataset(filepath):
    order_items_df = pd.read_csv(filepath,sep=",",skip_blank_lines=True)
    df =  order_items_df.drop_duplicates().dropna()
    df.to_csv("/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dags/rawdata/olist_sellers_dataset.csv",index=False)
    
    

def extract_product_category_name_translation(filepath):
    order_items_df = pd.read_csv(filepath,sep=",",skip_blank_lines=True)
    df =  order_items_df.drop_duplicates().dropna()
    df.to_csv("/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dags/rawdata/product_category_name_translation.csv",index=False)
    

def load_raw_data_to_s3(extractpath,bucket_name,folder_name,aws_access_key,aws_secret_access_key):
    csv_files = glob.glob(f'{extractpath}/*.csv')
    [upload_dataset_to_s3_bucket(bucket_name,folder_name,dataset,aws_access_key,aws_secret_access_key) for dataset in csv_files]

filepath ="/mnt/c/Users/PROGRESSIVE/Desktop/dataengineers_projects/e-commarce-dataengineering-dbt-snowflake/dataset/"


load_raw_data_to_s3(filepath,'raw-ecommarce-data','raw','AKIARDTV5PYPY5DFBB6P','LlA3HNoKT9o0d+myo7b+dPBdD7lgrmY7/sRL5tUg')
# aws_access_key='AKIARDTV5PYPY5DFBB6P'
# aws_secret_access_key='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'

# upload_scrap_data_to_space('raw-ecommarce-data','raw',filepath,'AKIARDTV5PYPY5DFBB6P','LlA3HNoKT9o0d+myo7b+dPBdD7lgrmY7/sRL5tUg')