import requests
from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.providers.trino.operators.trino import TrinoOperator
import os.path
import urllib.request
import time

with DAG(
    "pcaf_ingestion-oecd", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="@daily", catchup=False
) as dag:

    @task(
        task_id="load_data_to_s3_bucket"
    )
    def load_data_to_s3_bucket():
        import pandas as pd
        import zipfile
        import urllib.request
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        url = "https://stats.oecd.org/sdmx-json/data/IO_GHG_2021/IMGR_FCO2+EXGR_DCO2...DTOTAL+D35/all?contentType=csv"
        query_parameters = {"downloadformat": "csv"}
        local_file = "oecdlocal2.csv"

        if not os.path.isfile(local_file):
            print('inside')
            print(local_file)
            with urllib.request.urlopen(url) as file:
                print('load')
                with open(local_file, "wb") as new_file:
                    new_file.write(file.read())
                new_file.close()

        s3_hook = S3Hook(aws_conn_id='s3')

        with open(local_file,  "r") as file_descriptor:
            df = pd.read_csv(file_descriptor)
            cols=pd.Series(df.columns.str.lower())
            print(cols[cols.duplicated()].unique())
            for dup in cols[cols.duplicated()].unique(): 
                cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
            df.columns=cols
            parquet_bytes = df.to_parquet(compression='gzip')
            s3_hook.load_bytes(parquet_bytes, bucket_name= "pcaf", key="/OECD/oecd.parquet", replace=True)

    load_data_to_s3_bucket()