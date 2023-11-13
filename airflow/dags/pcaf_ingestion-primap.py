import requests
from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.providers.trino.operators.trino import TrinoOperator
import os.path
import urllib.request

with DAG(
    "pcaf_ingestion-primap", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
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
        url = "https://zenodo.org/records/7727475/files/Guetschow-et-al-2023a-PRIMAP-hist_v2.4.2_final_no_rounding_09-Mar-2023.csv"
        local_file = "localfile2.csv"
        if not os.path.isfile(local_file):
            with urllib.request.urlopen(url) as file:
                with open(local_file, "wb") as new_file:
                    new_file.write(file.read())
                new_file.close()

        s3_hook = S3Hook(aws_conn_id='s3')

        with open(local_file,  "r") as file_descriptor:
            df = pd.read_csv(file_descriptor)
            parquet_bytes = df.to_parquet(compression='gzip')
            s3_hook.load_bytes(parquet_bytes, bucket_name= "pcaf", key="/PRIMAP/primap.parquet", replace=True)

   

    load_data_to_s3_bucket()