import requests
from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.providers.trino.operators.trino import TrinoOperator
from pyarrow import fs, csv, parquet
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
        local_file = "oecd_api_data.csv"
        if os.path.isfile(local_file):
             os.remove(local_file)

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
            s3_hook.load_bytes(parquet_bytes, bucket_name= "pcaf", key="raw/oecd/oecd.parquet", replace=True)
        
        if os.path.isfile(local_file):
             os.remove(local_file)

    
    trino_create_schema = TrinoOperator(
        task_id="trino_create_schema",
        trino_conn_id="trino_connection",
        sql=f"CREATE SCHEMA IF NOT EXISTS hive.pcaf WITH (location = 's3a://pcaf/')",
        handler=list,
    )

    trino_create_oecd_table = TrinoOperator(
        task_id="trino_create_oecd_table",
        trino_conn_id="trino_connection",
        sql=f"""create table if not exists hive.pcaf.oecd (
                        "var" varchar,"indicator" varchar,"cou" varchar,"country" varchar,"par" varchar,"partner" varchar,"ind" varchar,"industry" varchar,"time" bigint,"time_1" bigint,"unit code" varchar,"unit" varchar,"powercode code" bigint,"powercode" varchar,"reference period code" double,"reference period" double,"value" double,"flag codes" double,"flags" double
                        )
                        with (
                         external_location = 's3a://pcaf/raw/oecd/',
                         format = 'PARQUET'
                        )""",
        handler=list,
        outlets=['hive.pcaf.oecd']
    )

    load_data_to_s3_bucket()  >> trino_create_schema >> trino_create_oecd_table