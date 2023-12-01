import requests
from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.providers.trino.operators.trino import TrinoOperator
import os.path
import urllib.request

with DAG(
    "pcaf_ingestion-unfccc", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
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
        url = "https://zenodo.org/records/8159736/files/parquet-only.zip"
        local_file = "parquet-only.zip"
        if os.path.isfile(local_file):
             os.remove(local_file)   

        if not os.path.isfile(local_file):
            with urllib.request.urlopen(url) as file:
                with open(local_file, "wb") as new_file:
                    new_file.write(file.read())
                new_file.close()

        zipfile = zipfile.ZipFile(open(local_file, "rb"))


        s3_hook = S3Hook(aws_conn_id='s3')
        for parquet_file_name in zipfile.namelist():
            with zipfile.open(parquet_file_name, "r") as file_descriptor:
                df = pd.read_parquet(file_descriptor)
                parquet_bytes = df.to_parquet(compression='gzip')
                s3_hook.load_bytes(parquet_bytes, bucket_name= "pcaf", key='raw/unfccc/' + parquet_file_name.lower(), replace=True)


    trino_create_schema = TrinoOperator(
        task_id="trino_create_schema",
        trino_conn_id="trino_connection",
        sql=f"CREATE SCHEMA IF NOT EXISTS hive.pcaf WITH (location = 's3a://pcaf/')",
        handler=list,
    )

    trino_create_annexi_table = TrinoOperator(
        task_id="trino_create_annexi_table",
        trino_conn_id="trino_connection",
        sql=f"""create table if not exists hive.pcaf.annexi (
                    party varchar,
                    category varchar,
                    classification varchar,
                    measure varchar,
                    gas varchar,
                    unit varchar,
                    year varchar,
                    numberValue double,
                    stringValue varchar
                    )
                    with (
                     external_location = 's3a://pcaf/raw/unfccc/data/annexi',
                     format = 'PARQUET'
                    )""",
        handler=list,
        outlets=['hive.pcaf.annexi']
    )

    trino_create_non_annexi_table = TrinoOperator(
        task_id="trino_create_non_annexi_table",
        trino_conn_id="trino_connection",
        sql=f"""create table if not exists hive.pcaf.non_annexi (
                    party varchar,
                    category varchar,
                    classification varchar,
                    measure varchar,
                    gas varchar,
                    unit varchar,
                    year varchar,
                    numberValue double,
                    stringValue varchar
                    )
                    with (
                     external_location = 's3a://pcaf/raw/unfccc/data/non-annexi',
                     format = 'PARQUET'
                    )""",
        handler=list,
        outlets=['hive.pcaf.non_annexi']
    )

    load_data_to_s3_bucket()  >> trino_create_schema >> [trino_create_annexi_table, trino_create_non_annexi_table]
