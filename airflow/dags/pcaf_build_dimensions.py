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
    "pcaf_build_general_dimensions", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="@daily", catchup=False
) as dag:

    @task(
        task_id="load_data_to_s3_bucket"
    )
    def load_country_dimension():
        import pycountry
        import pandas as pd
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        pycountry.countries.add_entry(alpha_3="ROW", alpha_2 = "", name="Rest of the World", numeric = "" )       
        df_country = pd.DataFrame([[country.alpha_3, country.alpha_2, country.name]  for country in pycountry.countries ], columns=['country_iso3_code', 'country_iso2_code', 'country_name'], dtype='string' )
        #row = {'country_iso3_code':'ROW', 'country_iso2_code':'', 'country_name':'Rest of the World'}
        #df_country = pd.concat([df_country, pd.DataFrame([row])], ignore_index=True)
        df_country.to_parquet(compression='gzip')

        s3_hook = S3Hook(aws_conn_id='s3')
        parquet_bytes = df_country.to_parquet(compression='gzip')
        s3_hook.load_bytes(parquet_bytes, bucket_name= "pcaf", key=f"raw/pycountry/pycountry.parquet", replace=True)
    
    trino_create_schema = TrinoOperator(
        task_id="trino_create_schema",
        trino_conn_id="trino_connection",
        sql=f"CREATE SCHEMA IF NOT EXISTS hive.pcaf WITH (location = 's3a://pcaf/data')",
        handler=list,
    )

    trino_create_oecd_table = TrinoOperator(
        task_id="trino_create_pycountry_table",
        trino_conn_id="trino_connection",
        sql=f"""create table if not exists hive.pcaf.pycountry (
                        "country_iso3_code" varchar, "country_iso2_code" varchar, "country_name" varchar
                        )
                        with (
                         external_location = 's3a://pcaf/raw/pycountry/',
                         format = 'PARQUET'
                        )""",
        handler=list,
        outlets=['hive.pcaf.pycountry']
    )

    load_country_dimension()  >> trino_create_schema >> trino_create_oecd_table