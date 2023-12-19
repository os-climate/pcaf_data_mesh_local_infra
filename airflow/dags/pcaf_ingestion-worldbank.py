import requests
from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.providers.trino.operators.trino import TrinoOperator
import os.path
import urllib.request

with DAG(
    "pcaf_ingestion-worldbank", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
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
        url = "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD;NY.GDP.MKTP.PP.CD?source=2&downloadformat=csv"
        local_file = "worldbank.zip"
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
            print(parquet_file_name)
            if "Metadata" not in parquet_file_name:
                with zipfile.open(parquet_file_name, "r") as file_descriptor:
                    df = pd.read_csv(file_descriptor, skiprows=4, quotechar= '"')
                    parquet_bytes = df.to_parquet(compression='gzip')
                    s3_hook.load_bytes(parquet_bytes, bucket_name= "pcaf", key=f"raw/worldbank/worldbank.parquet", replace=True)

    trino_create_schema = TrinoOperator(
        task_id="trino_create_schema",
        trino_conn_id="trino_connection",
        sql=f"CREATE SCHEMA IF NOT EXISTS hive.pcaf WITH (location = 's3a://pcaf/data')",
        handler=list,
    )

    trino_create_worldbank_table = TrinoOperator(
        task_id="trino_create_worldbank_table",
        trino_conn_id="trino_connection",
        sql=f"""create table if not exists hive.pcaf.worldbank (
                        "Country Name" varchar,"Country Code" varchar,"Indicator Name" varchar,"Indicator Code" varchar,"1960" double,"1961" double,"1962" double,"1963" double,"1964" double,"1965" double,"1966" double,"1967" double,"1968" double,"1969" double,"1970" double,"1971" double,"1972" double,"1973" double,"1974" double,"1975" double,"1976" double,"1977" double,"1978" double,"1979" double,"1980" double,"1981" double,"1982" double,"1983" double,"1984" double,"1985" double,"1986" double,"1987" double,"1988" double,"1989" double,"1990" double,"1991" double,"1992" double,"1993" double,"1994" double,"1995" double,"1996" double,"1997" double,"1998" double,"1999" double,"2000" double,"2001" double,"2002" double,"2003" double,"2004" double,"2005" double,"2006" double,"2007" double,"2008" double,"2009" double,"2010" double,"2011" double,"2012" double,"2013" double,"2014" double,"2015" double,"2016" double,"2017" double,"2018" double,"2019" double,"2020" double,"2021" double,"2022" double
                        )
                        with (
                         external_location = 's3a://pcaf/raw/worldbank/',
                         format = 'PARQUET'
                        )""",
        handler=list,
        outlets=['hive.pcaf.worldbank']
    )


    load_data_to_s3_bucket()  >> trino_create_schema >> trino_create_worldbank_table