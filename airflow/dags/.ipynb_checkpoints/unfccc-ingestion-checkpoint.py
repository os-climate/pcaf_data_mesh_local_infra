from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.providers.trino.operators.trino import TrinoOperator

with DAG(
    "unfccc_ingestion", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="@daily", catchup=False
) as dag:

    @task.virtualenv(
        task_id="load_data_to_s3_bucket", requirements=["unfccc_di_api==4.0.0", "pooch==1.7.0", ], system_site_packages=True
    )
    def load_zenodo_data_to_bucket():
        import unfccc_di_api
        import pandas as pd
        import zipfile
        import pooch
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        reader = unfccc_di_api.ZenodoReader()
        frames = []
        url = "doi:10.5281/zenodo.8159736/parquet-only.zip"
        known_hash = "md5:95d98404f642ed2b684594abba8934ba"
        zipfile_path = pooch.retrieve(url=url, known_hash=known_hash)
        zipfile = zipfile.ZipFile(zipfile_path)
        s3_hook = S3Hook(aws_conn_id='s3')
        for parquet_file_name in zipfile.namelist():
            print(parquet_file_name)
            with zipfile.open(parquet_file_name, "r") as file_descriptor:
                df = pd.read_parquet(file_descriptor)
                parquet_bytes = df.to_parquet(compression='gzip')
                s3_hook.load_bytes(parquet_bytes, bucket_name= "unfccc-raw", key=parquet_file_name, replace=True)

    trino_create_schema = TrinoOperator(
        task_id="trino_create_schema",
        trino_conn_id="trino_connection",
        sql=f"CREATE SCHEMA IF NOT EXISTS hive.unfccc6 WITH (location = 's3a://unfccc-raw/')",
        handler=list,
    )

    trino_create_annex_I_table = TrinoOperator(
        task_id="trino_create_annexI_table",
        trino_conn_id="trino_connection",
        sql=f"""create table if not exists hive.unfccc6.annexI (
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
                     external_location = 's3a://unfccc-raw/data/annexI',
                     format = 'PARQUET'
                    )""",
        handler=list,
        outlets=['hive.unfccc6.annexI']
    )

    trino_create_nonannex_I_table = TrinoOperator(
        task_id="trino_create_nonAnnexI_table",
        trino_conn_id="trino_connection",
        sql=f"""create table if not exists hive.unfccc6.nonAnnexI (
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
                     external_location = 's3a://unfccc-raw/data/non-annexI',
                     format = 'PARQUET'
                    )""",
        handler=list,
        outlets=['hive.unfccc6.nonAnnexI']
    )

    load_zenodo_data_to_bucket() >> trino_create_schema >> [trino_create_annex_I_table, trino_create_nonannex_I_table]
