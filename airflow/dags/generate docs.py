from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.operators import DbtDocsS3Operator


profile_config = ProfileConfig(
    profile_name="pcaf",
    target_name="dev",
    profiles_yml_filepath="/opt/airflow/dags/dbt/profiles.yml",
)

project_config = ProjectConfig(
    dbt_project_path="/opt/airflow/dags/dbt/pcaf",
)

execution_config = ExecutionConfig(
        dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt",
)


with DAG(
    dag_id="generate_docs",
    start_date=datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="@daily", catchup=False
):
   
    generate_dbt_docs_aws = DbtDocsS3Operator(
        task_id="generate_dbt_docs_aws",
        project_dir="/opt/airflow/dags/dbt/pcaf",
        profile_config=profile_config,
        # docs-specific arguments
        bucket_name="pcaf",
        folder_dir="dbt_trino",
        aws_conn_id="s3",
        install_deps=True
    )

  

generate_dbt_docs_aws