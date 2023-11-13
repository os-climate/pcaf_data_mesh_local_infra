from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig


profile_config = ProfileConfig(
    profile_name="pcaf",
    target_name="dev",
    profiles_yml_filepath="/opt/airflow/dags/dbt/profiles.yml",
)

project_config = ProjectConfig(
    dbt_project_path="/opt/airflow/dags/dbt/pcaf",
)

execution_config = ExecutionConfig(
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
)

with DAG(
    dag_id="extract_dag",
    start_date=datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="@daily", catchup=False
):
    e1 = EmptyOperator(task_id="pre_dbt")

    dbt_tg = DbtTaskGroup(
        project_config=project_config,
        profile_config=profile_config,
        execution_config = execution_config
    )

    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> dbt_tg >> e2