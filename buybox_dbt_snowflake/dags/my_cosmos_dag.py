from datetime import datetime
import os
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from airflow.decorators import dag
from pathlib import Path

os.environ['AIRFLOW_HOME'] = '/usr/local/airflow'
dbt_project_path = Path("/usr/local/airflow/dags/dbt/cosmosproject_buybox/")
dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="cosmosproject_buybox",
    target_name="dev",
    profiles_yml_filepath=dbt_project_path / "profiles.yml",
)

@dag(
    start_date=datetime(2023, 9, 10),
    schedule="@daily",  # Runs daily
    catchup=False,
)
def first_seeds_dag():
    # Task group for running dbt seeds
    seed_dag = DbtTaskGroup(
        group_id="seed_dag",
        project_config=ProjectConfig(dbt_project_path),
        execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["path:seeds"],  
        )
    )

    model_dag = DbtTaskGroup(
        group_id="model_dag",
        project_config=ProjectConfig(dbt_project_path),
        execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=[
                "path:models/cdc/base_seed_model.sql"
            ],
        )
    )
    final_model_dag = DbtTaskGroup(
        group_id="final_model_dag",
        project_config=ProjectConfig(dbt_project_path),
        execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=[
                "path:models/cdc/final_data_table.sql",
            ],
        )
    )

    seed_dag >>model_dag>> final_model_dag

first_seeds_dag_instance = first_seeds_dag()