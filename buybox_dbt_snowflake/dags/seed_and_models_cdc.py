

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
    schedule="@daily",
    catchup=False,
)

#For Snowflake

# def cosmosproject_buybox_dag():
#     seed_data = DbtTaskGroup(
#         group_id="seed_data",
#         project_config=ProjectConfig(dbt_project_path),
#         execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#         profile_config=profile_config,
#         render_config=RenderConfig(
#             select=["path:seeds"],  
#         )
#     )

#     run_models = DbtTaskGroup(
#         group_id="run_models",
#         project_config=ProjectConfig(dbt_project_path),
#         execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#         profile_config=profile_config,
#         render_config=RenderConfig(
#             select=[
#                 "path:models/cdc/base_seed_model.sql",
#                 "path:models/cdc/merge_seed.sql",
#             ],
#         )
#     )
#     run_final_model = DbtTaskGroup(
#         group_id="run_final_model",
#         project_config=ProjectConfig(dbt_project_path),
#         execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#         profile_config=profile_config,
#         render_config=RenderConfig(
#             select=[
#                 "path:models/cdc/final_data_table.sql",
#             ],
#         )
#     )

#     seed_data >> run_models >> run_final_model

# cosmosproject_buybox_dag_instance = cosmosproject_buybox_dag()

# For GCP

def cosmosproject_buybox_dag_gcp():
    seed_gcp_data = DbtTaskGroup(
        group_id="seed_gcp_data",
        project_config=ProjectConfig(dbt_project_path),
        execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["path:seeds"],  
        )
    )

    run_gcp_models = DbtTaskGroup(
        group_id="run_gcp_models",
        project_config=ProjectConfig(dbt_project_path),
        execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=[
                "path:models/cdc/base_seed_gcp.sql",
                "path:models/cdc/merge_seed_gcp.sql",
            ],
        )
    )
    run_final_model = DbtTaskGroup(
        group_id="run_final_model",
        project_config=ProjectConfig(dbt_project_path),
        execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=[
                "path:models/cdc/final_data_gcp.sql",
            ],
        )
    )

    seed_gcp_data >> run_gcp_models >> run_final_model

cosmosproject_buybox_dag_instance = cosmosproject_buybox_dag_gcp()


