from datetime import datetime
import os
import cosmos
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig ,RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from pathlib import Path


# For Snowflake

# os.environ['AIRFLOW_HOME'] = '/usr/local/airflow'
# dbt_project_path = Path("/usr/local/airflow/dags/dbt/cosmosproject_buybox/")
# dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

# profile_config = ProfileConfig(
#     profile_name="cosmosproject_buybox",
#     target_name="dev",
#     profiles_yml_filepath=dbt_project_path / "profiles.yml",  # Ensure this points to the correct path

# )

# dbt_snowflake_dag = DbtDag(
#     project_config=ProjectConfig(dbt_project_path),
#     operator_args={"install_deps": True},
#     profile_config=profile_config,
#     execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#     render_config=RenderConfig(
#         select=["path:models/raw_data.sql"],
#     ),
#     schedule_interval="@daily",
#     start_date=datetime(2023, 9, 10),
#     catchup=False,
#     dag_id="dbt_snowflake_dag",
# )


#For GCP

os.environ['AIRFLOW_HOME'] = '/usr/local/airflow'
dbt_project_path = Path("/usr/local/airflow/dags/dbt/cosmosproject_buybox/")
dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="cosmosproject_buybox",
    target_name="dev",
    profiles_yml_filepath=dbt_project_path / "profiles.yml",  # Ensure this points to the correct path

)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig(dbt_project_path),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
    render_config=RenderConfig(
        select=["path:models/src_orders.sql"],
    ),
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_gcp_dag",
)
