from datetime import datetime
import os
from pathlib import Path
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from airflow.decorators import dag
from airflow.utils.log.logging_mixin import LoggingMixin

# Use LoggingMixin for better integration with Airflow's logging system
class CustomLogger(LoggingMixin):
    pass

# Create an instance of CustomLogger
logger = CustomLogger().log

# Define Airflow home and paths
os.environ['AIRFLOW_HOME'] = '/usr/local/airflow'
dbt_project_path = Path("/usr/local/airflow/dags/dbt/cosmosproject_buybox/")
dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

# Setup DBT profile configuration
profile_config = ProfileConfig(
    profile_name="cosmosproject_buybox",
    target_name="dev",
    profiles_yml_filepath=dbt_project_path / "profiles.yml",
)

@dag(
    start_date=datetime(2024, 9, 10),
    schedule="@daily",
    catchup=False,
)
def materialised_models_dag():
    logger.info("Starting the materialised_models_dag...")

    # Initialize src_model_dag
    logger.info("Initializing src_model_dag...")
    src_model_dag = DbtTaskGroup(
        group_id="src_model_dag",
        project_config=ProjectConfig(dbt_project_path),
        execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["path:models/src"],
        )
    )
    logger.info("src_model_dag initialized successfully.")

    # Initialize fact_model_dag with full refresh
    logger.info("Initializing fact_model_dag with full refresh...")
    fact_model_dag = DbtTaskGroup(
        group_id="fact_model_dag",
        project_config=ProjectConfig(dbt_project_path),
        execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
        # execution_config=ExecutionConfig(
        #     dbt_executable_path=f"{dbt_executable_path} --full-refresh"
        # ),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["path:models/fact"]
        )
    )



    logger.info("fact_model_dag initialized successfully.")

    # Set task dependencies
    logger.info("Setting task dependencies: src_model_dag >> fact_model_dag")
    src_model_dag >> fact_model_dag

# Instantiate the DAG
logger.info("Creating materialised_models_dag_instance...")
materialised_models_dag_instance = materialised_models_dag()
logger.info("materialised_models_dag_instance created successfully.")
