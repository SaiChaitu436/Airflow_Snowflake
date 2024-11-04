from datetime import datetime
import os
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from airflow.decorators import dag
from pathlib import Path

# Define a Mixin class for logging
class LoggingMixin:
    def log_info(self, message):
        print(f"[INFO] {datetime.now()}: {message}")

    def log_error(self, message):
        print(f"[ERROR] {datetime.now()}: {message}")

# Your main DAG class incorporating the logging mixin
class DbtDagWithLogging(LoggingMixin):
    def __init__(self):
        os.environ['AIRFLOW_HOME'] = '/usr/local/airflow'
        self.dbt_project_path = Path("/usr/local/airflow/dags/dbt/cosmosproject_buybox/")
        self.dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
        self.profile_config = ProfileConfig(
            profile_name="cosmosproject_buybox",
            target_name="dev",
            profiles_yml_filepath=self.dbt_project_path / "profiles.yml",
        )

    def create_dag(self):
        @dag(
            start_date=datetime(2023, 9, 10),
            schedule="@daily",  # Runs daily
            catchup=False,
        )
        def run_model_separately_instance():
            self.log_info("Starting the DAG execution...")

            # Adding print statements for visibility
            print("[DEBUG] Preparing to initialize DbtTaskGroup...")
            self.log_info("Initializing the DbtTaskGroup...")

            try:
                seed_dag = DbtTaskGroup(
                    group_id="seed_dag",
                    project_config=ProjectConfig(self.dbt_project_path),
                    execution_config=ExecutionConfig(dbt_executable_path=self.dbt_executable_path),
                    profile_config=self.profile_config,
                    render_config=RenderConfig(
                        select=["path:models/src/src_salesranking.sql"],  
                    )
                )
                self.log_info("DbtTaskGroup initialized successfully.")
                print("Here is some standard text.")
                print("[DEBUG] DbtTaskGroup initialized without errors.")
            except Exception as e:
                self.log_error(f"Failed to initialize DbtTaskGroup: {e}")
                print("[ERROR] Exception encountered:", e)
                raise

            seed_dag

        return run_model_separately_instance()

# Instantiate and create the DAG
dbt_dag_with_logging = DbtDagWithLogging()
run_model_separately_instance_instance = dbt_dag_with_logging.create_dag()
