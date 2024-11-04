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
def cosmosproject_buybox_dag():
    seed_data = DbtTaskGroup(
        group_id="seed_data",
        project_config=ProjectConfig(dbt_project_path),
        execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["path:seeds"],  
        )
    )

    run_models = DbtTaskGroup(
        group_id="run_models",
        project_config=ProjectConfig(dbt_project_path),
        execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=[
                "path:models/cdc/base_seed_model.sql",
                "path:models/cdc/merge_seed_final_table.sql",
            ],
        )
    )
    # run_final_model = DbtTaskGroup(
    #     group_id="run_final_model",
    #     project_config=ProjectConfig(dbt_project_path),
    #     execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
    #     profile_config=profile_config,
    #     render_config=RenderConfig(
    #         select=[
    #             "path:models/cdc/final_data_table.sql",
    #         ],
    #     )
    # )

    seed_data >> run_models 

cosmosproject_buybox_dag_instance = cosmosproject_buybox_dag()


# -------------------------------------------------------------------------
# from datetime import datetime
# import os
# import cosmos
# from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
# from cosmos.profiles import SnowflakeUserPasswordProfileMapping
# from pathlib import Path

# os.environ['AIRFLOW_HOME'] = '/usr/local/airflow'
# dbt_project_path = Path("/usr/local/airflow/dags/dbt/cosmosproject_buybox/")
# dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

# profile_config = ProfileConfig(
#     profile_name="cosmosproject_buybox",
#     target_name="dev",
#     profiles_yml_filepath=dbt_project_path / "profiles.yml",  # Ensure this points to the correct path
# )

# dbt_seed_model_cdc_dag = DbtDag(
#     project_config=ProjectConfig(dbt_project_path),
#     operator_args={"install_deps": True},
#     profile_config=profile_config,
#     execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#     render_config=RenderConfig(
#         select=["path:seeds",
#          "path:models/cdc/base_seed_model.sql",
#          "path:models/cdc/merge_seed.sql",
#          "path:models/cdc/final_data_table.sql",
         
#          ],  # Combine the select arguments into one list
#     ),
#     schedule_interval="@daily",
#     start_date=datetime(2023, 9, 10),
#     catchup=False,
#     dag_id="dbt_seed_model_cdc_dag",
# )


# from datetime import datetime
# import os
# import cosmos
# from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
# from cosmos.profiles import SnowflakeUserPasswordProfileMapping
# from pathlib import Path

# os.environ['AIRFLOW_HOME'] = '/usr/local/airflow'
# dbt_project_path = Path("/usr/local/airflow/dags/dbt/cosmosproject_buybox/")
# dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

# profile_config = ProfileConfig(
#     profile_name="cosmosproject_buybox",
#     target_name="dev",
#     profiles_yml_filepath=dbt_project_path / "profiles.yml",  # Ensure this points to the correct path
# )

# # 1. Seed DAG
# dbt_seed_dag = DbtDag(
#     project_config=ProjectConfig(dbt_project_path),
#     operator_args={"install_deps": True},
#     profile_config=profile_config,
#     execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#     render_config=RenderConfig(
#         select=["path:seeds"]
#     ),
#     schedule_interval="@daily",
#     start_date=datetime(2023, 9, 10),
#     catchup=False,
#     dag_id="dbt_seed_dag",
# )

# # 2. Base Seed Model DAG
# dbt_base_seed_model_dag = DbtDag(
#     project_config=ProjectConfig(dbt_project_path),
#     operator_args={"install_deps": True},
#     profile_config=profile_config,
#     execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#     render_config=RenderConfig(
#         select=["path:models/cdc/base_seed_model.sql"]
#     ),
#     schedule_interval="@daily",
#     start_date=datetime(2023, 9, 10),
#     catchup=False,
#     dag_id="dbt_base_seed_model_dag",
# )

# # 3. Merge Seed DAG
# dbt_merge_seed_dag = DbtDag(
#     project_config=ProjectConfig(dbt_project_path),
#     operator_args={"install_deps": True},
#     profile_config=profile_config,
#     execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#     render_config=RenderConfig(
#         select=["path:models/cdc/merge_seed.sql"]
#     ),
#     schedule_interval="@daily",
#     start_date=datetime(2023, 9, 10),
#     catchup=False,
#     dag_id="dbt_merge_seed_dag",
# )

# # 4. Final Data Table DAG
# dbt_final_data_table_dag = DbtDag(
#     project_config=ProjectConfig(dbt_project_path),
#     operator_args={"install_deps": True},
#     profile_config=profile_config,
#     execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#     render_config=RenderConfig(
#         select=["path:models/cdc/final_data_table.sql"]
#     ),
#     schedule_interval="@daily",
#     start_date=datetime(2023, 9, 10),
#     catchup=False,
#      dag_id="dbt_final_data_table_dag",
# )

# # Group the DAGs
# # dbt_seed_dag >> dbt_base_seed_model_dag>> dbt_merge_seed_dag >>  dbt_final_data_table_dag

# from datetime import datetime
# import os
# from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
# from airflow import DAG
# from airflow.operators.dummy import DummyOperator
# from cosmos.operators import DbtRunOperator
# from pathlib import Path

# # Set up environment
# os.environ['AIRFLOW_HOME'] = '/usr/local/airflow'
# dbt_project_path = Path("/usr/local/airflow/dags/dbt/cosmosproject_buybox/")
# dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

# # Define profile config
# profile_config = ProfileConfig(
#     profile_name="cosmosproject_buybox",
#     target_name="dev",
#     profiles_yml_filepath=dbt_project_path / "profiles.yml",  # Ensure this points to the correct path
# )

# # Define the DAG
# with DAG(
#     dag_id="dbt_task_separated_dag",
#     schedule_interval="@daily",  # Set daily schedule
#     start_date=datetime(2023, 9, 10),
#     catchup=False,
# ) as dag:

#     # Task to install dependencies (optional)
#     install_deps = DummyOperator(
#         task_id="install_dependencies",
#         dag=dag,
#     )

#     # Define tasks for each part of your DBT process
#     seed_task = DbtRunOperator(
#         task_id="run_db_seeds",
#         project_dir=str(dbt_project_path),
#         project_config=ProjectConfig(dbt_project_path),
#         profile_config=profile_config,
#         execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#         select="path:seeds",
#         install_deps=True,  # Optional, installs dependencies
#         dag=dag,
#     )

#     base_seed_model_task = DbtRunOperator(
#         task_id="run_base_seed_model",
#         project_dir=str(dbt_project_path),
#         project_config=ProjectConfig(dbt_project_path),
#         profile_config=profile_config,
#         execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#         select="path:models/cdc/base_seed_model.sql",
#         dag=dag,
#     )

#     merge_seed_task = DbtRunOperator(
#         task_id="run_merge_seed",
#         project_dir=str(dbt_project_path),
#         project_config=ProjectConfig(dbt_project_path),
#         profile_config=profile_config,
#         execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#         select="path:models/cdc/merge_seed.sql",
#         dag=dag,
#     )

#     final_data_table_task = DbtRunOperator(
#         task_id="run_final_data_table",
#         project_dir=str(dbt_project_path),
#         project_config=ProjectConfig(dbt_project_path),
#         profile_config=profile_config,
#         execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#         select="path:models/cdc/final_data_table.sql",
#         dag=dag,
#     )

#     # Define task dependencies (sequential flow)
#     install_deps >> seed_task >> base_seed_model_task >> merge_seed_task >> final_data_table_task


# ------------------------------------------------------------------------------------------------------

# from datetime import datetime
# import os
# from pathlib import Path
# from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
# from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# # Set the AIRFLOW_HOME environment variable
# os.environ['AIRFLOW_HOME'] = '/usr/local/airflow'

# # Define the paths for the dbt project and executable
# dbt_project_path = Path("/usr/local/airflow/dags/dbt/cosmosproject_buybox/")
# dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

# # Create profile configuration
# profile_config = ProfileConfig(
#     profile_name="cosmosproject_buybox",
#     target_name="dev",
#     profiles_yml_filepath=dbt_project_path / "profiles.yml"  # Ensure this points to the correct path
# )

# # Create execution configuration
# execution_config = ExecutionConfig(
#     dbt_executable_path=dbt_executable_path,
# )

# # Create render configuration
# render_config = RenderConfig(
#     select=[
#         "path:seeds",
#         "path:models/cdc/base_seed_model.sql",
#         "path:models/cdc/merge_seed.sql",
#         "path:models/cdc/final_data_table.sql",
#     ]
# )

# # Create the DbtDag
# dbt_seed_model_cdc_dag = DbtDag(
#     project_config=ProjectConfig(dbt_project_path),
#     operator_args={"install_deps": True},
#     profile_config=profile_config,
#     execution_config=execution_config,
#     render_config=render_config,
#     schedule_interval="@daily",
#     start_date=datetime(2023, 9, 10),
#     catchup=False,
#     dag_id="dbt_seed_model_cdc_dag",
# )

# ----------------------------------------------------------------------------------------------

# from airflow.decorators import dag
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig 
# from pendulum import datetime
# import os
# from pathlib import Path
# os.environ['AIRFLOW_HOME'] = '/usr/local/airflow'
# dbt_project_path = Path("/usr/local/airflow/dags/dbt/cosmosproject_buybox/")
# dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

# profile_config = ProfileConfig(
#     profile_name="cosmosproject_buybox",
#     target_name="dev",
#     profiles_yml_filepath=dbt_project_path / "profiles.yml",    
# )

# # Execution configuration for dbt
# # execution_config = ExecutionConfig(
# #     dbt_executable_path=dbt_executable_path,
# #     project_path=str(dbt_project_path) 
# # )

# @dag(
#     start_date=datetime(2023, 9, 10),
#     schedule="@daily",  # Runs daily
#     catchup=False,
# )
# def cosmosproject_buybox_dag():
#     # Task group for running dbt seeds
#     seed_data = DbtTaskGroup(
#         group_id="seed_data",
#         project_config=ProjectConfig(dbt_project_path),
#         execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#         profile_config=profile_config,
#         render_config={
#             "select": ["path:seeds"],  # Specify seed paths
#         },
#         default_args={"retries": 2},  # Retry on failure
#     )

#     # Task group for running dbt models
#     run_models = DbtTaskGroup(
#         group_id="run_models",
#         project_config=ProjectConfig(dbt_project_path),
#         execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#         profile_config=profile_config,
#         render_config={
#             "select": [
#                 "path:models/cdc/base_seed_model.sql",
#                 "path:models/cdc/merge_seed.sql",
#                 "path:models/cdc/final_data_table.sql",
#             ],  # Specify the models to run
#         },
#         default_args={"retries": 2},  # Retry on failure
#     )

   

#     # DAG flow: seed_data -> run_models -> query_final_table
#     seed_data >> run_models 


# # Create the DAG object
# cosmosproject_buybox_dag()

# ------------------------------------------------------------------------------------------


# from datetime import datetime
# import os
# from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
# from airflow.decorators import dag
# from pathlib import Path

# os.environ['AIRFLOW_HOME'] = '/usr/local/airflow'
# dbt_project_path = Path("/usr/local/airflow/dags/dbt/cosmosproject_buybox/")
# dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

# profile_config = ProfileConfig(
#     profile_name="cosmosproject_buybox",
#     target_name="dev",
#     profiles_yml_filepath=dbt_project_path / "profiles.yml",
# )

# @dag(
#     start_date=datetime(2023, 9, 10),
#     schedule="@daily",  # Runs daily
#     catchup=False,
# )
# def cosmosproject_buybox_dag():
#     # Task group for running dbt seeds
#     seed_data = DbtTaskGroup(
#         group_id="seed_data",
#         project_config=ProjectConfig(dbt_project_path),
#         execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#         profile_config=profile_config,
#         render_config=RenderConfig(
#             select=["path:seeds"],  # Specify seed paths
#         )
#     )

#     # Task group for running dbt models
#     run_models = DbtTaskGroup(
#         group_id="run_models",
#         project_config=ProjectConfig(dbt_project_path),
#         execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
#         profile_config=profile_config,
#         render_config=RenderConfig(
#             select=[
#                 "path:models/cdc/base_seed_model.sql",
#                 "path:models/cdc/merge_seed.sql",
#                 "path:models/cdc/final_data_table.sql",
#             ],
#         )
#     )

#     seed_data >> run_models 

# cosmosproject_buybox_dag()

