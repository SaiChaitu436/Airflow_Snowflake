# # FROM quay.io/astronomer/astro-runtime:12.2.0


# # FROM quay.io/astronomer/astro-runtime:9.1.0-python-3.9-base

# # RUN python -m venv dbt_venv && \
# #     . dbt_venv/bin/activate && \
# #     pip install --no-cache-dir dbt-snowflake==1.5.4 && \
# #     pip install --no-cache-dir astronomer-cosmos && \
# #     deactivate

# # Set the working directory
# # WORKDIR /app

# # # Copy the requirements.txt file into the image
# # COPY requirements.txt .

# # RUN pip install requirements.txt

# # # Create a virtual environment and install dependencies
# # RUN python -m venv dbt_venv && \
# #     . dbt_venv/bin/activate && \
# #     pip install --no-cache-dir -r requirements.txt && \
# #     deactivate

# FROM python:3.10-slim
# # Use the base image for Astronomer Runtime
# FROM quay.io/astronomer/astro-runtime:9.1.0-python-3.9-base

# # Set the working directory
# WORKDIR /app

# # Copy the requirements.txt file into the image
# COPY requirements.txt .

# # Install required system packages (optional, if needed for any specific dependency)
# # RUN apt-get update && apt-get install -y <required-system-packages>

# # Install dependencies from requirements.txt
# RUN pip install --no-cache-dir -r requirements.txt

# # Create a virtual environment and install additional dependencies for dbt
# RUN python -m venv dbt_venv && \
#     . dbt_venv/bin/activate && \
#     pip install --no-cache-dir dbt==1.0.0.38.14 dbt-snowflake==1.8.3 astronomer-cosmos==1.7.0 && \
#     deactivate

#########################
# # Use the Astronomer runtime base image with Python 3.10
# FROM quay.io/astronomer/astro-runtime:9.1.0-python-3.10-base

# # Set the working directory
# WORKDIR /app

# # Copy the requirements.txt file into the image
# COPY requirements.txt .

# # Install required system packages (optional, if needed for any specific dependency)
# # RUN apt-get update && apt-get install -y <required-system-packages>

# # Upgrade pip and install dependencies from requirements.txt
# RUN pip install --upgrade pip && \
#     pip install --no-cache-dir -r requirements.txt

# # Create a virtual environment and install additional dependencies for dbt
# RUN python -m venv dbt_venv && \
#     . dbt_venv/bin/activate && \
#     pip install --no-cache-dir dbt==1.0.0.38.14 dbt-snowflake==1.8.3 astronomer-cosmos==1.7.0 && \
#     deactivate

###############################
# Use the Astronomer runtime base image with Python 3.10
FROM quay.io/astronomer/astro-runtime:9.1.0-python-3.10-base

# Set the working directory
WORKDIR /app

# Set the AIRFLOW_HOME environment variable
ENV AIRFLOW_HOME=/usr/local/airflow

# Copy the requirements.txt file into the image
COPY requirements.txt .

# Install required system packages (optional, if needed for any specific dependency)
# RUN apt-get update && apt-get install -y <required-system-packages>

# Upgrade pip and install dependencies from requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Create a virtual environment inside the AIRFLOW_HOME and install additional dependencies for dbt
RUN python -m venv $AIRFLOW_HOME/dbt_venv && \
    . $AIRFLOW_HOME/dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt==1.0.0.38.14 dbt-snowflake==1.8.3 dbt-bigquery==1.8.3 astronomer-cosmos==1.7.0 && \
    deactivate

