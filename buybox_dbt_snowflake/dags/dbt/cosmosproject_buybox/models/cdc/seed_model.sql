{{ config(
  materialized='table'
) }}

WITH seed_data AS (
  SELECT *
  FROM AIRFLOW.dev.SINGLE_ASIN
)
SELECT *
FROM seed_data
