WITH source_data AS (
  SELECT *
  FROM airflow.dev.SINGLE_ASIN
)SELECT * ,current_timestamp() as created_at
FROM source_data
