WITH source_data AS (
  SELECT *
  FROM DAG_DB.DEV.SINGLE_ASIN
)SELECT * ,current_timestamp() as created_at
FROM source_data
