WITH source_data AS (
  SELECT *
  FROM dbtproject-439612.wellbefore.SINGLE_ASIN
)SELECT * ,current_timestamp() as created_at
FROM source_data
