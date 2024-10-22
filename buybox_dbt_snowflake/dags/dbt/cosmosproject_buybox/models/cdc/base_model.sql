WITH source_data AS (
  SELECT *
  FROM {{ ref('raw_data') }}
)SELECT *
FROM source_data
