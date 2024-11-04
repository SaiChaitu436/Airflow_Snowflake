{{ config(
    materialized='table',
    post_hook=[
        "TRUNCATE TABLE {{ ref('final_data_table') }}",
        "INSERT INTO {{ ref('final_data_table') }} SELECT * FROM {{ this }}"
    ]
) }}

WITH
new_data AS (
  SELECT *
  FROM {{ ref('base_seed_model') }}
),

existing_data AS (
  SELECT *
  FROM {{ ref('final_data_table') }}  
)

SELECT *
FROM new_data

UNION ALL

SELECT *
FROM existing_data
WHERE message_id NOT IN (SELECT message_id FROM new_data)
