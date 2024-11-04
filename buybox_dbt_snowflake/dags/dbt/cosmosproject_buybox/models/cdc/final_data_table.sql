WITH source_data AS (
    SELECT * 
    FROM {{ ref('base_seed_model') }}  
)
SELECT 
  message_id,
  message_body,
  TIMESTAMP(event_time) AS event_time, 
  TIMESTAMP(create_time) AS create_time, 
  TIMESTAMP(publish_time) AS publish_time, 
  batch_id,
  created_at
FROM source_data