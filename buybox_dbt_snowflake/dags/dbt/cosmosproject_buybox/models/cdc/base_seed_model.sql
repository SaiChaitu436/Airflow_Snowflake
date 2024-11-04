WITH source_data AS (
  SELECT *
  FROM `de-coe.buybox_dataset.BUYBOX_RAW_DATA`
)
SELECT 
  message_id,
  message_body,
  TIMESTAMP(event_time) AS event_time, 
  TIMESTAMP(create_time) AS create_time, 
  TIMESTAMP(publish_time) AS publish_time, 
  batch_id,
  CURRENT_TIMESTAMP() AS created_at
FROM source_data
