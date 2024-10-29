WITH
new_data AS (
  SELECT *
  FROM dbtproject-439612.wellbefore.SINGLE_ASIN
),

existing_data AS (
  SELECT *
  FROM dbtproject-439612.wellbefore.SINGLE_ASIN
)

SELECT *
FROM new_data
UNION ALL
SELECT *
FROM existing_data
WHERE message_id NOT IN (SELECT message_id FROM new_data)
