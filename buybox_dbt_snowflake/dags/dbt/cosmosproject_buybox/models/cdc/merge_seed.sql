WITH
-- New data from the source
new_data AS (
  SELECT *
  FROM {{ ref('base_model') }}  -- Fresh data from the source model
),

-- Existing seed data from the previous run
existing_data AS (
  SELECT *
  FROM {{ ref('seed_model') }}  -- The physical table created from seeding
)

-- Combine new and existing data
SELECT *
FROM new_data
UNION ALL
SELECT *
FROM existing_data
WHERE message_id NOT IN (SELECT message_id FROM new_data)
