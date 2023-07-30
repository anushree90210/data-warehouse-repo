WITH deduplicated_events AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp DESC) AS row_num
  FROM {{ ref("events") }}
)
SELECT
  insert_timestamp,
  event_id,
  event_type,
  user_id,
  event_timestamp
FROM deduplicated_events
WHERE row_num = 1;
