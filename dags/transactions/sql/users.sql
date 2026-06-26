-- One row per user_id. Takes the most recent values for mutable fields
-- (name, gender, location) from the user's latest event.
WITH ranked AS (
    SELECT
        CAST(user_id AS BIGINT)              AS user_id,
        first_name                           AS first_name,
        last_name                            AS last_name,
        gender,
        location,
        CAST(from_unixtime(registration / 1000) AS TIMESTAMP) AS registration,
        CAST(from_unixtime(ts / 1000) AS TIMESTAMP) AS event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY ts DESC
        ) AS rn
    FROM iceberg.raw.logs
    WHERE user_id IS NOT NULL
      AND user_id <> ''
      AND auth = 'Logged In'
      AND data_interval = '{{ ti.xcom_pull(task_ids="metadata")["data_interval"] }}'
)
SELECT
    user_id,
    first_name,
    last_name,
    gender,
    location,
    registration,
    event_ts AS last_seen_at
FROM ranked
WHERE rn = 1
