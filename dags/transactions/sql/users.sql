-- One row per user_id. Takes the most recent values for mutable fields
-- (name, gender, location) from the user's latest event.
WITH ranked AS (
    SELECT
        CAST(userid AS BIGINT)              AS user_id,
        firstname                           AS first_name,
        lastname                            AS last_name,
        gender,
        location,
        CAST(from_unixtime(registration / 1000) AS TIMESTAMP) AS registration,
        CAST(from_unixtime(ts / 1000) AS TIMESTAMP) AS event_ts,
        ROW_NUMBER() OVER (
            PARTITION BY userid
            ORDER BY ts DESC
        ) AS rn
    FROM iceberg.raw.logs
    WHERE userid IS NOT NULL
      AND userid <> ''
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
