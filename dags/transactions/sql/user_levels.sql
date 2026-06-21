-- One row per level transition. Composite PK (user_id, changed_at).
-- Captures both explicit transitions (Submit Upgrade / Submit Downgrade)
-- and an initial 'seed' row from each user's first event.
WITH transitions AS (
    SELECT
        CAST(userid AS BIGINT)              AS user_id,
        CAST(from_unixtime(ts / 1000) AS TIMESTAMP) AS changed_at,
        CASE
            WHEN page = 'Submit Upgrade'   THEN 'paid'
            WHEN page = 'Submit Downgrade' THEN 'free'
        END                                  AS new_level,
        'transition'                         AS source
    FROM iceberg.raw.logs
    WHERE userid IS NOT NULL
      AND userid <> ''
      AND page IN ('Submit Upgrade', 'Submit Downgrade')
      AND data_interval = '{{ ti.xcom_pull(task_ids="metadata")["data_interval"] }}'
),
initial_seed AS (
    SELECT
        user_id,
        changed_at,
        new_level,
        'initial' AS source
    FROM (
        SELECT
            CAST(userid AS BIGINT)       AS user_id,
            CAST(from_unixtime(ts / 1000) AS TIMESTAMP) AS changed_at,
            level                        AS new_level,
            ROW_NUMBER() OVER (
                PARTITION BY userid
                ORDER BY ts ASC
            ) AS rn
        FROM iceberg.raw.logs
        WHERE userid IS NOT NULL
          AND userid <> ''
          AND auth = 'Logged In'
          AND level IS NOT NULL
    )
    WHERE rn = 1
),
combined AS (
    SELECT * FROM transitions
    UNION ALL
    SELECT * FROM initial_seed
),
ranked AS (
    SELECT
        user_id,
        changed_at,
        new_level,
        source,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, changed_at
            ORDER BY CASE WHEN source = 'transition' THEN 0 ELSE 1 END
        ) AS rn
    FROM combined
)
SELECT
    user_id,
    changed_at,
    new_level,
    source
FROM ranked
WHERE rn = 1
