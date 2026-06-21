-- One row per (song_id, duration) — the discriminating attributes for a
-- playable version. version_id is a deterministic hash so the same physical
-- version produces the same key across runs.
WITH ranked AS (
    SELECT
        SHA2(CONCAT(song_id, ':', CAST(duration AS STRING)), 256) AS version_id,
        song_id,
        CAST(duration AS DOUBLE) AS duration,
        ROW_NUMBER() OVER (
            PARTITION BY song_id, duration
            ORDER BY song_id
        ) AS rn
    FROM iceberg.raw.songs
    WHERE song_id  IS NOT NULL
      AND song_id  <> ''
      AND duration IS NOT NULL
      AND duration > 0
      AND data_interval = '{{ ti.xcom_pull(task_ids="metadata")["data_interval"] }}'
)
SELECT
    version_id,
    song_id,
    duration
FROM ranked
WHERE rn = 1