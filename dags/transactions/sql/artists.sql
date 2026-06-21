-- One row per artist_id. Picks the version with the most complete metadata
-- (non-null location preferred) when duplicates exist.
WITH ranked AS (
    SELECT
        artist_id,
        artist_name,
        artist_location  AS location,
        artist_latitude  AS latitude,
        artist_longitude AS longitude,
        ROW_NUMBER() OVER (
            PARTITION BY artist_id
            ORDER BY
                CASE WHEN artist_location IS NOT NULL AND artist_location <> '' THEN 0 ELSE 1 END,
                CASE WHEN artist_latitude IS NOT NULL THEN 0 ELSE 1 END
        ) AS rn
    FROM iceberg.raw.songs
    WHERE artist_id IS NOT NULL
      AND artist_id <> ''
      AND data_interval = '{{ ti.xcom_pull(task_ids="metadata")["data_interval"] }}'
)
SELECT
    artist_id,
    artist_name,
    location,
    latitude,
    longitude
FROM ranked
WHERE rn = 1