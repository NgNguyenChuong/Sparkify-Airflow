-- One row per song_id (the song concept, not the version). Picks the
-- canonical title and artist association from the first version encountered.
-- Year of 0 in the source means "unknown" — normalized to NULL.
WITH ranked AS (
    SELECT
        song_id,
        title,
        artist_id,
        CASE WHEN year = 0 THEN NULL ELSE year END AS year,
        ROW_NUMBER() OVER (
            PARTITION BY song_id
            ORDER BY year DESC
        ) AS rn
    FROM iceberg.raw.songs
    WHERE song_id IS NOT NULL
      AND song_id <> ''
      AND artist_id IS NOT NULL
      AND artist_id <> ''
      AND title IS NOT NULL
      AND data_interval = '{{ ti.xcom_pull(task_ids="metadata")["data_interval"] }}'
)
SELECT
    song_id,
    title,
    artist_id,
    year
FROM ranked
WHERE rn = 1