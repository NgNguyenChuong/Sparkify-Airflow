-- One row per artist. Aggregates plays across the artist's entire catalog.
-- Includes artists in the catalog with zero plays (LEFT JOIN from artists).
WITH play_aggs AS (
    SELECT
        e.artist_id,
        COUNT(*)                                       AS total_plays,
        COUNT(DISTINCT e.user_id)                      AS distinct_listeners,
        COUNT(DISTINCT e.song_id)                      AS distinct_songs_played,
        COUNT(DISTINCT e.session_id)                   AS distinct_sessions,
        MIN(e.ts)                                      AS first_played_at,
        MAX(e.ts)                                      AS last_played_at,
        SUM(CASE WHEN e.level = 'paid' THEN 1 ELSE 0 END) AS paid_play_count
    FROM iceberg.transactions.events e
    WHERE e.artist_id IS NOT NULL
    GROUP BY e.artist_id
)
SELECT
    a.artist_id,
    a.artist_name,
    COALESCE(pa.total_plays, 0)            AS total_plays,
    COALESCE(pa.distinct_listeners, 0)     AS distinct_listeners,
    COALESCE(pa.distinct_songs_played, 0)  AS distinct_songs_played,
    COALESCE(pa.distinct_sessions, 0)      AS distinct_sessions,
    pa.first_played_at,
    pa.last_played_at,
    CASE
        WHEN COALESCE(pa.total_plays, 0) = 0 THEN 0.0
        ELSE CAST(pa.paid_play_count AS DOUBLE) / pa.total_plays
    END AS paid_play_share
FROM iceberg.transactions.artists a
LEFT JOIN play_aggs pa ON pa.artist_id = a.artist_id