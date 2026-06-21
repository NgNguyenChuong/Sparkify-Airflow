-- One row per song. Aggregates plays across all versions of the song.
-- Includes songs in the catalog with zero plays (LEFT JOIN from songs).
WITH play_aggs AS (
    SELECT
        e.song_id,
        COUNT(*)                                       AS total_plays,
        COUNT(DISTINCT e.user_id)                      AS distinct_listeners,
        COUNT(DISTINCT e.session_id)                   AS distinct_sessions,
        COUNT(DISTINCT e.version_id)                   AS distinct_versions_played,
        MIN(e.ts)                                      AS first_played_at,
        MAX(e.ts)                                      AS last_played_at,
        SUM(CASE WHEN e.level = 'paid' THEN 1 ELSE 0 END) AS paid_play_count
    FROM iceberg.transactions.events e
    WHERE e.song_id IS NOT NULL
    GROUP BY e.song_id
)
SELECT
    s.song_id,
    s.title,
    s.artist_id,
    COALESCE(pa.total_plays, 0)              AS total_plays,
    COALESCE(pa.distinct_listeners, 0)       AS distinct_listeners,
    COALESCE(pa.distinct_sessions, 0)        AS distinct_sessions,
    COALESCE(pa.distinct_versions_played, 0) AS distinct_versions_played,
    pa.first_played_at,
    pa.last_played_at,
    CASE
        WHEN COALESCE(pa.total_plays, 0) = 0 THEN 0.0
        ELSE CAST(pa.paid_play_count AS DOUBLE) / pa.total_plays
    END AS paid_play_share
FROM iceberg.transactions.songs s
LEFT JOIN play_aggs pa ON pa.song_id = s.song_id