-- One row per session. Aggregates plays within a session and captures
-- the user's level/location at the time (taken from the first event).
WITH session_aggs AS (
    SELECT
        e.session_id,
        e.user_id,
        MIN(e.ts)                                AS started_at,
        MAX(e.ts)                                AS ended_at,
        COUNT(*)                                 AS play_count,
        COUNT(DISTINCT e.song_id)                AS distinct_songs,
        COUNT(DISTINCT e.artist_id)              AS distinct_artists,
        COALESCE(SUM(sv.duration), 0)            AS total_listening_seconds
    FROM iceberg.transactions.events e
    LEFT JOIN iceberg.transactions.song_versions sv
           ON sv.version_id = e.version_id
    GROUP BY e.session_id, e.user_id
),
session_context AS (
    SELECT
        session_id,
        level,
        location,
        ROW_NUMBER() OVER (
            PARTITION BY session_id
            ORDER BY ts ASC
        ) AS rn
    FROM iceberg.transactions.events
)
SELECT
    sa.session_id,
    sa.user_id,
    sa.started_at,
    sa.ended_at,
    UNIX_TIMESTAMP(sa.ended_at) - UNIX_TIMESTAMP(sa.started_at) AS duration_seconds,
    sa.play_count,
    sa.distinct_songs,
    sa.distinct_artists,
    sa.total_listening_seconds,
    sc.level,
    sc.location
FROM session_aggs sa
LEFT JOIN session_context sc
       ON sc.session_id = sa.session_id
      AND sc.rn = 1