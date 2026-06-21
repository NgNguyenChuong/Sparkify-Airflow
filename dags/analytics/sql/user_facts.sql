-- One row per user. Aggregates listening behavior and folds in current
-- subscription status from user_levels.
WITH event_aggs AS (
    SELECT
        e.user_id,
        COUNT(*)                                 AS total_plays,
        COUNT(DISTINCT e.session_id)             AS total_sessions,
        COALESCE(SUM(sv.duration), 0)            AS total_listening_seconds,
        COUNT(DISTINCT e.song_id)                AS distinct_songs_played,
        COUNT(DISTINCT e.artist_id)              AS distinct_artists_played,
        MIN(e.ts)                                AS first_seen_at,
        MAX(e.ts)                                AS last_seen_at
    FROM iceberg.transactions.events e
    LEFT JOIN iceberg.transactions.song_versions sv
           ON sv.version_id = e.version_id
    GROUP BY e.user_id
),
current_levels AS (
    SELECT
        user_id,
        new_level AS current_level,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY changed_at DESC
        ) AS rn
    FROM iceberg.transactions.user_levels
),
ever_paid AS (
    SELECT DISTINCT user_id
    FROM iceberg.transactions.user_levels
    WHERE new_level = 'paid' AND source = 'transition'
)
SELECT
    u.user_id,
    COALESCE(ea.total_plays, 0)              AS total_plays,
    COALESCE(ea.total_sessions, 0)           AS total_sessions,
    COALESCE(ea.total_listening_seconds, 0)  AS total_listening_seconds,
    COALESCE(ea.distinct_songs_played, 0)    AS distinct_songs_played,
    COALESCE(ea.distinct_artists_played, 0)  AS distinct_artists_played,
    ea.first_seen_at,
    ea.last_seen_at,
    cl.current_level,
    CASE WHEN ep.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_converted
FROM iceberg.transactions.users u
LEFT JOIN event_aggs    ea ON ea.user_id = u.user_id
LEFT JOIN current_levels cl ON cl.user_id = u.user_id AND cl.rn = 1
LEFT JOIN ever_paid      ep ON ep.user_id = u.user_id