-- Fact table: one row per song play (page = 'NextSong'). event_id is a
-- deterministic surrogate from (user_id, session_id, item_in_session).
-- version_id is resolved via (song, artist, length) lookup with a small
-- duration tolerance to absorb floating-point precision.
WITH plays AS (
    SELECT
        SHA2(CONCAT_WS(':',
            CAST(userid          AS STRING),
            CAST(sessionid       AS STRING),
            CAST(iteminsession   AS STRING)
        ), 256)                                                AS event_id,
        CAST(from_unixtime(ts / 1000) AS TIMESTAMP)            AS ts,
        CAST(userid    AS BIGINT)                              AS user_id,
        CAST(sessionid AS BIGINT)                              AS session_id,
        CAST(iteminsession AS INT)                             AS item_in_session,
        artist                                                 AS artist_name_raw,
        song                                                   AS song_title_raw,
        CAST(length     AS DOUBLE)                             AS length,
        level,
        location,
        useragent                                    AS user_agent,
        CAST(from_unixtime(ts / 1000) AS DATE)       AS event_date
    FROM iceberg.raw.logs
    WHERE page    = 'NextSong'
      AND userid IS NOT NULL
      AND userid <> ''
      AND song    IS NOT NULL
      AND artist  IS NOT NULL
      AND length  IS NOT NULL
      AND length  > 0
      AND data_interval = '{{ ti.xcom_pull(task_ids="metadata")["data_interval"] }}'
),
resolved AS (
    SELECT
        p.event_id,
        p.ts,
        p.user_id,
        p.session_id,
        p.item_in_session,
        sv.version_id,
        sv.song_id,
        s.artist_id,
        p.level,
        p.location,
        p.user_agent,
        p.event_date,
        ROW_NUMBER() OVER (
            PARTITION BY p.event_id
            ORDER BY ABS(sv.duration - p.length) ASC
        ) AS rn
    FROM plays p
    LEFT JOIN iceberg.transactions.songs   s
           ON s.title = p.song_title_raw
    LEFT JOIN iceberg.transactions.artists a
           ON a.artist_id   = s.artist_id
          AND a.artist_name = p.artist_name_raw
    LEFT JOIN iceberg.transactions.song_versions sv
           ON sv.song_id  = s.song_id
          AND ABS(sv.duration - p.length) < 0.01
)
SELECT
    event_id,
    ts,
    user_id,
    session_id,
    item_in_session,
    version_id,
    song_id,
    artist_id,
    level,
    location,
    user_agent,
    event_date
FROM resolved
WHERE rn = 1
