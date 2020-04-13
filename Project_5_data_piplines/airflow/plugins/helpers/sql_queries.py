class SqlQueries:
    songplay_table_insert = ("""
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, sessionId, location, user_agent)
        SELECT 
            DISTINCT
            se.ts as start_time,
            se.userId AS user_id,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.sessionId,
            se.location,
            se.userAgent as user_agent
        FROM staging_events se 
        LEFT JOIN staging_songs ss 
            ON se.song = ss.title
            AND se.artist = ss.artist_name
        WHERE se.page = 'NextSong'
    """)

    user_table_insert = ("""
        INSERT INTO users 
        SELECT 
            user_id,
            first_name,
            last_name,
            gender,
            level
        FROM (
                SELECT
                    ROW_NUMBER() OVER (PARTITION BY userId ORDER BY ts DESC) AS ROW_NUM,
                    userId as user_id,
                    firstName as first_name,
                    lastName as last_name,
                    gender,
                    level
                FROM staging_events se
                WHERE userId IS NOT NULL
        ) T
        WHERE T.ROW_NUM = 1
    """)

    song_table_insert = ("""
        INSERT INTO songs 
        SELECT
            DISTINCT(song_id) as song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs ss
        WHERE song_id IS NOT NULL
    """)

    artist_table_insert = ("""
        INSERT INTO artists 
        SELECT
            artist_id,
            name,
            location,
            latitude,
            longitude
        FROM (
                SELECT
                    ROW_NUMBER() OVER(PARTITION BY artist_id ORDER BY artist_name) AS row_num,
                    artist_id,
                    ss.artist_name as name,
                    ss.artist_location as location,
                    ss.artist_latitude as latitude,
                    ss.artist_longitude as longitude
                FROM staging_songs ss
                WHERE artist_id is not null
        ) T1
        WHERE T1.row_num = 1
    """)

    time_table_insert = ("""
        INSERT INTO time 
        SELECT
            DISTINCT(ts) as start_time,
            EXTRACT(hrs FROM ts) as hour,
            EXTRACT(d FROM ts) as day,
            EXTRACT(w FROM ts) as week,
            EXTRACT(mons FROM ts) as month,
            EXTRACT(yrs FROM ts) as year,
            EXTRACT(weekday FROM ts) as weekday
        FROM staging_events se
        WHERE ts IS NOT NULL
            AND se.page = 'NextSong'
    """)