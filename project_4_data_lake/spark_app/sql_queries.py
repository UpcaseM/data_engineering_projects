# Queries to create fact and dimension tables

sql_songplays = '''

SELECT 
    DISTINCT
    l.time as start_time,
    l.userId AS user_id,
    l.level,
    s.song_id,
    s.artist_id,
    l.sessionId,
    l.location,
    l.userAgent as user_agent,
    EXTRACT(month FROM l.time) as month,
    EXTRACT(year FROM l.time) as year
FROM tb_log l 
LEFT JOIN tb_song s
    ON l.song = s.title
    AND l.artist = s.artist_name
WHERE l.page = 'NextSong'

'''

# Select unique users
sql_users = '''

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
        FROM tb_log l
        WHERE userId IS NOT NULL
) T
WHERE T.ROW_NUM = 1

'''

sql_songs = '''

SELECT
    DISTINCT(song_id) as song_id,
    title,
    artist_id,
    year,
    duration
FROM tb_song
WHERE song_id IS NOT NULL

'''

# Select unqiue artist id
sql_artists = '''

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
            s.artist_name as name,
            s.artist_location as location,
            s.artist_latitude as latitude,
            s.artist_longitude as longitude
        FROM tb_song s
        WHERE artist_id is not null
) T1
WHERE T1.row_num = 1

'''

sql_time = '''

SELECT
    DISTINCT(time) as start_time,
    EXTRACT(hour FROM time) as hour,
    EXTRACT(day FROM time) as day,
    EXTRACT(week FROM time) as week,
    EXTRACT(month FROM time) as month,
    EXTRACT(year FROM time) as year,
    EXTRACT(dayofweek FROM time) as weekday
FROM tb_log l
WHERE time IS NOT NULL
    AND l.page = 'NextSong'

'''