import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
REGION = config.get('DEFAULT', 'REGION')
ACCESS_KEY = config.get('DEFAULT', 'ACCESS_KEY')
SECRET_KEY = config.get('DEFAULT', 'SECRET_KEY')

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Use song, artist as the sortkeys to improve the speed while joining staging_songs
staging_events_table_create= ("""

CREATE TABLE staging_events(
    artist           VARCHAR,
    auth             VARCHAR,
    firstName        VARCHAR,
    gender           VARCHAR,
    itemInSession    INTEGER,
    lastName         VARCHAR,
    length           REAL,
    level            VARCHAR,
    location         VARCHAR,
    method           VARCHAR,
    page             VARCHAR,
    registration     FLOAT,
    sessionId        INTEGER distkey,
    song             VARCHAR,
    status           INTEGER,
    ts               TIMESTAMP,
    userAgent        VARCHAR,
    userId           INTEGER
)
COMPOUND SORTKEY(song, artist)

""")

# The song tables is large. Using the song_id as the distkey \
# can even data distribution.
# Use title, artist_name for fast-join
staging_songs_table_create = ("""

CREATE TABLE staging_songs (
    num_songs        INTEGER,
    artist_id        VARCHAR,
    artist_latitude  VARCHAR,
    artist_longitude VARCHAR,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR distkey,
    title            VARCHAR,
    duration         REAL,
    year             INTEGER
)
COMPOUND SORTKEY(title, artist_name)
 
""")

# The songplays tables is large. Using the songplay_id as the distkey \
# can even data distribution.
# I choose the song_id as the sortkey becase \
#We will use song_id to join the songs table frequently and it's large.  
songplay_table_create = ("""

CREATE TABLE songplays (
    songplay_id       INTEGER PRIMARY KEY IDENTITY(0,1) distkey ,
    start_time        TIMESTAMP,
    user_id           INTEGER,
    level             VARCHAR,
    song_id           VARCHAR sortkey,
    artist_id         VARCHAR,
    sessionId         INTEGER,
    location          VARCHAR,
    user_agent        VARCHAR
)

""")

# The users table is relatively small, therefore we choose to use all distribution.
user_table_create = ("""

CREATE TABLE users (
    user_id           INTEGER PRIMARY KEY sortkey,
    first_name        VARCHAR,
    last_name         VARCHAR,
    gender            VARCHAR,
    level             VARCHAR
)

""")

# The songs table is large, therefore we choose to use the song_id \
# as the distkey and sortkey.
song_table_create = ("""

CREATE TABLE songs(
    song_id           VARCHAR PRIMARY KEY sortkey distkey ,
    title             VARCHAR,
    artist_id         VARCHAR,
    year              INTEGER,
    duration          REAL
)

""")

# The users table is relatively small, therefore we choose to use all distribution \
# to improve query performance 
artist_table_create = ("""

CREATE TABLE artists(
    artist_id         VARCHAR PRIMARY KEY sortkey,
    name              VARCHAR,
    location          VARCHAR,
    latitude          VARCHAR,
    longitude         VARCHAR
)

""")

# The time table is relatively small, therefore we choose to use all distribution \
# to improve query performance 
time_table_create = ("""

CREATE TABLE time(
    start_time        TIMESTAMP PRIMARY KEY sortkey,
    hour              SMALLINT,
    day               SMALLINT,
    week              SMALLINT,
    month             SMALLINT,
    year              SMALLINT,
    weekday           SMALLINT
)

""")

# STAGING TABLES

staging_events_copy = ("""

copy 
staging_events
FROM {}
iam_role {}
FORMAT AS JSON {}
TIMEFORMAT AS 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""

copy 
staging_songs
FROM {}
iam_role {}
json 'auto';
""").format(SONG_DATA, ARN)


# FINAL TABLES

# Select columns based on the songplays table created \
# and filter the data with se.page = 'NextSong'
songplay_table_insert = ("""

INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, sessionId, location, user_agent)
SELECT 
    DISTINCT
    se.ts as start_time,
    se.userId as user_id,
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


# Select unique user_id base on latest timestamp.
user_table_insert = ("""

INSERT INTO users 
SELECT
    DISTINCT(userId) as　user_id,
    firstName as　first_name,
    lastName as　last_name,
    gender,
    level
FROM staging_events se
WHERE userId IS NOT NULL
    AND page = 'NextSong'
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

# Query that will import duplicates
artist_table_insert = ("""

INSERT INTO artists 
SELECT
    DISTINCT(artist_id) as artist_id,
    ss.artist_name as name,
    ss.artist_location as location,
    ss.artist_latitude as latitude,
    ss.artist_longitude as longitude
FROM staging_songs ss
WHERE artist_id IS NOT NULL
""")

# Updated query to only select unique artist_id

# artist_table_insert = ("""

# INSERT INTO artists 
# SELECT
#     artist_id,
#     name,
#     location,
#     latitude,
#     longitude
# FROM (
#         SELECT
#             ROW_NUMBER() OVER(PARTITION BY artist_id ORDER BY artist_name) AS row_num,
#             artist_id,
#             ss.artist_name as name,
#             ss.artist_location as location,
#             ss.artist_latitude as latitude,
#             ss.artist_longitude as longitude
#         FROM staging_songs ss
#         WHERE artist_id is not null
# ) T1
# WHERE T1.row_num = 1
# """)


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

# QUERIES FOR DATA QUALITY CHECK UP

# Check-verify for duplicate ids

quality_check_users = '''

SELECT
    'users_id' as table,
    COUNT(*) AS num
FROM (
    SELECT user_id, count(*) as num
    FROM users
    GROUP BY user_id
    HAVING count(*)> 1
) T
'''

quality_check_songs = '''

SELECT 
    'songs_id' as table,
    COUNT(*) AS num
FROM (
    SELECT song_id, count(*) as num
    FROM songs
    GROUP BY song_id
    HAVING count(*)> 1
) T

'''

quality_check_artists = '''
SELECT 
    'artists_id' as table,
    COUNT(*) AS num
FROM (
    SELECT artist_id, count(*) as num
    FROM artists
    GROUP BY artist_id
    HAVING count(*)> 1
) T
'''

# Total rows of each table

total_rows = '''

SELECT 
    'songplays' as table_name,
    count(*) as total_rows
From songplays

UNION ALL

SELECT 
    'users' as table_name,
    count(*) as total_rows
From users

UNION ALL

SELECT 
    'songs' as table_name,
    count(*) as total_rows
From songs

UNION ALL

SELECT 
    'artists' as table_name,
    count(*) as total_rows
From artists

UNION ALL

SELECT 
    'time' as table_name,
    count(*) as total_rows
From time

'''

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

# QUERIES FOR DATA QUALITY CHECK UP
quality_check = [quality_check_users, quality_check_songs, quality_check_artists]
