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
    songplay_id       INTEGER PRIMARY KEY IDENTITY(0,1) distkey,
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

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

